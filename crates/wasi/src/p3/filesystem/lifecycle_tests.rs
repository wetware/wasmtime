use crate::filesystem::FilesystemOperation;
use crate::p3::bindings::Command;
use crate::{FsPerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use futures::FutureExt as _;
use std::path::Path;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use test_programs_artifacts::{
    P3_API_FILESYSTEM_LIFECYCLE_COMPONENT, P3_FILE_WRITE_CHUNKED_COMPONENT,
    P3_FILE_WRITE_COMPONENT, P3_READDIR_COMPONENT,
};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Store, error::Context as _};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NativeWorkSite {
    Read,
    Write,
    Append,
    ReadDirectory,
}

struct NativeGate {
    site: NativeWorkSite,
    next_hit: usize,
    remaining_hits: usize,
    panic_after_release: bool,
    started: mpsc::Sender<usize>,
    permits: Arc<(Mutex<usize>, Condvar)>,
}

static NATIVE_GATE: Mutex<Option<NativeGate>> = Mutex::new(None);

pub(super) fn native_work_gate(site: NativeWorkSite) {
    let Some((hit, started, permits, panic_after_release)) = (|| {
        let mut state = NATIVE_GATE.lock().unwrap();
        let gate = state.as_mut()?;
        if gate.site != site || gate.remaining_hits == 0 {
            return None;
        }
        let hit = gate.next_hit;
        gate.next_hit += 1;
        gate.remaining_hits -= 1;
        Some((
            hit,
            gate.started.clone(),
            gate.permits.clone(),
            gate.panic_after_release,
        ))
    })() else {
        return;
    };

    let _ = started.send(hit);
    let (lock, ready) = &*permits;
    let mut permits = lock.lock().unwrap();
    while *permits == 0 {
        permits = ready.wait(permits).unwrap();
    }
    *permits -= 1;
    drop(permits);
    if panic_after_release {
        panic!("injected native filesystem panic at {site:?}");
    }
}

pub(super) struct InstalledGate {
    started: mpsc::Receiver<usize>,
    permits: Arc<(Mutex<usize>, Condvar)>,
}

impl InstalledGate {
    pub(super) fn new(site: NativeWorkSite, hits: usize) -> Self {
        Self::install(site, hits, false)
    }

    pub(super) fn new_panicking(site: NativeWorkSite) -> Self {
        Self::install(site, 1, true)
    }

    fn install(site: NativeWorkSite, hits: usize, panic_after_release: bool) -> Self {
        let (started_tx, started) = mpsc::channel();
        let permits = Arc::new((Mutex::new(0), Condvar::new()));
        let mut state = NATIVE_GATE.lock().unwrap();
        assert!(
            state.is_none(),
            "only one native-work gate may be installed"
        );
        *state = Some(NativeGate {
            site,
            next_hit: 1,
            remaining_hits: hits,
            panic_after_release,
            started: started_tx,
            permits: permits.clone(),
        });
        drop(state);
        Self { started, permits }
    }

    pub(super) fn wait_started(&self) -> usize {
        self.started
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("native filesystem work did not reach the gate")
    }

    pub(super) fn release_one(&self) {
        let (lock, ready) = &*self.permits;
        *lock.lock().unwrap() += 1;
        ready.notify_one();
    }
}

impl Drop for InstalledGate {
    fn drop(&mut self) {
        let (lock, ready) = &*self.permits;
        *lock.lock().unwrap() = usize::MAX;
        ready.notify_all();
        NATIVE_GATE.lock().unwrap().take();
    }
}

struct TestCtx {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for TestCtx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

struct CountActive {
    active: Arc<AtomicUsize>,
    drop_count: Arc<AtomicUsize>,
    dropped: mpsc::Sender<()>,
}

impl Drop for CountActive {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.drop_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.dropped.send(());
    }
}

async fn run_component(
    component_path: &'static str,
    scenario: Option<&'static str>,
    allow_blocking_current_thread: bool,
    active: Arc<AtomicUsize>,
    admitted: Arc<AtomicUsize>,
    drop_count: Arc<AtomicUsize>,
    dropped: mpsc::Sender<()>,
    panic_caught: Option<mpsc::Sender<()>>,
) -> wasmtime::Result<()> {
    let path = Path::new(component_path);
    let name = path.file_stem().unwrap().to_str().unwrap();
    let engine = test_programs_artifacts::engine(|config| {
        config.wasm_component_model_async(true);
    });
    let mut linker = Linker::new(&engine);
    // The P3 test components are built through the Preview 1 adapter, whose
    // Preview 2 imports remain part of the component graph.
    crate::p2::add_to_linker_async(&mut linker)?;
    crate::p3::add_to_linker(&mut linker)?;

    let workspace = tempfile::Builder::new()
        .prefix("p3-filesystem-lifecycle-")
        .tempdir()?;
    let mut builder = WasiCtxBuilder::new();
    if let Some(scenario) = scenario {
        builder.args(&[name, ".", scenario]);
    } else {
        builder.args(&[name, "."]);
    }
    builder
        .allow_blocking_current_thread(allow_blocking_current_thread)
        .preopened_dir(workspace.path(), ".", FsPerms::ReadWrite)?;
    builder.filesystem_operation_hook(move || {
        admitted.fetch_add(1, Ordering::SeqCst);
        active.fetch_add(1, Ordering::SeqCst);
        Ok(FilesystemOperation::new(CountActive {
            active: active.clone(),
            drop_count: drop_count.clone(),
            dropped: dropped.clone(),
        }))
    });

    let mut table = ResourceTable::new();
    table.set_max_capacity(1000);
    let mut store = Store::new(
        &engine,
        TestCtx {
            wasi: builder.build(),
            table,
        },
    );
    let component = Component::from_file(&engine, path)?;
    let command = Command::instantiate_async(&mut store, &component, &linker)
        .await
        .context("failed to instantiate P3 lifecycle test component")?;
    if let Some(panic_caught) = panic_caught {
        let result = std::panic::AssertUnwindSafe(
            store.run_concurrent(async move |store| command.wasi_cli_run().call_run(store).await),
        )
        .catch_unwind()
        .await;
        assert!(
            result.is_err(),
            "native panic did not reach the component call"
        );
        let _ = panic_caught.send(());
        // Keep the Store, including the producer/consumer resource which saw
        // the panic, alive until the test has observed operation release.
        std::future::pending::<()>().await;
        drop(store);
        unreachable!()
    } else {
        store
            .run_concurrent(async move |store| command.wasi_cli_run().call_run(store).await)
            .await
            .context("failed to invoke P3 lifecycle test component")??
            .map_err(|()| wasmtime::format_err!("P3 lifecycle test component failed"))
    }
}

fn wait_until_inactive(active: &AtomicUsize, dropped: &mpsc::Receiver<()>) {
    while active.load(Ordering::SeqCst) != 0 {
        dropped
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("filesystem operation did not release after native work exited");
    }
}

async fn check_store_drop_during_native_work(
    component: &'static str,
    scenario: Option<&'static str>,
    site: NativeWorkSite,
    gated_hits: usize,
) {
    let gate = InstalledGate::new(site, gated_hits);
    let active = Arc::new(AtomicUsize::new(0));
    let admitted = Arc::new(AtomicUsize::new(0));
    let drop_count = Arc::new(AtomicUsize::new(0));
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let task = tokio::spawn(run_component(
        component,
        scenario,
        false,
        active.clone(),
        admitted.clone(),
        drop_count.clone(),
        dropped_tx,
        None,
    ));

    assert_eq!(gate.wait_started(), 1);
    let admitted_for_session = admitted.load(Ordering::SeqCst);
    assert_eq!(active.load(Ordering::SeqCst), 1);

    if gated_hits > 1 {
        gate.release_one();
        assert_eq!(gate.wait_started(), 2);
        assert_eq!(
            admitted.load(Ordering::SeqCst),
            admitted_for_session,
            "a second native chunk performed another admission"
        );
    }

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        active.load(Ordering::SeqCst) == 1,
        "Store destruction released the operation while native work was gated"
    );

    gate.release_one();
    wait_until_inactive(&active, &dropped_rx);
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        admitted.load(Ordering::SeqCst)
    );
}

async fn check_terminal_stream_release(
    scenario: &'static str,
    gated_hits: usize,
    allow_blocking_current_thread: bool,
) {
    let gate = InstalledGate::new(NativeWorkSite::Read, gated_hits);
    let active = Arc::new(AtomicUsize::new(0));
    let admitted = Arc::new(AtomicUsize::new(0));
    let drop_count = Arc::new(AtomicUsize::new(0));
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let task = tokio::spawn(run_component(
        P3_API_FILESYSTEM_LIFECYCLE_COMPONENT,
        Some(scenario),
        allow_blocking_current_thread,
        active.clone(),
        admitted.clone(),
        drop_count.clone(),
        dropped_tx,
        None,
    ));

    for expected_hit in 1..=gated_hits {
        assert_eq!(gate.wait_started(), expected_hit);
        assert_eq!(
            active.load(Ordering::SeqCst),
            1,
            "the operation was not retained while native work was active"
        );
        if expected_hit > 1 {
            assert_eq!(
                admitted.load(Ordering::SeqCst),
                drop_count.load(Ordering::SeqCst) + 1,
                "a later stream chunk performed another admission"
            );
        }
        gate.release_one();
    }

    wait_until_inactive(&active, &dropped_rx);
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        admitted.load(Ordering::SeqCst)
    );
    assert!(
        !task.is_finished(),
        "the guest did not retain the inert stream after terminal completion"
    );

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

async fn check_native_panic_releases_session(
    component: &'static str,
    site: NativeWorkSite,
    allow_blocking_current_thread: bool,
) {
    let gate = InstalledGate::new_panicking(site);
    let active = Arc::new(AtomicUsize::new(0));
    let admitted = Arc::new(AtomicUsize::new(0));
    let drop_count = Arc::new(AtomicUsize::new(0));
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let (panic_tx, panic_rx) = mpsc::channel();
    let task = tokio::spawn(run_component(
        component,
        None,
        allow_blocking_current_thread,
        active.clone(),
        admitted.clone(),
        drop_count.clone(),
        dropped_tx,
        Some(panic_tx),
    ));

    assert_eq!(gate.wait_started(), 1);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    gate.release_one();
    panic_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("native panic did not propagate through the component call");
    wait_until_inactive(&active, &dropped_rx);
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        admitted.load(Ordering::SeqCst)
    );
    assert!(
        !task.is_finished(),
        "the Store was dropped before operation release was observed"
    );

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

pub(super) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_session_survives_store_drop_and_reuses_admission_across_chunks() {
    let _lock = TEST_LOCK.lock().await;
    check_store_drop_during_native_work(P3_FILE_WRITE_COMPONENT, None, NativeWorkSite::Read, 2)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_session_survives_store_drop_and_reuses_admission_across_chunks() {
    let _lock = TEST_LOCK.lock().await;
    check_store_drop_during_native_work(
        P3_FILE_WRITE_CHUNKED_COMPONENT,
        None,
        NativeWorkSite::Write,
        2,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_session_survives_store_drop_while_native_work_runs() {
    let _lock = TEST_LOCK.lock().await;
    check_store_drop_during_native_work(
        P3_API_FILESYSTEM_LIFECYCLE_COMPONENT,
        Some("eof"),
        NativeWorkSite::Append,
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eager_readdir_survives_store_drop_while_native_work_runs() {
    let _lock = TEST_LOCK.lock().await;
    check_store_drop_during_native_work(
        P3_READDIR_COMPONENT,
        None,
        NativeWorkSite::ReadDirectory,
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_close_does_not_end_session_and_eof_releases_inert_stream() {
    let _lock = TEST_LOCK.lock().await;
    check_terminal_stream_release("eof", 3, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_error_releases_inert_stream() {
    let _lock = TEST_LOCK.lock().await;
    check_terminal_stream_release("error", 1, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_mode_terminal_eof_releases_inert_stream() {
    let _lock = TEST_LOCK.lock().await;
    check_terminal_stream_release("eof", 3, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_native_panic_releases_session_while_store_remains_alive() {
    let _lock = TEST_LOCK.lock().await;
    check_native_panic_releases_session(P3_FILE_WRITE_COMPONENT, NativeWorkSite::Read, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_native_panic_releases_session_while_store_remains_alive() {
    let _lock = TEST_LOCK.lock().await;
    check_native_panic_releases_session(P3_FILE_WRITE_COMPONENT, NativeWorkSite::Write, false)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_native_panic_releases_session_while_store_remains_alive() {
    let _lock = TEST_LOCK.lock().await;
    check_native_panic_releases_session(P3_READDIR_COMPONENT, NativeWorkSite::ReadDirectory, false)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_native_panic_releases_session_while_store_remains_alive() {
    let _lock = TEST_LOCK.lock().await;
    check_native_panic_releases_session(P3_FILE_WRITE_COMPONENT, NativeWorkSite::Read, true).await;
}

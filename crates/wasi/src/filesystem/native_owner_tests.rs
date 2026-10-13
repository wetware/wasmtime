use super::*;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, OnceLock, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::sync::oneshot;

struct Release {
    count: Arc<AtomicUsize>,
    done: Option<oneshot::Sender<()>>,
}

impl Drop for Release {
    fn drop(&mut self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}

fn owned(native: std::fs::File) -> (FilesystemHandle, Arc<AtomicUsize>, oneshot::Receiver<()>) {
    let count = Arc::new(AtomicUsize::new(0));
    let (done, released) = oneshot::channel();
    let handle = FilesystemHandle::new(
        native,
        Release {
            count: count.clone(),
            done: Some(done),
        },
    );
    (handle, count, released)
}

fn ambient_dir(path: &std::path::Path) -> std::fs::File {
    cap_primitives::fs::open_ambient_dir(path, cap_primitives::ambient_authority()).unwrap()
}

#[test]
fn idle_descriptor_and_preserving_duplicate_keep_content_without_active_operation() {
    let (handle, content, _) = owned(tempfile::tempfile().unwrap());
    let operation = Arc::new(AtomicUsize::new(0));
    let active = FilesystemOperation::new(Release {
        count: operation.clone(),
        done: None,
    });
    let file = File::new(handle, FsPerms::ReadOnly, OpenMode::READ, false);

    drop(active);
    assert_eq!(operation.load(Ordering::SeqCst), 1);
    assert_eq!(content.load(Ordering::SeqCst), 0);

    let shared = Arc::clone(&file.file);
    let duplicate = shared.try_clone().unwrap();
    drop(file);
    drop(shared);
    assert_eq!(content.load(Ordering::SeqCst), 0);
    assert!(duplicate.metadata().is_ok());
    drop(duplicate);
    assert_eq!(content.load(Ordering::SeqCst), 1);
}

#[test]
fn idle_directory_shared_clone_keeps_content_without_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (handle, content, _) = owned(ambient_dir(temp.path()));
    let dir = Dir::new(handle, FsPerms::ReadOnly, OpenMode::READ, false);
    let clone = dir.clone();
    let native = Arc::clone(&dir.dir);
    drop(dir);
    drop(clone);
    assert_eq!(content.load(Ordering::SeqCst), 0);
    assert!(native.metadata().unwrap().is_dir());
    drop(native);
    assert_eq!(content.load(Ordering::SeqCst), 1);
}

// A keyed gate in the actual is_same_object worker lets both descriptors and
// the initiating future disappear while the worker still carries B.
struct ComparisonGate {
    started: mpsc::Sender<()>,
    released: Mutex<bool>,
    wake: Condvar,
}

static COMPARISONS: OnceLock<Mutex<BTreeMap<usize, Arc<ComparisonGate>>>> = OnceLock::new();

pub(super) fn is_same_object_gate(file: &std::fs::File) {
    let gate = COMPARISONS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(file as *const std::fs::File as usize));
    if let Some(gate) = gate {
        let _ = gate.started.send(());
        let mut released = gate.released.lock().unwrap();
        while !*released {
            released = gate.wake.wait(released).unwrap();
        }
    }
}

struct InstalledComparison {
    key: usize,
    gate: Arc<ComparisonGate>,
    started: mpsc::Receiver<()>,
}

impl InstalledComparison {
    fn new(file: &std::fs::File) -> Self {
        let (started, rx) = mpsc::channel();
        let gate = Arc::new(ComparisonGate {
            started,
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        let key = file as *const std::fs::File as usize;
        assert!(
            COMPARISONS
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(key, gate.clone())
                .is_none()
        );
        Self {
            key,
            gate,
            started: rx,
        }
    }
}

impl Drop for InstalledComparison {
    fn drop(&mut self) {
        *self.gate.released.lock().unwrap() = true;
        self.gate.wake.notify_all();
        COMPARISONS.get().unwrap().lock().unwrap().remove(&self.key);
    }
}

#[tokio::test]
async fn second_descriptor_content_survives_actual_comparison_worker() {
    for directory in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (handle, content, released) = owned(if directory {
            ambient_dir(temp.path())
        } else {
            tempfile::tempfile().unwrap()
        });
        let target = if directory {
            Descriptor::Dir(Dir::new(handle, FsPerms::ReadOnly, OpenMode::READ, false))
        } else {
            Descriptor::File(File::new(handle, FsPerms::ReadOnly, OpenMode::READ, false))
        };
        let source = if directory {
            Descriptor::Dir(Dir::new(
                ambient_dir(temp.path()),
                FsPerms::ReadOnly,
                OpenMode::READ,
                false,
            ))
        } else {
            Descriptor::File(File::new(
                tempfile::tempfile().unwrap(),
                FsPerms::ReadOnly,
                OpenMode::READ,
                false,
            ))
        };
        let gate = InstalledComparison::new(match &source {
            Descriptor::File(file) => &file.file,
            Descriptor::Dir(dir) => &dir.dir,
        });
        let operation = Arc::new(AtomicUsize::new(0));
        let (operation_done, operation_released) = oneshot::channel();
        let mut future = Box::pin(source.is_same_object(
            Some(FilesystemOperation::new(Release {
                count: operation.clone(),
                done: Some(operation_done),
            })),
            &target,
        ));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        gate.started.recv_timeout(Duration::from_secs(30)).unwrap();
        drop(future);
        drop(target);
        drop(source);
        assert_eq!(operation.load(Ordering::SeqCst), 0);
        assert_eq!(
            content.load(Ordering::SeqCst),
            0,
            "B content released while its native handle is in A's worker"
        );
        drop(gate);
        tokio::time::timeout(Duration::from_secs(30), released)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(30), operation_released)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.load(Ordering::SeqCst), 1);
        assert_eq!(content.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_worker_retains_content_after_store_drop() {
    let (handle, content, released) = owned(tempfile::tempfile().unwrap());
    let file = File::new(handle, FsPerms::ReadOnly, OpenMode::READ, false);
    let mut store = wasmtime::Store::new(&wasmtime::Engine::default(), file);
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    {
        let mut future = Box::pin(store.data_mut().run_blocking(None, move |file| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            file.metadata()
        }));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        started_rx.await.unwrap();
    }
    drop(store);
    assert_eq!(content.load(Ordering::SeqCst), 0);
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(30), released)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(content.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn child_result_retains_content_through_installation_or_disposal() {
    for (directory, install) in [(false, true), (false, false), (true, true), (true, false)] {
        let temp = tempfile::tempdir().unwrap();
        if directory {
            std::fs::create_dir(temp.path().join("child")).unwrap();
        } else {
            std::fs::write(temp.path().join("child"), b"contents").unwrap();
        }
        let (handle, content, _) = owned(ambient_dir(temp.path()));
        let parent = Dir::new(handle, FsPerms::ReadOnly, OpenMode::READ, false);
        let result = parent
            .open_at(
                None,
                PathFlags::empty(),
                "child".into(),
                OpenFlags::empty(),
                DescriptorFlags::READ,
                false,
            )
            .await
            .unwrap_or_else(|_| panic!("open failed"));
        drop(parent);
        assert_eq!(content.load(Ordering::SeqCst), 0);
        let mut table = ResourceTable::new();
        if install {
            let child = table.push(result).unwrap();
            match table.get(&child).unwrap() {
                Descriptor::Dir(dir) => assert!(directory && dir.dir.metadata().unwrap().is_dir()),
                Descriptor::File(file) => {
                    assert!(!directory);
                    assert_eq!(file.file.metadata().unwrap().len(), 8);
                }
            }
            assert_eq!(content.load(Ordering::SeqCst), 0);
            drop(table);
        } else {
            table.set_max_capacity(0);
            assert!(matches!(
                table.push(result),
                Err(wasmtime::component::ResourceTableError::Full)
            ));
        }
        assert_eq!(content.load(Ordering::SeqCst), 1);
    }
}

struct Signal(std::sync::mpsc::Sender<()>);

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

#[test]
fn completed_unpolled_open_owns_only_successful_native_results() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        // Missing path, successful file, successful directory, and a native
        // file opened but rejected because DIRECTORY was requested.
        for (kind, success) in [(0, false), (1, true), (2, true), (3, false)] {
            let (entered_tx, entered_rx) = oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            entered_rx.await.unwrap();
            let temp = tempfile::tempdir().unwrap();
            if kind == 1 || kind == 3 {
                std::fs::write(temp.path().join("child"), b"contents").unwrap();
            } else if kind == 2 {
                std::fs::create_dir(temp.path().join("child")).unwrap();
            }
            let (handle, content, _) = owned(ambient_dir(temp.path()));
            let parent = Dir::new(handle, FsPerms::ReadOnly, OpenMode::READ, false);
            let (woke_tx, woke_rx) = std::sync::mpsc::channel();
            let waker = Waker::from(Arc::new(Signal(woke_tx)));
            let mut future = Box::pin(parent.open_at(
                None,
                PathFlags::empty(),
                "child".into(),
                if kind == 3 {
                    OpenFlags::DIRECTORY
                } else {
                    OpenFlags::empty()
                },
                DescriptorFlags::READ,
                false,
            ));
            assert!(matches!(
                future.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Pending
            ));
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            woke_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            assert_eq!(
                Arc::strong_count(parent.dir.owner.as_ref().unwrap()),
                if success { 2 } else { 1 }
            );
            drop(future);
            assert_eq!(Arc::strong_count(parent.dir.owner.as_ref().unwrap()), 1);
            drop(parent);
            assert_eq!(content.load(Ordering::SeqCst), 1);
        }
    });
}

#[cfg(feature = "p3")]
#[test]
fn independent_directory_iterator_retains_content_after_descriptor_drop() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("child"), b"contents").unwrap();
    let (handle, content, _) = owned(ambient_dir(temp.path()));
    let dir = Dir::new(handle, FsPerms::ReadOnly, OpenMode::READ, true);
    let mut entries = dir.dir.read_dir().unwrap();
    drop(dir);
    assert_eq!(content.load(Ordering::SeqCst), 0);
    assert!(entries.next().unwrap().is_ok());
    drop(entries);
    assert_eq!(content.load(Ordering::SeqCst), 1);
}

#[cfg(unix)]
#[test]
fn content_owner_runs_after_native_file_closes() {
    // Isolate the descriptor table so another test cannot reuse a closed fd
    // before the owner checks F_GETFD.
    const CHILD: &str = "WASMTIME_V48_NATIVE_OWNER_CLOSE_PROBE";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "filesystem::native_owner_tests::content_owner_runs_after_native_file_closes",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }

    use std::os::fd::AsRawFd;
    use std::sync::atomic::{AtomicBool, AtomicI32};

    unsafe extern "C" {
        fn fcntl(fd: std::ffi::c_int, cmd: std::ffi::c_int, ...) -> std::ffi::c_int;
    }

    struct Check {
        fd: Arc<AtomicI32>,
        closed: Arc<AtomicBool>,
    }

    impl Drop for Check {
        fn drop(&mut self) {
            // F_GETFD is 1 on this Unix test target. No fd borrow is made.
            self.closed.store(
                unsafe { fcntl(self.fd.load(Ordering::SeqCst), 1) } == -1,
                Ordering::SeqCst,
            );
        }
    }

    let native = tempfile::tempfile().unwrap();
    let closed = Arc::new(AtomicBool::new(false));
    let owner = Check {
        fd: Arc::new(AtomicI32::new(native.as_raw_fd())),
        closed: closed.clone(),
    };
    drop(FilesystemHandle::new(native, owner));
    assert!(closed.load(Ordering::SeqCst));

    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let temp = tempfile::tempdir().unwrap();
            std::fs::write(temp.path().join("child"), b"contents").unwrap();
            let native = ambient_dir(temp.path());
            let fd = Arc::new(AtomicI32::new(native.as_raw_fd()));
            let closed = Arc::new(AtomicBool::new(false));
            let parent = Dir::new(
                FilesystemHandle::new(
                    native,
                    Check {
                        fd: fd.clone(),
                        closed: closed.clone(),
                    },
                ),
                FsPerms::ReadOnly,
                OpenMode::READ,
                false,
            );
            let result = parent
                .open_at(
                    None,
                    PathFlags::empty(),
                    "child".into(),
                    OpenFlags::empty(),
                    DescriptorFlags::READ,
                    false,
                )
                .await
                .unwrap_or_else(|_| panic!("open failed"));
            let Descriptor::File(child) = &result else {
                panic!("expected child file")
            };
            fd.store(child.file.as_raw_fd(), Ordering::SeqCst);
            drop(parent);
            assert!(!closed.load(Ordering::SeqCst));
            let mut table = ResourceTable::new();
            table.set_max_capacity(0);
            assert!(matches!(
                table.push(result),
                Err(wasmtime::component::ResourceTableError::Full)
            ));
            assert!(
                closed.load(Ordering::SeqCst),
                "child native fd must close before its content owner releases"
            );
        });
}

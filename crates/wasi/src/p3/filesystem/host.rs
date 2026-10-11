use crate::filesystem::sys;
use crate::filesystem::{
    Descriptor, Dir, File, FilesystemOperation, FilesystemOperationError,
    FilesystemOperationResult, WasiFilesystem, WasiFilesystemCtx, WasiFilesystemCtxView,
};
use crate::p3::bindings::clocks::system_clock;
use crate::p3::bindings::filesystem::types::{
    self, Advice, DescriptorFlags, DescriptorStat, DescriptorType, DirectoryEntry, ErrorCode,
    Filesize, MetadataHashValue, NewTimestamp, OpenFlags, PathFlags,
};
use crate::p3::filesystem::{FilesystemError, FilesystemResult, preopens};
use crate::p3::{DEFAULT_BUFFER_CAPACITY, FallibleIteratorProducer};
use bytes::BytesMut;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use core::{iter, mem};
use std::io;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, spawn_blocking};
use wasmtime::StoreContextMut;
use wasmtime::component::{
    Access, Accessor, Destination, FutureReader, Resource, ResourceTable, Source, StreamConsumer,
    StreamProducer, StreamReader, StreamResult,
};
use wasmtime::error::Context as _;

fn get_descriptor<'a>(
    table: &'a ResourceTable,
    fd: &'a Resource<Descriptor>,
) -> FilesystemResult<&'a Descriptor> {
    table
        .get(fd)
        .context("failed to get descriptor resource from table")
        .map_err(FilesystemError::trap)
}

fn get_file<'a>(
    table: &'a ResourceTable,
    fd: &'a Resource<Descriptor>,
) -> FilesystemResult<&'a File> {
    let file = get_descriptor(table, fd).map(Descriptor::file)??;
    Ok(file)
}

fn get_dir<'a>(
    table: &'a ResourceTable,
    fd: &'a Resource<Descriptor>,
) -> FilesystemResult<&'a Dir> {
    let dir = get_descriptor(table, fd).map(Descriptor::dir)??;
    Ok(dir)
}

trait AccessorExt {
    fn get_descriptor(&self, fd: &Resource<Descriptor>) -> FilesystemResult<Descriptor>;
    fn get_file(&self, fd: &Resource<Descriptor>) -> FilesystemResult<File>;
    fn get_dir(&self, fd: &Resource<Descriptor>) -> FilesystemResult<Dir>;
    fn get_dir_pair(
        &self,
        a: &Resource<Descriptor>,
        b: &Resource<Descriptor>,
    ) -> FilesystemResult<(Dir, Dir)>;
}

impl<T> AccessorExt for Accessor<T, WasiFilesystem> {
    fn get_descriptor(&self, fd: &Resource<Descriptor>) -> FilesystemResult<Descriptor> {
        self.with(|mut store| {
            let fd = get_descriptor(store.get().table, fd)?;
            Ok(fd.clone())
        })
    }

    fn get_file(&self, fd: &Resource<Descriptor>) -> FilesystemResult<File> {
        self.with(|mut store| {
            let file = get_file(store.get().table, fd)?;
            Ok(file.clone())
        })
    }

    fn get_dir(&self, fd: &Resource<Descriptor>) -> FilesystemResult<Dir> {
        self.with(|mut store| {
            let dir = get_dir(store.get().table, fd)?;
            Ok(dir.clone())
        })
    }

    fn get_dir_pair(
        &self,
        a: &Resource<Descriptor>,
        b: &Resource<Descriptor>,
    ) -> FilesystemResult<(Dir, Dir)> {
        self.with(|mut store| {
            let table = store.get().table;
            let a = get_dir(table, a)?;
            let b = get_dir(table, b)?;
            Ok((a.clone(), b.clone()))
        })
    }
}

fn start_operation(ctx: &WasiFilesystemCtx) -> Result<Option<FilesystemOperation>, ErrorCode> {
    ctx.start_operation().map_err(|err| match err {
        FilesystemOperationError::Rejected => ErrorCode::Busy,
    })
}

fn start_accessor_operation<U>(
    store: &Accessor<U, WasiFilesystem>,
) -> FilesystemResult<Option<FilesystemOperation>> {
    store.with(|mut store| start_operation(store.get().ctx).map_err(FilesystemError::from))
}

struct LifecycleStreamProducer<P> {
    producer: Option<P>,
    operation: Option<FilesystemOperation>,
}

impl<P> LifecycleStreamProducer<P> {
    fn new(producer: P, operation: Option<FilesystemOperation>) -> Self {
        Self {
            producer: Some(producer),
            operation,
        }
    }
}

impl<D, P> StreamProducer<D> for LifecycleStreamProducer<P>
where
    P: StreamProducer<D> + Unpin,
{
    type Item = P::Item;
    type Buffer = P::Buffer;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<'a, D>,
        dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let this = self.get_mut();
        // Move both values into this poll. If the producer panics, unwinding
        // drops the producer before the operation. A nonterminal poll restores
        // both values for the next chunk.
        let operation = this.operation.take();
        let Some(mut producer) = this.producer.take() else {
            return Poll::Ready(Ok(StreamResult::Dropped));
        };
        let result = Pin::new(&mut producer).poll_produce(cx, store, dst, finish);
        if matches!(
            result,
            Poll::Ready(Ok(StreamResult::Dropped)) | Poll::Ready(Err(_))
        ) {
            drop(producer);
            drop(operation);
        } else {
            this.producer = Some(producer);
            this.operation = operation;
        }
        result
    }
}

fn systemtime_from(t: system_clock::Instant) -> Result<std::time::SystemTime, ErrorCode> {
    if let Ok(seconds) = t.seconds.try_into() {
        std::time::SystemTime::UNIX_EPOCH
            .checked_add(core::time::Duration::new(seconds, t.nanoseconds))
            .ok_or(ErrorCode::Overflow)
    } else {
        std::time::SystemTime::UNIX_EPOCH
            .checked_sub(core::time::Duration::new(
                t.seconds.unsigned_abs(),
                t.nanoseconds,
            ))
            .ok_or(ErrorCode::Overflow)
    }
}

fn systemtimespec_from(t: NewTimestamp) -> Result<Option<SystemTime>, ErrorCode> {
    match t {
        NewTimestamp::NoChange => Ok(None),
        NewTimestamp::Now => Ok(Some(SystemTime::now())),
        NewTimestamp::Timestamp(st) => Ok(Some(systemtime_from(st)?)),
    }
}

struct ReadStreamProducer {
    file: File,
    offset: u64,
    result: Option<oneshot::Sender<Result<(), ErrorCode>>>,
    operation: Option<FilesystemOperation>,
    task: Option<JoinHandle<FilesystemOperationResult<std::io::Result<BytesMut>>>>,
}

impl Drop for ReadStreamProducer {
    fn drop(&mut self) {
        self.close(Ok(()))
    }
}

impl ReadStreamProducer {
    fn close(&mut self, res: Result<(), ErrorCode>) {
        if let Some(tx) = self.result.take() {
            _ = tx.send(res);
        }
        self.operation.take();
    }

    /// Update the internal `offset` field after reading `amt` bytes from the file.
    fn complete_read(&mut self, amt: usize) -> StreamResult {
        let Ok(amt) = amt.try_into() else {
            self.close(Err(ErrorCode::Overflow));
            return StreamResult::Dropped;
        };
        let Some(amt) = self.offset.checked_add(amt) else {
            self.close(Err(ErrorCode::Overflow));
            return StreamResult::Dropped;
        };
        self.offset = amt;
        StreamResult::Completed
    }
}

impl<D> StreamProducer<D> for ReadStreamProducer {
    type Item = u8;
    type Buffer = BytesMut;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<'a, D>,
        mut dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        if self.result.is_none() {
            return Poll::Ready(Ok(StreamResult::Dropped));
        }
        if self.file.as_blocking_file().is_some() {
            // Once a blocking file, always a blocking file, so assert as such.
            assert!(self.task.is_none());
            let mut dst = dst.as_direct(store, DEFAULT_BUFFER_CAPACITY);
            let buf = dst.remaining();
            if buf.is_empty() {
                return Poll::Ready(Ok(StreamResult::Completed));
            }
            let result_tx = self.result.take();
            let operation = self.operation.take();
            let offset = self.offset;
            let file = self.file.as_blocking_file().unwrap();
            #[cfg(test)]
            super::lifecycle_tests::native_work_gate(super::lifecycle_tests::NativeWorkSite::Read);
            let result = sys::read_at_cursor_unspecified(file, buf, offset);
            self.operation = operation;
            self.result = result_tx;
            return match result {
                Ok(0) => {
                    self.close(Ok(()));
                    Poll::Ready(Ok(StreamResult::Dropped))
                }
                Ok(n) => {
                    dst.mark_written(n);
                    Poll::Ready(Ok(self.complete_read(n)))
                }
                Err(err) => {
                    self.close(Err(err.into()));
                    Poll::Ready(Ok(StreamResult::Dropped))
                }
            };
        }

        // Lazily spawn a read task if one hasn't already been spawned yet.
        let me = &mut *self;
        let task = me.task.get_or_insert_with(|| {
            let mut buf = dst.take_buffer();
            buf.resize(DEFAULT_BUFFER_CAPACITY, 0);
            let file = Arc::clone(me.file.as_file());
            let offset = me.offset;
            let operation = me.operation.clone();
            spawn_blocking(move || {
                let operation = operation;
                let file = file;
                #[cfg(test)]
                super::lifecycle_tests::native_work_gate(
                    super::lifecycle_tests::NativeWorkSite::Read,
                );
                let result = sys::read_at_cursor_unspecified(&file, &mut buf, offset).map(|n| {
                    buf.truncate(n);
                    buf
                });
                drop(file);
                FilesystemOperationResult::new(result, operation)
            })
        });

        // Await the completion of the read task. Note that this is not a
        // cancellable await point because we can't cancel the other task, so
        // the `finish` parameter is ignored.
        let result = match Pin::new(&mut *task).poll(cx) {
            // If cancellation is requested, then flag that to Tokio. Note that
            // this still waits for the actual completion of the spawned task,
            // which won't actually happen if it's already executing.
            Poll::Pending if finish => {
                task.abort();
                ready!(Pin::new(task).poll(cx))
            }
            other => ready!(other),
        };
        self.task = None;
        match result.map(FilesystemOperationResult::into_inner) {
            Ok(Ok(buf)) if buf.is_empty() => {
                self.close(Ok(()));
                Poll::Ready(Ok(StreamResult::Dropped))
            }
            Ok(Ok(buf)) => {
                let n = buf.len();
                dst.set_buffer(buf);
                Poll::Ready(Ok(self.complete_read(n)))
            }
            Ok(Err(err)) => {
                self.close(Err(err.into()));
                Poll::Ready(Ok(StreamResult::Dropped))
            }
            Err(err) => {
                if err.is_cancelled() {
                    return Poll::Ready(Ok(StreamResult::Cancelled));
                }
                self.close(Err(ErrorCode::Io));
                panic!("I/O task should not panic: {err}")
            }
        }
    }
}

fn map_dir_entry(
    entry: std::io::Result<cap_primitives::fs::DirEntry>,
) -> Result<Option<DirectoryEntry>, ErrorCode> {
    match entry {
        Ok(entry) => {
            let meta = entry.metadata()?;
            let Ok(name) = entry.file_name().into_string() else {
                return Err(ErrorCode::IllegalByteSequence);
            };
            Ok(Some(DirectoryEntry {
                type_: meta.file_type().into(),
                name,
            }))
        }
        Err(err) => {
            // On windows, filter out files like `C:\DumpStack.log.tmp` which we
            // can't get full metadata for.
            #[cfg(windows)]
            {
                use windows_sys::Win32::Foundation::{
                    ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION,
                };
                if err.raw_os_error() == Some(ERROR_SHARING_VIOLATION as i32)
                    || err.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32)
                {
                    return Ok(None);
                }
            }
            Err(err.into())
        }
    }
}

struct ReadDirStream {
    rx: mpsc::Receiver<DirectoryEntry>,
    task: JoinHandle<FilesystemOperationResult<Result<(), ErrorCode>>>,
    result: Option<oneshot::Sender<Result<(), ErrorCode>>>,
    operation: Option<FilesystemOperation>,
}

impl ReadDirStream {
    fn new(
        dir: Arc<std::fs::File>,
        result: oneshot::Sender<Result<(), ErrorCode>>,
        operation: Option<FilesystemOperation>,
    ) -> ReadDirStream {
        let (tx, rx) = mpsc::channel(1);
        let native_operation = operation.clone();
        ReadDirStream {
            task: spawn_blocking(move || {
                let operation = native_operation;
                let dir = dir;
                let tx = tx;
                #[cfg(test)]
                super::lifecycle_tests::native_work_gate(
                    super::lifecycle_tests::NativeWorkSite::ReadDirectory,
                );
                let value = (|| {
                    let entries = cap_primitives::fs::read_base_dir(&dir)?;
                    for entry in entries {
                        if let Some(entry) = map_dir_entry(entry)? {
                            if let Err(_) = tx.blocking_send(entry) {
                                break;
                            }
                        }
                    }
                    Ok(())
                })();
                drop(tx);
                drop(dir);
                FilesystemOperationResult::new(value, operation)
            }),
            rx,
            result: Some(result),
            operation,
        }
    }

    fn close(&mut self, res: Result<(), ErrorCode>) {
        self.rx.close();
        self.task.abort();
        if let Some(result) = self.result.take() {
            let _ = result.send(res);
        }
        self.operation.take();
    }
}

impl<D> StreamProducer<D> for ReadDirStream {
    type Item = DirectoryEntry;
    type Buffer = Option<DirectoryEntry>;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, D>,
        mut dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        if self.result.is_none() {
            return Poll::Ready(Ok(StreamResult::Dropped));
        }
        // If this is a 0-length read then `mpsc::Receiver` does not expose an
        // API to wait for an item to be available without taking it out of the
        // channel. In lieu of that just say that we're complete and ready for a
        // read.
        if dst.remaining(&mut store) == Some(0) {
            return Poll::Ready(Ok(StreamResult::Completed));
        }

        match self.rx.poll_recv(cx) {
            // If an item is on the channel then send that along and say that
            // the read is now complete with one item being yielded.
            Poll::Ready(Some(item)) => {
                dst.set_buffer(Some(item));
                Poll::Ready(Ok(StreamResult::Completed))
            }

            // If there's nothing left on the channel then that means that an
            // error occurred or the iterator is done. In both cases an
            // un-cancellable wait for the spawned task is entered and we await
            // its completion. Upon completion there our own stream is closed
            // with the result (sending an error code on our oneshot) and then
            // the stream is reported as dropped.
            Poll::Ready(None) => {
                let result = match ready!(Pin::new(&mut self.task).poll(cx)) {
                    Ok(result) => result.into_inner(),
                    Err(err) => {
                        self.close(Err(ErrorCode::Io));
                        panic!("directory task should not panic: {err}")
                    }
                };
                self.close(result);
                Poll::Ready(Ok(StreamResult::Dropped))
            }

            // If an item isn't ready yet then cancel this outstanding request
            // if `finish` is set, otherwise propagate the `Pending` status.
            Poll::Pending if finish => Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ReadDirStream {
    fn drop(&mut self) {
        if self.result.is_some() {
            self.close(Ok(()));
        }
    }
}

struct WriteStreamConsumer {
    file: File,
    location: WriteLocation,
    result: Option<oneshot::Sender<Result<(), ErrorCode>>>,
    operation: Option<FilesystemOperation>,
    buffer: BytesMut,
    task: Option<JoinHandle<FilesystemOperationResult<std::io::Result<(BytesMut, usize)>>>>,
}

#[derive(Copy, Clone)]
enum WriteLocation {
    End,
    Offset(u64),
}

impl WriteStreamConsumer {
    fn new_at(
        file: File,
        offset: u64,
        result: oneshot::Sender<Result<(), ErrorCode>>,
        operation: Option<FilesystemOperation>,
    ) -> Self {
        Self {
            file,
            location: WriteLocation::Offset(offset),
            result: Some(result),
            operation,
            buffer: BytesMut::default(),
            task: None,
        }
    }

    fn new_append(
        file: File,
        result: oneshot::Sender<Result<(), ErrorCode>>,
        operation: Option<FilesystemOperation>,
    ) -> Self {
        Self {
            file,
            location: WriteLocation::End,
            result: Some(result),
            operation,
            buffer: BytesMut::default(),
            task: None,
        }
    }

    fn close(&mut self, res: Result<(), ErrorCode>) {
        if let Some(result) = self.result.take() {
            _ = result.send(res);
        }
        self.operation.take();
    }

    /// Update the internal `offset` field after writing `amt` bytes from the file.
    fn complete_write(&mut self, amt: usize) -> StreamResult {
        match &mut self.location {
            WriteLocation::End => StreamResult::Completed,
            WriteLocation::Offset(offset) => {
                let Ok(amt) = amt.try_into() else {
                    self.close(Err(ErrorCode::Overflow));
                    return StreamResult::Dropped;
                };
                let Some(amt) = offset.checked_add(amt) else {
                    self.close(Err(ErrorCode::Overflow));
                    return StreamResult::Dropped;
                };
                *offset = amt;
                StreamResult::Completed
            }
        }
    }
}

impl WriteLocation {
    fn write(&self, file: &std::fs::File, bytes: &[u8]) -> io::Result<usize> {
        match *self {
            WriteLocation::End => sys::append_cursor_unspecified(file, bytes),
            WriteLocation::Offset(at) => sys::write_at_cursor_unspecified(file, bytes, at),
        }
    }
}

impl<D> StreamConsumer<D> for WriteStreamConsumer {
    type Item = u8;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        src: Source<Self::Item>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        if self.result.is_none() {
            return Poll::Ready(Ok(StreamResult::Dropped));
        }
        let mut src = src.as_direct(store);
        if self.file.as_blocking_file().is_some() {
            // Once a blocking file, always a blocking file, so assert as such.
            assert!(self.task.is_none());
            let result_tx = self.result.take();
            let operation = self.operation.take();
            let location = self.location;
            let file = self.file.as_blocking_file().unwrap();
            #[cfg(test)]
            super::lifecycle_tests::native_work_gate(match location {
                WriteLocation::End => super::lifecycle_tests::NativeWorkSite::Append,
                WriteLocation::Offset(_) => super::lifecycle_tests::NativeWorkSite::Write,
            });
            let result = location.write(file, src.remaining());
            self.operation = operation;
            self.result = result_tx;
            return match result {
                Ok(n) => {
                    src.mark_read(n);
                    Poll::Ready(Ok(self.complete_write(n)))
                }
                Err(err) => {
                    self.close(Err(err.into()));
                    Poll::Ready(Ok(StreamResult::Dropped))
                }
            };
        }
        let me = &mut *self;
        let task = me.task.get_or_insert_with(|| {
            debug_assert!(me.buffer.is_empty());
            let remaining = src.remaining();
            let n = remaining.len().min(DEFAULT_BUFFER_CAPACITY);
            me.buffer.extend_from_slice(&remaining[..n]);
            let buf = mem::take(&mut me.buffer);
            let file = Arc::clone(me.file.as_file());
            let location = me.location;
            let operation = me.operation.clone();
            spawn_blocking(move || {
                let operation = operation;
                let file = file;
                #[cfg(test)]
                super::lifecycle_tests::native_work_gate(match location {
                    WriteLocation::End => super::lifecycle_tests::NativeWorkSite::Append,
                    WriteLocation::Offset(_) => super::lifecycle_tests::NativeWorkSite::Write,
                });
                let result = location.write(&file, &buf).map(|n| (buf, n));
                drop(file);
                FilesystemOperationResult::new(result, operation)
            })
        });
        let result = match Pin::new(&mut *task).poll(cx) {
            // If cancellation is requested, then flag that to Tokio. Note that
            // this still waits for the actual completion of the spawned task,
            // which won't actually happen if it's already executing.
            Poll::Pending if finish => {
                task.abort();
                ready!(Pin::new(task).poll(cx))
            }
            other => ready!(other),
        };
        self.task = None;
        match result.map(FilesystemOperationResult::into_inner) {
            Ok(Ok((buf, n))) => {
                src.mark_read(n);
                self.buffer = buf;
                self.buffer.clear();
                Poll::Ready(Ok(self.complete_write(n)))
            }
            Ok(Err(err)) => {
                self.close(Err(err.into()));
                Poll::Ready(Ok(StreamResult::Dropped))
            }
            Err(err) => {
                if err.is_cancelled() {
                    return Poll::Ready(Ok(StreamResult::Cancelled));
                }
                self.close(Err(ErrorCode::Io));
                panic!("I/O task should not panic: {err}")
            }
        }
    }
}

impl Drop for WriteStreamConsumer {
    fn drop(&mut self) {
        if self.result.is_some() {
            self.close(Ok(()))
        }
    }
}

impl types::Host for WasiFilesystemCtxView<'_> {
    fn convert_error_code(&mut self, error: FilesystemError) -> wasmtime::Result<ErrorCode> {
        error.downcast()
    }
}

pub(super) fn finish_stream_factory<U, T>(
    store: &mut Access<'_, U, WasiFilesystem>,
    mut status: FutureReader<Result<(), ErrorCode>>,
    stream: wasmtime::Result<StreamReader<T>>,
) -> wasmtime::Result<(StreamReader<T>, FutureReader<Result<(), ErrorCode>>)> {
    match stream {
        Ok(stream) => Ok((stream, status)),
        Err(err) => {
            status
                .close(&mut *store)
                .context("failed to close filesystem status after stream construction failed")?;
            Err(err)
        }
    }
}

pub(super) fn finish_pipe_factory<U>(
    store: &mut Access<'_, U, WasiFilesystem>,
    mut status: FutureReader<Result<(), ErrorCode>>,
    pipe: wasmtime::Result<()>,
) -> wasmtime::Result<FutureReader<Result<(), ErrorCode>>> {
    match pipe {
        Ok(()) => Ok(status),
        Err(err) => {
            status
                .close(&mut *store)
                .context("failed to close filesystem status after stream pipe failed")?;
            Err(err)
        }
    }
}

impl<U> types::HostDescriptorWithStore<U> for WasiFilesystem {
    fn read_via_stream(
        mut store: Access<U, Self>,
        fd: Resource<Descriptor>,
        offset: Filesize,
    ) -> wasmtime::Result<(StreamReader<u8>, FutureReader<Result<(), ErrorCode>>)> {
        let file = match get_descriptor(store.get().table, &fd)? {
            Descriptor::File(file) => file.clone(),
            Descriptor::Dir(_) => {
                let status = FutureReader::new(&mut store, async move {
                    wasmtime::error::Ok(Err(ErrorCode::IsDirectory))
                })?;
                let stream = StreamReader::new(&mut store, iter::empty());
                return finish_stream_factory(&mut store, status, stream);
            }
        };
        let operation = match start_operation(store.get().ctx) {
            Ok(operation) => operation,
            Err(err) => {
                let status =
                    FutureReader::new(&mut store, async move { wasmtime::error::Ok(Err(err)) })?;
                let stream = StreamReader::new(&mut store, iter::empty());
                return finish_stream_factory(&mut store, status, stream);
            }
        };
        let (result_tx, result_rx) = oneshot::channel();
        let status = FutureReader::new(&mut store, result_rx)?;
        let stream = StreamReader::new(
            &mut store,
            ReadStreamProducer {
                file,
                offset,
                result: Some(result_tx),
                operation,
                task: None,
            },
        );
        finish_stream_factory(&mut store, status, stream)
    }

    fn write_via_stream(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
        mut data: StreamReader<u8>,
        offset: Filesize,
    ) -> wasmtime::Result<FutureReader<Result<(), ErrorCode>>> {
        let file = get_file(store.get().table, &fd).and_then(|file| {
            if file.perms.write_not_permitted() {
                Err(ErrorCode::NotPermitted.into())
            } else {
                Ok(file.clone())
            }
        });
        let operation = match &file {
            Ok(_) => start_operation(store.get().ctx).map_err(FilesystemError::from),
            Err(_) => Ok(None),
        };
        let (result_tx, result_rx) = oneshot::channel();
        let mut status = match FutureReader::new(&mut store, result_rx) {
            Ok(status) => status,
            Err(err) => {
                data.close(&mut store)?;
                return Err(err);
            }
        };
        match file.and_then(|file| operation.map(|operation| (file, operation))) {
            Ok((file, operation)) => {
                let pipe = data.pipe(
                    &mut store,
                    WriteStreamConsumer::new_at(file, offset, result_tx, operation),
                );
                finish_pipe_factory(&mut store, status, pipe)
            }
            Err(err) => {
                if let Err(close_err) = data.close(&mut store) {
                    status.close(&mut store)?;
                    return Err(close_err);
                }
                let _ = result_tx.send(Err(err.downcast().unwrap_or(ErrorCode::Io)));
                Ok(status)
            }
        }
    }

    fn append_via_stream(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
        mut data: StreamReader<u8>,
    ) -> wasmtime::Result<FutureReader<Result<(), ErrorCode>>> {
        let file = get_file(store.get().table, &fd).and_then(|file| {
            if file.perms.write_not_permitted() {
                Err(ErrorCode::NotPermitted.into())
            } else {
                Ok(file.clone())
            }
        });
        let operation = match &file {
            Ok(_) => start_operation(store.get().ctx).map_err(FilesystemError::from),
            Err(_) => Ok(None),
        };
        let (result_tx, result_rx) = oneshot::channel();
        let mut status = match FutureReader::new(&mut store, result_rx) {
            Ok(status) => status,
            Err(err) => {
                data.close(&mut store)?;
                return Err(err);
            }
        };
        match file.and_then(|file| operation.map(|operation| (file, operation))) {
            Ok((file, operation)) => {
                let pipe = data.pipe(
                    &mut store,
                    WriteStreamConsumer::new_append(file, result_tx, operation),
                );
                finish_pipe_factory(&mut store, status, pipe)
            }
            Err(err) => {
                if let Err(close_err) = data.close(&mut store) {
                    status.close(&mut store)?;
                    return Err(close_err);
                }
                let _ = result_tx.send(Err(err.downcast().unwrap_or(ErrorCode::Io)));
                Ok(status)
            }
        }
    }

    async fn advise(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        offset: Filesize,
        length: Filesize,
        advice: Advice,
    ) -> FilesystemResult<()> {
        let file = store.get_file(&fd)?;
        let operation = start_accessor_operation(store)?;
        file.advise(operation.clone(), offset, length, advice.into())
            .await?;
        Ok(())
    }

    async fn sync_data(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FilesystemResult<()> {
        let fd = store.get_descriptor(&fd)?;
        let operation = start_accessor_operation(store)?;
        fd.sync_data(operation.clone()).await?;
        Ok(())
    }

    async fn get_flags(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FilesystemResult<DescriptorFlags> {
        let fd = store.get_descriptor(&fd)?;
        let operation = start_accessor_operation(store)?;
        let flags = fd.get_flags(operation.clone()).await?;
        Ok(flags.into())
    }

    async fn get_type(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FilesystemResult<DescriptorType> {
        let fd = store.get_descriptor(&fd)?;
        let operation = start_accessor_operation(store)?;
        let ty = fd.get_type(operation.clone()).await?;
        Ok(ty.into())
    }

    async fn set_size(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        size: Filesize,
    ) -> FilesystemResult<()> {
        let file = store.get_file(&fd)?;
        let operation = start_accessor_operation(store)?;
        file.set_size(operation.clone(), size).await?;
        Ok(())
    }

    async fn set_times(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        data_access_timestamp: NewTimestamp,
        data_modification_timestamp: NewTimestamp,
    ) -> FilesystemResult<()> {
        let fd = store.get_descriptor(&fd)?;
        let atim = systemtimespec_from(data_access_timestamp)?;
        let mtim = systemtimespec_from(data_modification_timestamp)?;
        let operation = start_accessor_operation(store)?;
        fd.set_times(operation.clone(), atim, mtim).await?;
        Ok(())
    }

    fn read_directory(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
    ) -> wasmtime::Result<(
        StreamReader<DirectoryEntry>,
        FutureReader<Result<(), ErrorCode>>,
    )> {
        let dir = match get_dir(store.get().table, &fd) {
            Ok(dir) => dir.clone(),
            Err(err) => {
                let err = err.downcast().unwrap_or(ErrorCode::Io);
                let status =
                    FutureReader::new(&mut store, async move { wasmtime::error::Ok(Err(err)) })?;
                let stream = StreamReader::new(&mut store, iter::empty());
                return finish_stream_factory(&mut store, status, stream);
            }
        };
        let operation = match start_operation(store.get().ctx) {
            Ok(operation) => operation,
            Err(err) => {
                let status =
                    FutureReader::new(&mut store, async move { wasmtime::error::Ok(Err(err)) })?;
                let stream = StreamReader::new(&mut store, iter::empty());
                return finish_stream_factory(&mut store, status, stream);
            }
        };
        let allow_blocking_current_thread = dir.allow_blocking_current_thread;
        let dir = Arc::clone(dir.as_dir());

        if allow_blocking_current_thread {
            // Perform the eager native setup before installing concurrent
            // resources. A panic therefore cannot strand a status future in
            // the Store.
            #[cfg(test)]
            super::lifecycle_tests::native_work_gate(
                super::lifecycle_tests::NativeWorkSite::ReadDirectory,
            );
            let readdir = match cap_primitives::fs::read_base_dir(&dir) {
                Ok(readdir) => readdir,
                Err(err) => {
                    let err = ErrorCode::from(err);
                    let status =
                        FutureReader::new(
                            &mut store,
                            async move { wasmtime::error::Ok(Err(err)) },
                        )?;
                    let stream = StreamReader::new(&mut store, iter::empty());
                    return finish_stream_factory(&mut store, status, stream);
                }
            };
            let (result_tx, result_rx) = oneshot::channel();
            let status = FutureReader::new(&mut store, result_rx)?;
            let stream = StreamReader::new(
                &mut store,
                LifecycleStreamProducer::new(
                    FallibleIteratorProducer::new(
                        readdir.filter_map(|e| map_dir_entry(e).transpose()),
                        result_tx,
                    ),
                    operation,
                ),
            );
            finish_stream_factory(&mut store, status, stream)
        } else {
            let (result_tx, result_rx) = oneshot::channel();
            let status = FutureReader::new(&mut store, result_rx)?;
            let stream =
                StreamReader::new(&mut store, ReadDirStream::new(dir, result_tx, operation));
            finish_stream_factory(&mut store, status, stream)
        }
    }

    async fn sync(store: &Accessor<U, Self>, fd: Resource<Descriptor>) -> FilesystemResult<()> {
        let fd = store.get_descriptor(&fd)?;
        let operation = start_accessor_operation(store)?;
        fd.sync(operation.clone()).await?;
        Ok(())
    }

    async fn create_directory_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FilesystemResult<()> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        dir.create_directory_at(operation.clone(), path).await?;
        Ok(())
    }

    async fn stat(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FilesystemResult<DescriptorStat> {
        let fd = store.get_descriptor(&fd)?;
        let operation = start_accessor_operation(store)?;
        let stat = fd.stat(operation.clone()).await?;
        Ok(stat.into())
    }

    async fn stat_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: PathFlags,
        path: String,
    ) -> FilesystemResult<DescriptorStat> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        let stat = dir
            .stat_at(operation.clone(), path_flags.into(), path)
            .await?;
        Ok(stat.into())
    }

    async fn set_times_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: PathFlags,
        path: String,
        data_access_timestamp: NewTimestamp,
        data_modification_timestamp: NewTimestamp,
    ) -> FilesystemResult<()> {
        let dir = store.get_dir(&fd)?;
        let atim = systemtimespec_from(data_access_timestamp)?;
        let mtim = systemtimespec_from(data_modification_timestamp)?;
        let operation = start_accessor_operation(store)?;
        dir.set_times_at(operation.clone(), path_flags.into(), path, atim, mtim)
            .await?;
        Ok(())
    }

    async fn link_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        old_path_flags: PathFlags,
        old_path: String,
        new_fd: Resource<Descriptor>,
        new_path: String,
    ) -> FilesystemResult<()> {
        let (old_dir, new_dir) = store.get_dir_pair(&fd, &new_fd)?;
        let operation = start_accessor_operation(store)?;
        old_dir
            .link_at(
                operation.clone(),
                old_path_flags.into(),
                old_path,
                &new_dir,
                new_path,
            )
            .await?;
        Ok(())
    }

    async fn open_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: PathFlags,
        path: String,
        open_flags: OpenFlags,
        flags: DescriptorFlags,
    ) -> FilesystemResult<Resource<Descriptor>> {
        let (allow_blocking_current_thread, dir, operation) = store.with(|mut store| {
            let store = store.get();
            let dir = get_dir(&store.table, &fd)?;
            let operation = start_operation(store.ctx)?;
            FilesystemResult::Ok((
                store.ctx.allow_blocking_current_thread,
                dir.clone(),
                operation,
            ))
        })?;
        let fd = dir
            .open_at(
                operation.clone(),
                path_flags.into(),
                path,
                open_flags.into(),
                flags.into(),
                allow_blocking_current_thread,
            )
            .await?;
        let fd = store.with(|mut store| store.get().table.push(fd))?;
        Ok(fd)
    }

    async fn readlink_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FilesystemResult<String> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        let path = dir.readlink_at(operation.clone(), path).await?;
        Ok(path)
    }

    async fn remove_directory_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FilesystemResult<()> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        dir.remove_directory_at(operation.clone(), path).await?;
        Ok(())
    }

    async fn rename_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        old_path: String,
        new_fd: Resource<Descriptor>,
        new_path: String,
    ) -> FilesystemResult<()> {
        let (old_dir, new_dir) = store.get_dir_pair(&fd, &new_fd)?;
        let operation = start_accessor_operation(store)?;
        old_dir
            .rename_at(operation.clone(), old_path, &new_dir, new_path)
            .await?;
        Ok(())
    }

    async fn symlink_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        old_path: String,
        new_path: String,
    ) -> FilesystemResult<()> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        dir.symlink_at(operation.clone(), old_path, new_path)
            .await?;
        Ok(())
    }

    async fn unlink_file_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FilesystemResult<()> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        dir.unlink_file_at(operation.clone(), path).await?;
        Ok(())
    }

    async fn is_same_object(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        other: Resource<Descriptor>,
    ) -> wasmtime::Result<bool> {
        let (fd, other, operation) = store.with(|mut store| {
            let store = store.get();
            let table = store.table;
            let fd = get_descriptor(table, &fd)?.clone();
            let other = get_descriptor(table, &other)?.clone();
            let operation = store.ctx.start_operation()?;
            wasmtime::error::Ok((fd, other, operation))
        })?;
        fd.is_same_object(operation.clone(), &other).await
    }

    async fn metadata_hash(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FilesystemResult<MetadataHashValue> {
        let fd = store.get_descriptor(&fd)?;
        let operation = start_accessor_operation(store)?;
        let meta = fd.metadata_hash(operation.clone()).await?;
        Ok(meta.into())
    }

    async fn metadata_hash_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: PathFlags,
        path: String,
    ) -> FilesystemResult<MetadataHashValue> {
        let dir = store.get_dir(&fd)?;
        let operation = start_accessor_operation(store)?;
        let meta = dir
            .metadata_hash_at(operation.clone(), path_flags.into(), path)
            .await?;
        Ok(meta.into())
    }
}

impl types::HostDescriptor for WasiFilesystemCtxView<'_> {
    fn drop(&mut self, fd: Resource<Descriptor>) -> wasmtime::Result<()> {
        self.table
            .delete(fd)
            .context("failed to delete descriptor resource from table")?;
        Ok(())
    }
}

impl preopens::Host for WasiFilesystemCtxView<'_> {
    fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        self.get_directories()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::{FsPerms, OpenMode};
    use crate::p3::filesystem::lifecycle_tests::{InstalledGate, NativeWorkSite, TEST_LOCK};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use wasmtime::component::{Access, ResourceTable};
    use wasmtime::{AsContextMut, Config, Engine, Store};

    struct TestCtx {
        filesystem: WasiFilesystemCtx,
        table: ResourceTable,
    }

    fn filesystem(ctx: &mut TestCtx) -> WasiFilesystemCtxView<'_> {
        WasiFilesystemCtxView {
            ctx: &mut ctx.filesystem,
            table: &mut ctx.table,
        }
    }

    fn store(ctx: TestCtx) -> Store<TestCtx> {
        let mut config = Config::new();
        config.wasm_component_model_async(true);
        Store::new(&Engine::new(&config).unwrap(), ctx)
    }

    struct CountDrop {
        count: Arc<AtomicUsize>,
        dropped: mpsc::Sender<()>,
    }

    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.count.fetch_add(1, Ordering::SeqCst);
            let _ = self.dropped.send(());
        }
    }

    #[test]
    fn terminal_overflow_releases_session_before_producer_drop() {
        let count = Arc::new(AtomicUsize::new(0));
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (result_tx, mut result_rx) = oneshot::channel();
        let mut producer = ReadStreamProducer {
            file: File::new(
                tempfile::tempfile().unwrap(),
                FsPerms::ReadOnly,
                OpenMode::READ,
                false,
            ),
            offset: u64::MAX,
            result: Some(result_tx),
            operation: Some(FilesystemOperation::new(CountDrop {
                count: count.clone(),
                dropped: dropped_tx,
            })),
            task: None,
        };

        assert!(matches!(producer.complete_read(1), StreamResult::Dropped));
        assert!(matches!(result_rx.try_recv(), Ok(Err(ErrorCode::Overflow))));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(matches!(dropped_rx.try_recv(), Ok(())));

        // Keep the producer alive through the terminal assertions. Its later
        // destructor must not be what releases the operation, or release it a
        // second time.
        drop(producer);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_factory_failures_close_status_and_retain_started_native_work() {
        let _lock = TEST_LOCK.lock().await;
        let gate = InstalledGate::new(NativeWorkSite::ReadDirectory, 1);
        let directory = tempfile::tempdir().unwrap();
        let dir = Arc::new(std::fs::File::open(directory.path()).unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let operation = FilesystemOperation::new(CountDrop {
            count: count.clone(),
            dropped: dropped_tx,
        });
        let mut store = store(TestCtx {
            filesystem: WasiFilesystemCtx::default(),
            table: ResourceTable::new(),
        });
        let baseline = store.concurrent_state_table_size();

        {
            let mut access =
                Access::<TestCtx, WasiFilesystem>::new(store.as_context_mut(), filesystem);
            let (result_tx, result_rx) = oneshot::channel();
            let status = FutureReader::new(&mut access, result_rx).unwrap();
            assert!(
                access.as_context_mut().concurrent_state_table_size() > baseline,
                "status future did not install concurrent state"
            );
            let producer = ReadDirStream::new(dir, result_tx, Some(operation));
            assert_eq!(gate.wait_started(), 1);

            let failed_stream: wasmtime::Result<StreamReader<DirectoryEntry>> = {
                drop(producer);
                Err(wasmtime::format_err!(
                    "injected stream construction failure"
                ))
            };
            assert!(finish_stream_factory(&mut access, status, failed_stream).is_err());
            assert_eq!(
                access.as_context_mut().concurrent_state_table_size(),
                baseline
            );
        }

        assert_eq!(count.load(Ordering::SeqCst), 0);
        gate.release_one();
        dropped_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("started directory work did not release its operation");
        assert_eq!(count.load(Ordering::SeqCst), 1);

        {
            let mut access =
                Access::<TestCtx, WasiFilesystem>::new(store.as_context_mut(), filesystem);
            let status = FutureReader::new(&mut access, async {
                wasmtime::error::Ok(Ok::<_, ErrorCode>(()))
            })
            .unwrap();
            assert!(
                access.as_context_mut().concurrent_state_table_size() > baseline,
                "status future did not install concurrent state"
            );
            assert!(
                finish_pipe_factory(
                    &mut access,
                    status,
                    Err(wasmtime::format_err!("injected pipe failure")),
                )
                .is_err()
            );
            assert_eq!(
                access.as_context_mut().concurrent_state_table_size(),
                baseline
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_readdir_panic_installs_no_concurrent_state() {
        let _lock = TEST_LOCK.lock().await;
        let gate = InstalledGate::new_panicking(NativeWorkSite::ReadDirectory);
        let directory = tempfile::tempdir().unwrap();
        let descriptor = Descriptor::Dir(Dir::new(
            std::fs::File::open(directory.path()).unwrap(),
            FsPerms::ReadOnly,
            OpenMode::READ,
            true,
        ));
        let count = Arc::new(AtomicUsize::new(0));
        let (dropped_tx, _dropped_rx) = mpsc::channel();
        let count_for_hook = count.clone();
        let mut filesystem_ctx = WasiFilesystemCtx::default();
        filesystem_ctx.set_operation_hook(move || {
            Ok(FilesystemOperation::new(CountDrop {
                count: count_for_hook.clone(),
                dropped: dropped_tx.clone(),
            }))
        });
        let mut table = ResourceTable::new();
        let fd = table.push(descriptor).unwrap();
        let mut store = store(TestCtx {
            filesystem: filesystem_ctx,
            table,
        });
        let baseline = store.concurrent_state_table_size();

        gate.release_one();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let access = Access::<TestCtx, WasiFilesystem>::new(store.as_context_mut(), filesystem);
            <WasiFilesystem as types::HostDescriptorWithStore<TestCtx>>::read_directory(access, fd)
        }));
        assert!(result.is_err());
        assert_eq!(gate.wait_started(), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(store.concurrent_state_table_size(), baseline);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn p3_rejection_is_busy_and_is_same_object_preserves_the_typed_error() {
        let _lock = TEST_LOCK.lock().await;
        let file = tempfile::tempfile().unwrap();
        let descriptor =
            Descriptor::File(File::new(file, FsPerms::ReadOnly, OpenMode::READ, false));
        let mut table = ResourceTable::new();
        let fd = table.push(descriptor).unwrap();
        let mut filesystem = WasiFilesystemCtx::default();
        filesystem.set_operation_hook(|| Err(FilesystemOperationError::Rejected));
        let mut store = store(TestCtx { filesystem, table });

        let stat = store
            .run_concurrent(async move |store| {
                let store = store.with_getter::<WasiFilesystem>(self::filesystem);
                <WasiFilesystem as types::HostDescriptorWithStore<TestCtx>>::stat(&store, fd).await
            })
            .await
            .unwrap();
        assert!(matches!(
            stat.unwrap_err().downcast().unwrap(),
            ErrorCode::Busy
        ));

        let fd = store
            .data_mut()
            .table
            .push(Descriptor::File(File::new(
                tempfile::tempfile().unwrap(),
                FsPerms::ReadOnly,
                OpenMode::READ,
                false,
            )))
            .unwrap();
        let other = store
            .data_mut()
            .table
            .push(Descriptor::File(File::new(
                tempfile::tempfile().unwrap(),
                FsPerms::ReadOnly,
                OpenMode::READ,
                false,
            )))
            .unwrap();
        let err = store
            .run_concurrent(async move |store| {
                let store = store.with_getter::<WasiFilesystem>(self::filesystem);
                <WasiFilesystem as types::HostDescriptorWithStore<TestCtx>>::is_same_object(
                    &store, fd, other,
                )
                .await
            })
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<FilesystemOperationError>(),
            Some(FilesystemOperationError::Rejected)
        ));
    }
}

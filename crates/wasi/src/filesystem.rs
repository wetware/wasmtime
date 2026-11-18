use crate::clocks::Datetime;
use crate::runtime::{AbortOnDropJoinHandle, spawn_blocking};
use cap_primitives::fs::{DirOptions, FollowSymlinks, Metadata, OpenOptions, SystemTimeSpec};
use std::collections::hash_map;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tracing::debug;
use wasmtime::component::{HasData, Resource, ResourceTable};
use wasmtime::error::Context as _;

#[cfg(unix)]
pub(crate) mod unix;
#[cfg(unix)]
pub(crate) use unix as sys;
#[cfg(windows)]
pub(crate) mod windows;
#[cfg(windows)]
pub(crate) use windows as sys;

/// A helper struct which implements [`HasData`] for the `wasi:filesystem` APIs.
///
/// This can be useful when directly calling `add_to_linker` functions directly,
/// such as [`wasmtime_wasi::p2::bindings::filesystem::types::add_to_linker`] as
/// the `D` type parameter. See [`HasData`] for more information about the type
/// parameter's purpose.
///
/// When using this type you can skip the [`WasiFilesystemView`] trait, for
/// example.
///
/// [`wasmtime_wasi::p2::bindings::filesystem::types::add_to_linker`]: crate::p2::bindings::filesystem::types::add_to_linker
///
/// # Examples
///
/// ```
/// use wasmtime::component::{Linker, ResourceTable};
/// use wasmtime::{Engine, Result};
/// use wasmtime_wasi::filesystem::*;
///
/// struct MyStoreState {
///     table: ResourceTable,
///     filesystem: WasiFilesystemCtx,
/// }
///
/// fn main() -> Result<()> {
///     let engine = Engine::default();
///     let mut linker = Linker::new(&engine);
///
///     wasmtime_wasi::p2::bindings::filesystem::types::add_to_linker::<MyStoreState, WasiFilesystem>(
///         &mut linker,
///         |state| WasiFilesystemCtxView {
///             table: &mut state.table,
///             ctx: &mut state.filesystem,
///         },
///     )?;
///     Ok(())
/// }
/// ```
pub struct WasiFilesystem;

impl HasData for WasiFilesystem {
    type Data<'a> = WasiFilesystemCtxView<'a>;
}

/// Shared ownership of one admitted native filesystem operation or session.
///
/// Cloning shares the admission; it does not call the hook again. Wasmtime never
/// accesses the value, and its destructor can run on a native worker thread.
#[derive(Clone)]
pub struct FilesystemOperation {
    _inner: Arc<dyn Send + Sync>,
}

impl FilesystemOperation {
    /// Retains `value` until the operation's last owner is released.
    /// A mutex allows values which are `Send` without requiring `Sync`.
    pub fn new(value: impl Send + 'static) -> Self {
        Self {
            _inner: Arc::new(Mutex::new(value)),
        }
    }
}

/// An embedder declined to start a native filesystem operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FilesystemOperationError {
    /// Reported as `again` (P1), `would-block` (P2), or `busy` (P3).
    /// Operations without an error result, such as `is-same-object`, trap.
    #[error("filesystem operation rejected")]
    Rejected,
}

impl From<FilesystemOperationError> for std::io::Error {
    fn from(error: FilesystemOperationError) -> Self {
        Self::new(std::io::ErrorKind::WouldBlock, error)
    }
}

pub(crate) type FilesystemOperationHook =
    Arc<dyn Fn() -> Result<FilesystemOperation, FilesystemOperationError> + Send + Sync>;

#[derive(Clone, Default)]
pub struct WasiFilesystemCtx {
    pub(crate) allow_blocking_current_thread: bool,
    // Close stored preopens before releasing the factory's captured owners.
    pub(crate) preopens: Vec<(Dir, String)>,
    operation_hook: Option<FilesystemOperationHook>,
}

impl WasiFilesystemCtx {
    /// Sets an optional admission and lifetime hook for native filesystem work.
    ///
    /// The callback must return promptly. Each ordinary operation and each P3
    /// stream session calls it once before native work starts. Successful values
    /// stay alive through native execution, even after the initiating future,
    /// resource, or [`wasmtime::Store`] is dropped. Sessions retain their value
    /// across serial work until completion or disposal. Undelivered native
    /// results retain ownership through disposal; completed P2 readiness/results
    /// containing only inert data do not wait for guest polling to release it.
    /// Idle descriptors do not retain active-operation values.
    ///
    /// To retain backing content, capture a shared content owner in the hook
    /// and clone that owner into each returned [`FilesystemOperation`]. The
    /// retained hook owns idle content; the returned value owns content and
    /// admission during native work. Operations involving two descriptors invoke
    /// only the initiating descriptor's hook and retain the secondary
    /// descriptor, including its hook, until native work finishes.
    ///
    /// Descriptors retain the hook configured when their preopen was issued;
    /// changing the hook affects subsequently issued preopens and their children.
    /// A context also retains its configured hook. For independently owned
    /// custom descriptors, a temporary context can attach each descriptor's hook
    /// with [`Self::push_descriptor`] and then be dropped.
    /// When replacing a hook, the replacement or another explicit owner must
    /// retain any backing content still needed by the context's stored preopens.
    ///
    /// Cloning [`File::file`] or [`Dir::dir`], or duplicating their native handles,
    /// does not retain the hook or its captured owners. Embedders must keep an
    /// unchanged clone of the complete [`Descriptor`], or an explicit content
    /// owner, until every extracted native handle closes. In an embedder-owned
    /// struct, declare the native field before the retained descriptor or owner
    /// so the native field drops first. Do not let the native field escape that
    /// owner or replace the retained descriptor's native field or hook.
    ///
    /// Without a hook, no operation value is allocated or reference counted.
    pub fn set_operation_hook<F>(&mut self, hook: F) -> &mut Self
    where
        F: Fn() -> Result<FilesystemOperation, FilesystemOperationError> + Send + Sync + 'static,
    {
        self.operation_hook = Some(Arc::new(hook));
        self
    }

    /// Installs a descriptor using this context's filesystem operation hook.
    ///
    /// Use this instead of [`ResourceTable::push`] when supplying custom
    /// descriptors constructed with [`File::new`] or [`Dir::new`]. The descriptor
    /// and any children retain the currently configured hook. Later changes to
    /// this context do not change already installed descriptors or streams.
    /// This replaces any hook previously attached to the supplied descriptor.
    /// When reattaching an existing descriptor, the replacement hook or another
    /// explicit owner must retain the descriptor's backing content.
    pub fn push_descriptor(
        &self,
        table: &mut ResourceTable,
        mut descriptor: Descriptor,
    ) -> Result<Resource<Descriptor>, wasmtime::component::ResourceTableError> {
        match &mut descriptor {
            Descriptor::File(file) => file.operation_hook = self.operation_hook.clone(),
            Descriptor::Dir(dir) => dir.operation_hook = self.operation_hook.clone(),
        }
        table.push(descriptor)
    }
}

// Field order ensures native values and captured handles are disposed before
// the last operation owner, including during unwinding and abandoned results.
pub(crate) struct NativeOperation<T> {
    pub(crate) value: T,
    pub(crate) operation: Option<FilesystemOperation>,
}

impl<T> NativeOperation<T> {
    pub(crate) fn new(value: T, operation: Option<FilesystemOperation>) -> Self {
        Self { value, operation }
    }

    pub(crate) fn run<R>(mut self, body: impl FnOnce(&mut T) -> R) -> R {
        body(&mut self.value)
    }
}

impl NativeOperation<Descriptor> {
    pub(crate) fn push(
        self,
        table: &mut ResourceTable,
    ) -> Result<Resource<Descriptor>, wasmtime::component::ResourceTableError> {
        let Self {
            value,
            operation: _operation,
        } = self;
        table.push(value)
    }
}

pub struct WasiFilesystemCtxView<'a> {
    pub ctx: &'a mut WasiFilesystemCtx,
    pub table: &'a mut ResourceTable,
}

pub trait WasiFilesystemView: Send {
    fn filesystem(&mut self) -> WasiFilesystemCtxView<'_>;
}

/// Permission bits for operating on filesystem, specified per preopen,
/// as enforced by wasmtime-wasi.
///
/// Filesystems can deny all mutation operations (read-only) or permit
/// mutations (read-write).
///
/// Read-only permissions allow reading the contents
/// of any file or directory reachable under the preopen, as well as reading
/// any file metadata. Changing, appending, or truncating files is not
/// permitted. Creating or deleting files, directories, symbolic links, and
/// hard links are not permitted.
///
/// Read-write permissions include changing the contents of any reachable
/// file, creating and deleting files, directories, symbolic links, and
/// creating hard links, as well as mutating any file metadata.
///
/// These permissions are enforced by wasmtime-wasi. The host filesystem may
/// enforce additional restrictions not covered by these.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FsPerms {
    // Only read operations are permitted - no mutation permitted
    ReadOnly,
    // All operations are permitted.
    ReadWrite,
}

impl FsPerms {
    /// Tests whether writes are not permitted, returning a boolean. Shorthand
    /// for matches!(perms, FsPerms::ReadOnly), used frequently in
    /// if-statements.
    pub fn write_not_permitted(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct OpenMode: usize {
        const READ = 0b1;
        const WRITE = 0b10;
    }
}

bitflags::bitflags! {
    /// Flags determining the method of how paths are resolved.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub(crate) struct PathFlags: usize {
        /// This directory can be read, for example its entries can be iterated
        /// over and files can be opened.
        const SYMLINK_FOLLOW = 0b1;
    }
}

bitflags::bitflags! {
    /// Open flags used by `open-at`.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub(crate) struct OpenFlags: usize {
        /// Create file if it does not exist, similar to `O_CREAT` in POSIX.
        const CREATE = 0b1;
        /// Fail if not a directory, similar to `O_DIRECTORY` in POSIX.
        const DIRECTORY = 0b10;
        /// Fail if file already exists, similar to `O_EXCL` in POSIX.
        const EXCLUSIVE = 0b100;
        /// Truncate file to size 0, similar to `O_TRUNC` in POSIX.
        const TRUNCATE = 0b1000;
    }
}

bitflags::bitflags! {
    /// Descriptor flags.
    ///
    /// Note: This was called `fdflags` in earlier versions of WASI.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub(crate) struct DescriptorFlags: usize {
        /// Read mode: Data can be read.
        const READ = 0b1;
        /// Write mode: Data can be written to.
        const WRITE = 0b10;
        /// Request that writes be performed according to synchronized I/O file
        /// integrity completion. The data stored in the file and the file's
        /// metadata are synchronized. This is similar to `O_SYNC` in POSIX.
        ///
        /// The precise semantics of this operation have not yet been defined for
        /// WASI. At this time, it should be interpreted as a request, and not a
        /// requirement.
        const FILE_INTEGRITY_SYNC = 0b100;
        /// Request that writes be performed according to synchronized I/O data
        /// integrity completion. Only the data stored in the file is
        /// synchronized. This is similar to `O_DSYNC` in POSIX.
        ///
        /// The precise semantics of this operation have not yet been defined for
        /// WASI. At this time, it should be interpreted as a request, and not a
        /// requirement.
        const DATA_INTEGRITY_SYNC = 0b1000;
        /// Requests that reads be performed at the same level of integrity
        /// requested for writes. This is similar to `O_RSYNC` in POSIX.
        ///
        /// The precise semantics of this operation have not yet been defined for
        /// WASI. At this time, it should be interpreted as a request, and not a
        /// requirement.
        const REQUESTED_WRITE_SYNC = 0b10000;
        /// Mutating directories mode: Directory contents may be mutated.
        ///
        /// When this flag is unset on a descriptor, operations using the
        /// descriptor which would create, rename, delete, modify the data or
        /// metadata of filesystem objects, or obtain another handle which
        /// would permit any of those, shall fail with `error-code::read-only` if
        /// they would otherwise succeed.
        ///
        /// This may only be set on directories.
        const MUTATE_DIRECTORY = 0b100000;
    }
}

/// Error codes returned by functions, similar to `errno` in POSIX.
/// Not all of these error codes are returned by the functions provided by this
/// API; some are used in higher-level library layers, and others are provided
/// merely for alignment with POSIX.
#[cfg_attr(
    windows,
    expect(dead_code, reason = "on Windows, some of these are not used")
)]
pub(crate) enum ErrorCode {
    /// Permission denied, similar to `EACCES` in POSIX.
    Access,
    /// Connection already in progress, similar to `EALREADY` in POSIX.
    Already,
    /// Bad descriptor, similar to `EBADF` in POSIX.
    BadDescriptor,
    /// Device or resource busy, similar to `EBUSY` in POSIX.
    Busy,
    /// File exists, similar to `EEXIST` in POSIX.
    Exist,
    /// File too large, similar to `EFBIG` in POSIX.
    FileTooLarge,
    /// Illegal byte sequence, similar to `EILSEQ` in POSIX.
    IllegalByteSequence,
    /// Operation in progress, similar to `EINPROGRESS` in POSIX.
    InProgress,
    /// Interrupted function, similar to `EINTR` in POSIX.
    Interrupted,
    /// Invalid argument, similar to `EINVAL` in POSIX.
    Invalid,
    /// I/O error, similar to `EIO` in POSIX.
    Io,
    /// Is a directory, similar to `EISDIR` in POSIX.
    IsDirectory,
    /// Too many levels of symbolic links, similar to `ELOOP` in POSIX.
    Loop,
    /// Too many links, similar to `EMLINK` in POSIX.
    TooManyLinks,
    /// Filename too long, similar to `ENAMETOOLONG` in POSIX.
    NameTooLong,
    /// No such file or directory, similar to `ENOENT` in POSIX.
    NoEntry,
    /// Not enough space, similar to `ENOMEM` in POSIX.
    InsufficientMemory,
    /// No space left on device, similar to `ENOSPC` in POSIX.
    InsufficientSpace,
    /// Not a directory or a symbolic link to a directory, similar to `ENOTDIR` in POSIX.
    NotDirectory,
    /// Directory not empty, similar to `ENOTEMPTY` in POSIX.
    NotEmpty,
    /// Not supported, similar to `ENOTSUP` and `ENOSYS` in POSIX.
    Unsupported,
    /// Value too large to be stored in data type, similar to `EOVERFLOW` in POSIX.
    Overflow,
    /// Operation not permitted, similar to `EPERM` in POSIX.
    NotPermitted,
    /// Broken pipe, similar to `EPIPE` in POSIX.
    Pipe,
    /// Invalid seek, similar to `ESPIPE` in POSIX.
    InvalidSeek,
    /// Resource unavailable, or operation would block, similar to `EAGAIN` and
    /// `EWOULDBLOCK` in POSIX.
    WouldBlock,
}

impl From<FilesystemOperationError> for ErrorCode {
    fn from(_: FilesystemOperationError) -> Self {
        Self::WouldBlock
    }
}

/// The type of a filesystem object referenced by a descriptor.
///
/// Note: This was called `filetype` in earlier versions of WASI.
pub(crate) enum DescriptorType {
    /// The type of the descriptor or file is unknown or is different from
    /// any of the other types specified.
    Unknown,
    /// The descriptor refers to a block device inode.
    #[cfg_attr(
        windows,
        expect(dead_code, reason = "windows has no notion of block devices")
    )]
    BlockDevice,
    /// The descriptor refers to a character device inode.
    CharacterDevice,
    /// The descriptor refers to a directory inode.
    Directory,
    /// The file refers to a symbolic link inode.
    SymbolicLink,
    /// The descriptor refers to a regular file inode.
    RegularFile,
}

impl From<cap_primitives::fs::FileType> for DescriptorType {
    fn from(ft: cap_primitives::fs::FileType) -> Self {
        if ft.is_dir() {
            DescriptorType::Directory
        } else if ft.is_symlink() {
            DescriptorType::SymbolicLink
        } else if ft.is_file() {
            DescriptorType::RegularFile
        } else {
            sys::descriptor_type(ft)
        }
    }
}

/// File attributes.
///
/// Note: This was called `filestat` in earlier versions of WASI.
pub(crate) struct DescriptorStat {
    /// File type.
    pub type_: DescriptorType,
    /// Number of hard links to the file.
    pub link_count: u64,
    /// For regular files, the file size in bytes. For symbolic links, the
    /// length in bytes of the pathname contained in the symbolic link.
    pub size: u64,
    /// Last data access timestamp.
    ///
    /// If the `option` is none, the platform doesn't maintain an access
    /// timestamp for this file.
    pub data_access_timestamp: Option<Datetime>,
    /// Last data modification timestamp.
    ///
    /// If the `option` is none, the platform doesn't maintain a
    /// modification timestamp for this file.
    pub data_modification_timestamp: Option<Datetime>,
    /// Last file status-change timestamp.
    ///
    /// If the `option` is none, the platform doesn't maintain a
    /// status-change timestamp for this file.
    pub status_change_timestamp: Option<Datetime>,
}

impl DescriptorStat {
    /// Creates a `DescriptorStat` from a `Metadata` plus the hard link
    /// count.
    fn new(meta: &Metadata, link_count: u64) -> Self {
        Self {
            type_: meta.file_type().into(),
            link_count,
            size: meta.len(),
            data_access_timestamp: meta
                .accessed()
                .ok()
                .and_then(|t| Datetime::try_from(t.into_std()).ok()),
            data_modification_timestamp: meta
                .modified()
                .ok()
                .and_then(|t| Datetime::try_from(t.into_std()).ok()),
            status_change_timestamp: meta
                .created()
                .ok()
                .and_then(|t| Datetime::try_from(t.into_std()).ok()),
        }
    }
}

/// A 128-bit hash value, split into parts because wasm doesn't have a
/// 128-bit integer type.
pub(crate) struct MetadataHashValue {
    /// 64 bits of a 128-bit hash value.
    pub lower: u64,
    /// Another 64 bits of a 128-bit hash value.
    pub upper: u64,
}

impl MetadataHashValue {
    /// Creates a hash value from a file's unique identity, e.g. a
    /// device/inode number pair.
    fn new(identity: impl std::hash::Hash) -> Self {
        // Without incurring any deps, std provides us with a 64 bit hash
        // function:
        use std::hash::Hasher as _;
        // Note that this means that the metadata hash (which becomes a preview1 ino) may
        // change when a different rustc release is used to build this host implementation:
        let mut hasher = hash_map::DefaultHasher::new();
        identity.hash(&mut hasher);
        let lower = hasher.finish();
        // MetadataHashValue has a pair of 64-bit members for representing a
        // single 128-bit number. However, we only have 64 bits of entropy. To
        // synthesize the upper 64 bits, lets xor the lower half with an arbitrary
        // constant, in this case the 64 bit integer corresponding to the IEEE
        // double representation of (a number as close as possible to) pi.
        // This seems better than just repeating the same bits in the upper and
        // lower parts outright, which could make folks wonder if the struct was
        // mangled in the ABI, or worse yet, lead to consumers of this interface
        // expecting them to be equal.
        let upper = lower ^ 4614256656552045848u64;
        Self { lower, upper }
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum Advice {
    Normal,
    Sequential,
    Random,
    WillNeed,
    DontNeed,
    NoReuse,
}

#[cfg(unix)]
fn from_raw_os_error(err: Option<i32>) -> Option<ErrorCode> {
    use rustix::io::Errno as RustixErrno;
    if err.is_none() {
        return None;
    }
    Some(match RustixErrno::from_raw_os_error(err.unwrap()) {
        RustixErrno::PIPE => ErrorCode::Pipe,
        RustixErrno::PERM => ErrorCode::NotPermitted,
        RustixErrno::NOENT => ErrorCode::NoEntry,
        RustixErrno::NOMEM => ErrorCode::InsufficientMemory,
        RustixErrno::IO => ErrorCode::Io,
        RustixErrno::BADF => ErrorCode::BadDescriptor,
        RustixErrno::BUSY => ErrorCode::Busy,
        RustixErrno::ACCESS => ErrorCode::Access,
        RustixErrno::NOTDIR => ErrorCode::NotDirectory,
        RustixErrno::ISDIR => ErrorCode::IsDirectory,
        RustixErrno::INVAL => ErrorCode::Invalid,
        RustixErrno::EXIST => ErrorCode::Exist,
        RustixErrno::FBIG => ErrorCode::FileTooLarge,
        RustixErrno::NOSPC => ErrorCode::InsufficientSpace,
        RustixErrno::SPIPE => ErrorCode::InvalidSeek,
        RustixErrno::MLINK => ErrorCode::TooManyLinks,
        RustixErrno::NAMETOOLONG => ErrorCode::NameTooLong,
        RustixErrno::NOTEMPTY => ErrorCode::NotEmpty,
        RustixErrno::LOOP => ErrorCode::Loop,
        RustixErrno::OVERFLOW => ErrorCode::Overflow,
        RustixErrno::ILSEQ => ErrorCode::IllegalByteSequence,
        RustixErrno::NOTSUP => ErrorCode::Unsupported,
        RustixErrno::ALREADY => ErrorCode::Already,
        RustixErrno::INPROGRESS => ErrorCode::InProgress,
        RustixErrno::INTR => ErrorCode::Interrupted,

        // On some platforms, these have the same value as other errno values.
        #[allow(unreachable_patterns, reason = "see comment")]
        RustixErrno::OPNOTSUPP => ErrorCode::Unsupported,

        _ => return None,
    })
}

#[cfg(windows)]
fn from_raw_os_error(raw_os_error: Option<i32>) -> Option<ErrorCode> {
    use windows_sys::Win32::Foundation;
    Some(match raw_os_error.map(|code| code as u32) {
        Some(Foundation::ERROR_FILE_NOT_FOUND) => ErrorCode::NoEntry,
        Some(Foundation::ERROR_PATH_NOT_FOUND) => ErrorCode::NoEntry,
        Some(Foundation::ERROR_ACCESS_DENIED) => ErrorCode::Access,
        Some(Foundation::ERROR_SHARING_VIOLATION) => ErrorCode::Access,
        Some(Foundation::ERROR_PRIVILEGE_NOT_HELD) => ErrorCode::NotPermitted,
        Some(Foundation::ERROR_INVALID_HANDLE) => ErrorCode::BadDescriptor,
        Some(Foundation::ERROR_INVALID_NAME) => ErrorCode::NoEntry,
        Some(Foundation::ERROR_NOT_ENOUGH_MEMORY) => ErrorCode::InsufficientMemory,
        Some(Foundation::ERROR_OUTOFMEMORY) => ErrorCode::InsufficientMemory,
        Some(Foundation::ERROR_DIR_NOT_EMPTY) => ErrorCode::NotEmpty,
        Some(Foundation::ERROR_NOT_READY) => ErrorCode::Busy,
        Some(Foundation::ERROR_BUSY) => ErrorCode::Busy,
        Some(Foundation::ERROR_NOT_SUPPORTED) => ErrorCode::Unsupported,
        Some(Foundation::ERROR_FILE_EXISTS) => ErrorCode::Exist,
        Some(Foundation::ERROR_BROKEN_PIPE) => ErrorCode::Pipe,
        Some(Foundation::ERROR_BUFFER_OVERFLOW) => ErrorCode::NameTooLong,
        Some(Foundation::ERROR_NOT_A_REPARSE_POINT) => ErrorCode::Invalid,
        Some(Foundation::ERROR_NEGATIVE_SEEK) => ErrorCode::Invalid,
        Some(Foundation::ERROR_DIRECTORY) => ErrorCode::NotDirectory,
        Some(Foundation::ERROR_ALREADY_EXISTS) => ErrorCode::Exist,
        Some(Foundation::ERROR_STOPPED_ON_SYMLINK) => ErrorCode::Loop,
        Some(Foundation::ERROR_DIRECTORY_NOT_SUPPORTED) => ErrorCode::IsDirectory,
        _ => return None,
    })
}

impl<'a> From<&'a std::io::Error> for ErrorCode {
    fn from(err: &'a std::io::Error) -> ErrorCode {
        if err
            .get_ref()
            .is_some_and(|e| e.is::<FilesystemOperationError>())
        {
            return ErrorCode::WouldBlock;
        }
        match from_raw_os_error(err.raw_os_error()) {
            Some(errno) => errno,
            None => {
                debug!("unknown raw os error: {err}");
                match err.kind() {
                    std::io::ErrorKind::NotFound => ErrorCode::NoEntry,
                    std::io::ErrorKind::PermissionDenied => ErrorCode::NotPermitted,
                    std::io::ErrorKind::AlreadyExists => ErrorCode::Exist,
                    std::io::ErrorKind::InvalidInput => ErrorCode::Invalid,
                    _ => ErrorCode::Io,
                }
            }
        }
    }
}

impl From<std::io::Error> for ErrorCode {
    fn from(err: std::io::Error) -> ErrorCode {
        ErrorCode::from(&err)
    }
}

#[derive(Clone)]
pub enum Descriptor {
    File(File),
    Dir(Dir),
}

impl Descriptor {
    pub(crate) fn start_operation(
        &self,
    ) -> Result<Option<FilesystemOperation>, FilesystemOperationError> {
        match self {
            Self::File(file) => file.start_operation(),
            Self::Dir(dir) => dir.start_operation(),
        }
    }

    pub(crate) fn file(&self) -> Result<&File, ErrorCode> {
        match self {
            Descriptor::File(f) => Ok(f),
            // File-only ops such as advise stay bad-descriptor on a dir
            // (wasi-testsuite filesystem-advise). read-via-stream maps Dir
            // to is-directory on its own.
            Descriptor::Dir(_) => Err(ErrorCode::BadDescriptor),
        }
    }

    pub(crate) fn dir(&self) -> Result<&Dir, ErrorCode> {
        match self {
            Descriptor::Dir(d) => Ok(d),
            Descriptor::File(_) => Err(ErrorCode::NotDirectory),
        }
    }

    pub(crate) async fn sync_data(&self) -> Result<(), ErrorCode> {
        match self {
            Self::File(f) => {
                match f.run_blocking(|f| f.sync_data()).await {
                    Ok(()) => Ok(()),
                    // On windows, `sync_data` uses `FileFlushBuffers` which fails with
                    // `ERROR_ACCESS_DENIED` if the file is not upen for writing. Ignore
                    // this error, for POSIX compatibility.
                    #[cfg(windows)]
                    Err(err)
                        if err.raw_os_error()
                            == Some(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED as _) =>
                    {
                        Ok(())
                    }
                    Err(err) => Err(err.into()),
                }
            }
            Self::Dir(d) => {
                d.run_blocking(|d| {
                    let d = cap_primitives::fs::open(
                        d,
                        std::path::Component::CurDir.as_ref(),
                        OpenOptions::new().read(true),
                    )?;
                    d.sync_data()?;
                    Ok(())
                })
                .await
            }
        }
    }

    pub(crate) async fn get_flags(&self) -> Result<DescriptorFlags, ErrorCode> {
        self.get_flags_with_operation(self.start_operation()?).await
    }

    pub(crate) async fn get_flags_with_operation(
        &self,
        operation: Option<FilesystemOperation>,
    ) -> Result<DescriptorFlags, ErrorCode> {
        match self {
            Self::File(f) => {
                let mut flags = f
                    .run_blocking_with(operation, |f| sys::get_flags(f))
                    .await?;
                if f.open_mode.contains(OpenMode::READ) {
                    flags |= DescriptorFlags::READ;
                }
                if f.open_mode.contains(OpenMode::WRITE) {
                    flags |= DescriptorFlags::WRITE;
                }
                Ok(flags)
            }
            Self::Dir(d) => {
                let mut flags = d
                    .run_blocking_with(operation, |d| sys::get_flags(d))
                    .await?;
                if d.open_mode.contains(OpenMode::READ) {
                    flags |= DescriptorFlags::READ;
                }
                if d.open_mode.contains(OpenMode::WRITE) {
                    flags |= DescriptorFlags::MUTATE_DIRECTORY;
                }
                Ok(flags)
            }
        }
    }

    pub(crate) async fn get_type(&self) -> Result<DescriptorType, ErrorCode> {
        self.get_type_with_operation(self.start_operation()?).await
    }

    pub(crate) async fn get_type_with_operation(
        &self,
        operation: Option<FilesystemOperation>,
    ) -> Result<DescriptorType, ErrorCode> {
        match self {
            Self::File(f) => {
                let meta = f
                    .run_blocking_with(operation, |f| Metadata::from_file(f))
                    .await?;
                Ok(meta.file_type().into())
            }
            Self::Dir(_) => Ok(DescriptorType::Directory),
        }
    }

    pub(crate) async fn set_times(
        &self,
        atim: Option<SystemTime>,
        mtim: Option<SystemTime>,
    ) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        let mut times = std::fs::FileTimes::new();
        if let Some(atim) = atim {
            times = times.set_accessed(atim);
        }
        if let Some(mtim) = mtim {
            times = times.set_modified(mtim);
        }
        match self {
            Self::File(f) => {
                if f.perms.write_not_permitted() {
                    return Err(ErrorCode::NotPermitted);
                }
                f.run_blocking_with(operation, move |f| f.set_times(times))
                    .await?;
                Ok(())
            }
            Self::Dir(d) => {
                if d.perms.write_not_permitted() {
                    return Err(ErrorCode::NotPermitted);
                }
                d.run_blocking_with(operation, move |d| d.set_times(times))
                    .await?;
                Ok(())
            }
        }
    }

    pub(crate) async fn sync(&self) -> Result<(), ErrorCode> {
        match self {
            Self::File(f) => {
                match f.run_blocking(|f| f.sync_all()).await {
                    Ok(()) => Ok(()),
                    // On windows, `sync_data` uses `FileFlushBuffers` which fails with
                    // `ERROR_ACCESS_DENIED` if the file is not upen for writing. Ignore
                    // this error, for POSIX compatibility.
                    #[cfg(windows)]
                    Err(err)
                        if err.raw_os_error()
                            == Some(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED as _) =>
                    {
                        Ok(())
                    }
                    Err(err) => Err(err.into()),
                }
            }
            Self::Dir(d) => {
                d.run_blocking(|d| {
                    let d = cap_primitives::fs::open(
                        d,
                        std::path::Component::CurDir.as_ref(),
                        OpenOptions::new().read(true),
                    )?;
                    d.sync_all()?;
                    Ok(())
                })
                .await
            }
        }
    }

    pub(crate) async fn stat(&self) -> Result<DescriptorStat, ErrorCode> {
        self.stat_with_operation(self.start_operation()?).await
    }

    pub(crate) async fn stat_with_operation(
        &self,
        operation: Option<FilesystemOperation>,
    ) -> Result<DescriptorStat, ErrorCode> {
        match self {
            Self::File(f) => Ok(f.run_blocking_with(operation, |f| sys::stat(f)).await?),
            Self::Dir(d) => Ok(d.run_blocking_with(operation, |f| sys::stat(f)).await?),
        }
    }

    pub(crate) async fn is_same_object(&self, other: &Self) -> wasmtime::Result<bool> {
        // No permissions check on metadata: if opened, allowed to stat it
        let operation = match self {
            Self::File(f) => f.start_operation()?,
            Self::Dir(d) => d.start_operation()?,
        };
        // Retain B's factory without invoking it. Capturing only B's native Arc
        // loses its external generation when the originating descriptor dies.
        let other = other.clone();
        Ok(match self {
            Self::File(f) => {
                f.run_blocking_with(operation, move |f| {
                    #[cfg(test)]
                    native_owner_tests::native_work_gate(f);
                    let native = match &other {
                        Self::File(file) => &file.file,
                        Self::Dir(dir) => &dir.dir,
                    };
                    sys::is_same_file(f, native)
                })
                .await?
            }
            Self::Dir(d) => {
                d.run_blocking_with(operation, move |d| {
                    #[cfg(test)]
                    native_owner_tests::native_work_gate(d);
                    let native = match &other {
                        Self::File(file) => &file.file,
                        Self::Dir(dir) => &dir.dir,
                    };
                    sys::is_same_file(d, native)
                })
                .await?
            }
        })
    }

    pub(crate) async fn metadata_hash(&self) -> Result<MetadataHashValue, ErrorCode> {
        self.metadata_hash_with_operation(self.start_operation()?)
            .await
    }

    pub(crate) async fn metadata_hash_with_operation(
        &self,
        operation: Option<FilesystemOperation>,
    ) -> Result<MetadataHashValue, ErrorCode> {
        match self {
            Self::File(f) => Ok(f
                .run_blocking_with(operation, |f| sys::metadata_hash(f))
                .await?),
            Self::Dir(d) => Ok(d
                .run_blocking_with(operation, |d| sys::metadata_hash(d))
                .await?),
        }
    }
}

#[derive(Clone)]
pub struct File {
    /// The operating system File this struct is mediating access to.
    ///
    /// Wrapped in an Arc because the same underlying file is used for
    /// implementing the stream types. A copy is also needed for
    /// `spawn_blocking`.
    /// Cloning this Arc does not retain the descriptor's operation hook or its
    /// captured owners. See [`WasiFilesystemCtx::set_operation_hook`].
    // Drop our native reference before the factory and its captured owners.
    pub file: Arc<std::fs::File>,
    /// Permissions to enforce on access to the file. These permissions are
    /// specified to the parent preopen by a user of the
    /// `crate::WasiCtxBuilder`, and are enforced prior to any enforced by the
    /// underlying operating system.
    pub perms: FsPerms,
    /// The mode the file was opened under: bits for reading, and writing.
    /// Required to correctly report the DescriptorFlags, because
    /// cap-primitives doesn't presently provide a cross-platform equivalent
    /// of reading the oflags back out using fcntl.
    pub open_mode: OpenMode,

    allow_blocking_current_thread: bool,
    pub(crate) operation_hook: Option<FilesystemOperationHook>,
}

impl File {
    /// Creates a file descriptor. Use [`WasiFilesystemCtx::push_descriptor`] to
    /// install it with a context's operation hook.
    pub fn new(
        file: std::fs::File,
        perms: FsPerms,
        open_mode: OpenMode,
        allow_blocking_current_thread: bool,
    ) -> Self {
        Self {
            file: Arc::new(file),
            perms,
            open_mode,
            allow_blocking_current_thread,
            operation_hook: None,
        }
    }

    pub(crate) fn start_operation(
        &self,
    ) -> Result<Option<FilesystemOperation>, FilesystemOperationError> {
        self.operation_hook.as_ref().map(|hook| hook()).transpose()
    }

    /// Execute the blocking `body` function.
    ///
    /// Depending on how the WasiCtx was configured, the body may either be:
    /// - Executed directly on the current thread. In this case the `async`
    ///   signature of this method is effectively a lie and the returned
    ///   Future will always be immediately Ready. Or:
    /// - Spawned on a background thread using [`tokio::task::spawn_blocking`]
    ///   and immediately awaited.
    ///
    /// Intentionally blocking the executor thread might seem unorthodox, but is
    /// not actually a problem for specific workloads. See:
    /// - [`crate::WasiCtxBuilder::allow_blocking_current_thread`]
    /// - [Poor performance of wasmtime file I/O maybe because tokio](https://github.com/bytecodealliance/wasmtime/issues/7973)
    /// - [Implement opt-in for enabling WASI to block the current thread](https://github.com/bytecodealliance/wasmtime/pull/8190)
    pub(crate) async fn run_blocking<F, R, E>(&self, body: F) -> Result<R, E>
    where
        F: FnOnce(&std::fs::File) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<FilesystemOperationError> + Send + 'static,
    {
        self.run_blocking_with(self.start_operation()?, body).await
    }

    pub(crate) async fn run_blocking_with<F, R>(
        &self,
        operation: Option<FilesystemOperation>,
        body: F,
    ) -> R
    where
        F: FnOnce(&std::fs::File) -> R + Send + 'static,
        R: Send + 'static,
    {
        match self.as_blocking_file() {
            Some(file) => NativeOperation::new(file, operation).run(|file| body(file)),
            None => self.spawn_blocking_with(operation, body).await,
        }
    }

    pub(crate) fn spawn_blocking<F, R>(
        &self,
        body: F,
    ) -> Result<AbortOnDropJoinHandle<R>, FilesystemOperationError>
    where
        F: FnOnce(&std::fs::File) -> R + Send + 'static,
        R: Send + 'static,
    {
        Ok(self.spawn_blocking_with(self.start_operation()?, body))
    }

    fn spawn_blocking_with<F, R>(
        &self,
        operation: Option<FilesystemOperation>,
        body: F,
    ) -> AbortOnDropJoinHandle<R>
    where
        F: FnOnce(&std::fs::File) -> R + Send + 'static,
        R: Send + 'static,
    {
        let work = NativeOperation::new((self.file.clone(), Some(body)), operation);
        spawn_blocking(move || work.run(|(file, body)| body.take().unwrap()(file)))
    }

    /// Returns `Some` when the current thread is allowed to block in filesystem
    /// operations, and otherwise returns `None` to indicate that
    /// `spawn_blocking` must be used.
    pub(crate) fn as_blocking_file(&self) -> Option<&std::fs::File> {
        if self.allow_blocking_current_thread {
            Some(&self.file)
        } else {
            None
        }
    }

    /// Returns a reference to the shared native handle.
    #[cfg(feature = "p3")]
    pub(crate) fn as_file(&self) -> &Arc<std::fs::File> {
        &self.file
    }

    pub(crate) async fn advise(
        &self,
        offset: u64,
        len: u64,
        advice: Advice,
    ) -> Result<(), ErrorCode> {
        self.run_blocking(move |f| sys::advise(f, offset, len, advice))
            .await?;
        Ok(())
    }

    pub(crate) async fn set_size(&self, size: u64) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        self.run_blocking_with(operation, move |f| f.set_len(size))
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Dir {
    /// The operating system file descriptor this struct is mediating access
    /// to.
    ///
    /// This is a handle to a directory, and all paths accessed through this
    /// struct are sandboxed to be within this directory via `cap-primitives`.
    ///
    /// Wrapped in an Arc because a copy is needed for `run_blocking`.
    /// Cloning this Arc does not retain the descriptor's operation hook or its
    /// captured owners. See [`WasiFilesystemCtx::set_operation_hook`].
    // Drop our native reference before the factory and its captured owners.
    pub dir: Arc<std::fs::File>,
    /// Permissions to enforce on access to the filesystem under this
    /// directory are specified by a user of the `crate::WasiCtxBuilder`, and
    /// are enforced prior to any enforced by the underlying operating system.
    ///
    /// These permissions are also enforced on any directories opened under
    /// this directory.
    pub perms: FsPerms,
    /// The mode the directory was opened under: bits for reading, and writing.
    /// Required to correctly report the DescriptorFlags, because
    /// cap-primitives doesn't presently provide a cross-platform equivalent
    /// of reading the oflags back out using fcntl.
    pub open_mode: OpenMode,

    pub(crate) allow_blocking_current_thread: bool,
    pub(crate) operation_hook: Option<FilesystemOperationHook>,
}

impl Dir {
    /// Creates a directory descriptor. Use [`WasiFilesystemCtx::push_descriptor`]
    /// to install it with a context's operation hook.
    pub fn new(
        dir: std::fs::File,
        perms: FsPerms,
        open_mode: OpenMode,
        allow_blocking_current_thread: bool,
    ) -> Self {
        Dir {
            dir: Arc::new(dir),
            perms,
            open_mode,
            allow_blocking_current_thread,
            operation_hook: None,
        }
    }

    pub(crate) fn start_operation(
        &self,
    ) -> Result<Option<FilesystemOperation>, FilesystemOperationError> {
        self.operation_hook.as_ref().map(|hook| hook()).transpose()
    }

    /// Execute the blocking `body` function.
    ///
    /// Depending on how the WasiCtx was configured, the body may either be:
    /// - Executed directly on the current thread. In this case the `async`
    ///   signature of this method is effectively a lie and the returned
    ///   Future will always be immediately Ready. Or:
    /// - Spawned on a background thread using [`tokio::task::spawn_blocking`]
    ///   and immediately awaited.
    ///
    /// Intentionally blocking the executor thread might seem unorthodox, but is
    /// not actually a problem for specific workloads. See:
    /// - [`crate::WasiCtxBuilder::allow_blocking_current_thread`]
    /// - [Poor performance of wasmtime file I/O maybe because tokio](https://github.com/bytecodealliance/wasmtime/issues/7973)
    /// - [Implement opt-in for enabling WASI to block the current thread](https://github.com/bytecodealliance/wasmtime/pull/8190)
    pub(crate) async fn run_blocking<F, R, E>(&self, body: F) -> Result<R, E>
    where
        F: FnOnce(&std::fs::File) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<FilesystemOperationError> + Send + 'static,
    {
        self.run_blocking_with(self.start_operation()?, body).await
    }

    pub(crate) async fn run_blocking_with<F, R>(
        &self,
        operation: Option<FilesystemOperation>,
        body: F,
    ) -> R
    where
        F: FnOnce(&std::fs::File) -> R + Send + 'static,
        R: Send + 'static,
    {
        if self.allow_blocking_current_thread {
            NativeOperation::new(&self.dir, operation).run(|dir| body(dir))
        } else {
            let work = NativeOperation::new((self.dir.clone(), Some(body)), operation);
            spawn_blocking(move || work.run(|(dir, body)| body.take().unwrap()(dir))).await
        }
    }

    /// Returns reference to the underlying directory handle.
    pub(crate) fn as_dir(&self) -> &Arc<std::fs::File> {
        &self.dir
    }

    pub(crate) async fn create_directory_at(&self, path: String) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        self.run_blocking_with(operation, move |d| {
            cap_primitives::fs::create_dir(d, path.as_ref(), &DirOptions::new())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn stat_at(
        &self,
        path_flags: PathFlags,
        path: String,
    ) -> Result<DescriptorStat, ErrorCode> {
        self.stat_at_with_operation(self.start_operation()?, path_flags, path)
            .await
    }

    pub(crate) async fn stat_at_with_operation(
        &self,
        operation: Option<FilesystemOperation>,
        path_flags: PathFlags,
        path: String,
    ) -> Result<DescriptorStat, ErrorCode> {
        let follow = if path_flags.contains(PathFlags::SYMLINK_FOLLOW) {
            FollowSymlinks::Yes
        } else {
            FollowSymlinks::No
        };
        let ret = self
            .run_blocking_with(operation, move |d| sys::stat_at(d, path.as_ref(), follow))
            .await?;
        Ok(ret)
    }

    pub(crate) async fn set_times_at(
        &self,
        path_flags: PathFlags,
        path: String,
        atim: Option<SystemTime>,
        mtim: Option<SystemTime>,
    ) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        let atim =
            atim.map(|t| SystemTimeSpec::Absolute(cap_primitives::time::SystemTime::from_std(t)));
        let mtim =
            mtim.map(|t| SystemTimeSpec::Absolute(cap_primitives::time::SystemTime::from_std(t)));
        if path_flags.contains(PathFlags::SYMLINK_FOLLOW) {
            self.run_blocking_with(operation, move |d| {
                cap_primitives::fs::set_times(d, path.as_ref(), atim, mtim)
            })
            .await?;
        } else {
            self.run_blocking_with(operation, move |d| {
                cap_primitives::fs::set_times_nofollow(d, path.as_ref(), atim, mtim)
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn link_at(
        &self,
        old_path_flags: PathFlags,
        old_path: String,
        new_dir: &Self,
        new_path: String,
    ) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        if new_dir.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        if old_path_flags.contains(PathFlags::SYMLINK_FOLLOW) {
            return Err(ErrorCode::Invalid);
        }
        if self.perms != new_dir.perms {
            return Err(ErrorCode::NotPermitted);
        }
        let new_dir = new_dir.clone();
        self.run_blocking_with(operation, move |d| {
            #[cfg(test)]
            native_owner_tests::native_work_gate(d);
            // Borrow the complete descriptor so closure capture retains its hook.
            cap_primitives::fs::hard_link(d, old_path.as_ref(), new_dir.as_dir(), new_path.as_ref())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn open_at(
        &self,
        path_flags: PathFlags,
        path: String,
        oflags: OpenFlags,
        flags: DescriptorFlags,
        allow_blocking_current_thread: bool,
    ) -> Result<NativeOperation<Descriptor>, ErrorCode> {
        let operation = self.start_operation()?;
        // Track whether we are creating file, for permission check:
        let mut create = false;
        // Track open mode, for permission check and recording in created descriptor:
        let mut open_mode = OpenMode::empty();
        // Construct the OpenOptions to give the OS:
        let mut opts = OpenOptions::new();
        sys::maybe_dir(&mut opts);

        if oflags.contains(OpenFlags::CREATE) {
            if oflags.contains(OpenFlags::EXCLUSIVE) {
                opts.create_new(true);
            } else {
                opts.create(true);
            }
            create = true;
            opts.write(true);
            open_mode |= OpenMode::WRITE;
        }

        if oflags.contains(OpenFlags::TRUNCATE) {
            opts.truncate(true).write(true);
            open_mode |= OpenMode::WRITE;
        }
        if flags.contains(DescriptorFlags::READ) {
            opts.read(true);
            open_mode |= OpenMode::READ;
        }
        if flags.contains(DescriptorFlags::WRITE) {
            opts.write(true);
            open_mode |= OpenMode::WRITE;
        } else {
            // If not opened write, open read. This way the OS lets us open
            // the file, but we can use perms to reject use of the file later.
            opts.read(true);
            open_mode |= OpenMode::READ;
        }

        use cap_fs_ext_avoid_using_this::OpenOptionsFollowExt;
        if path_flags.contains(PathFlags::SYMLINK_FOLLOW) {
            opts.follow(FollowSymlinks::Yes);
        } else {
            opts.follow(FollowSymlinks::No);
        }

        // These flags are not yet supported in cap-primitives:
        if flags.contains(DescriptorFlags::FILE_INTEGRITY_SYNC)
            || flags.contains(DescriptorFlags::DATA_INTEGRITY_SYNC)
            || flags.contains(DescriptorFlags::REQUESTED_WRITE_SYNC)
        {
            return Err(ErrorCode::Unsupported);
        }

        if oflags.contains(OpenFlags::DIRECTORY) {
            if oflags.contains(OpenFlags::CREATE)
                || oflags.contains(OpenFlags::EXCLUSIVE)
                || oflags.contains(OpenFlags::TRUNCATE)
            {
                return Err(ErrorCode::Invalid);
            }
        }

        // Now enforce this WasiCtx's permissions before letting the OS have
        // its shot:
        if self.perms.write_not_permitted() {
            if create || open_mode.contains(OpenMode::WRITE) {
                return Err(ErrorCode::NotPermitted);
            }
        }

        // Represents each possible outcome from the spawn_blocking operation.
        // This makes sure we don't have to give spawn_blocking any way to
        // manipulate the table.
        enum OpenResult {
            Dir(std::fs::File),
            File(std::fs::File),
            NotDir,
        }

        let result_operation = operation.clone();
        let opened = self
            .run_blocking_with(operation, move |d| {
                let result = (|| -> std::io::Result<OpenResult> {
                    let opened = cap_primitives::fs::open(d, path.as_ref(), &opts)?;
                    if Metadata::from_file(&opened)?.is_dir() {
                        Ok(OpenResult::Dir(opened))
                    } else if oflags.contains(OpenFlags::DIRECTORY) {
                        Ok(OpenResult::NotDir)
                    } else {
                        Ok(OpenResult::File(opened))
                    }
                })();
                let operation = match &result {
                    Ok(OpenResult::Dir(_) | OpenResult::File(_)) => result_operation,
                    _ => None,
                };
                NativeOperation::new(result, operation)
            })
            .await;
        let NativeOperation {
            value: opened,
            operation,
        } = opened;

        let descriptor = match opened? {
            // Paper over a divergence between Windows and POSIX, where
            // POSIX returns EISDIR if you open a directory with the
            // WRITE flag: https://pubs.opengroup.org/onlinepubs/9699919799/functions/open.html#:~:text=EISDIR
            #[cfg(windows)]
            OpenResult::Dir(_) if flags.contains(DescriptorFlags::WRITE) => {
                Err(ErrorCode::IsDirectory)
            }

            OpenResult::Dir(dir) => {
                let mut dir = Dir::new(dir, self.perms, open_mode, allow_blocking_current_thread);
                dir.operation_hook = self.operation_hook.clone();
                Ok(Descriptor::Dir(dir))
            }

            OpenResult::File(file) => {
                let mut file =
                    File::new(file, self.perms, open_mode, allow_blocking_current_thread);
                file.operation_hook = self.operation_hook.clone();
                Ok(Descriptor::File(file))
            }

            OpenResult::NotDir => Err(ErrorCode::NotDirectory),
        }?;
        Ok(NativeOperation::new(descriptor, operation))
    }

    pub(crate) async fn readlink_at(&self, path: String) -> Result<String, ErrorCode> {
        let link = self
            .run_blocking(move |d| cap_primitives::fs::read_link(d, path.as_ref()))
            .await?;
        link.into_os_string()
            .into_string()
            .or(Err(ErrorCode::IllegalByteSequence))
    }

    pub(crate) async fn remove_directory_at(&self, path: String) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        self.run_blocking_with(operation, move |d| {
            cap_primitives::fs::remove_dir(d, path.as_ref())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn rename_at(
        &self,
        old_path: String,
        new_dir: &Self,
        new_path: String,
    ) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        if new_dir.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        if self.perms != new_dir.perms {
            return Err(ErrorCode::NotPermitted);
        }
        let new_dir = new_dir.clone();
        self.run_blocking_with(operation, move |d| {
            #[cfg(test)]
            native_owner_tests::native_work_gate(d);
            // Borrow the complete descriptor so closure capture retains its hook.
            cap_primitives::fs::rename(d, old_path.as_ref(), new_dir.as_dir(), new_path.as_ref())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn symlink_at(
        &self,
        src_path: String,
        dest_path: String,
    ) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        self.run_blocking_with(operation, move |d| {
            sys::symlink(src_path.as_ref(), d, dest_path.as_ref())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn unlink_file_at(&self, path: String) -> Result<(), ErrorCode> {
        let operation = self.start_operation()?;
        if self.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted);
        }
        self.run_blocking_with(operation, move |d| {
            sys::remove_file_or_symlink(d, path.as_ref())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn metadata_hash_at(
        &self,
        path_flags: PathFlags,
        path: String,
    ) -> Result<MetadataHashValue, ErrorCode> {
        self.metadata_hash_at_with_operation(self.start_operation()?, path_flags, path)
            .await
    }

    pub(crate) async fn metadata_hash_at_with_operation(
        &self,
        operation: Option<FilesystemOperation>,
        path_flags: PathFlags,
        path: String,
    ) -> Result<MetadataHashValue, ErrorCode> {
        // No permissions check on metadata: if dir opened, allowed to stat it
        let follow = if path_flags.contains(PathFlags::SYMLINK_FOLLOW) {
            FollowSymlinks::Yes
        } else {
            FollowSymlinks::No
        };
        let hash = self
            .run_blocking_with(operation, move |d| {
                sys::metadata_hash_at(d, path.as_ref(), follow)
            })
            .await?;
        Ok(hash)
    }
}

impl WasiFilesystemCtxView<'_> {
    pub(crate) fn get_directories(
        &mut self,
    ) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        let preopens = self.ctx.preopens.clone();
        let mut results = Vec::with_capacity(preopens.len());
        for (dir, name) in preopens {
            let fd = self
                .ctx
                .push_descriptor(self.table, Descriptor::Dir(dir))
                .with_context(|| format!("failed to push preopen {name}"))?;
            results.push((fd, name));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod lifecycle_tests;

#[cfg(test)]
mod native_owner_tests;

#[cfg(test)]
mod operation_lifecycle_tests {
    use super::{
        Descriptor, Dir, File, FilesystemOperation, FilesystemOperationError, FsPerms,
        NativeOperation, OpenMode, WasiFilesystemCtx,
    };
    use std::cell::Cell;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_secs(30);

    struct NotifyOnDrop(mpsc::Sender<&'static str>, &'static str);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(self.1);
        }
    }

    struct SetOnDrop(Arc<AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct SendOnlyGuard {
        _not_sync: Cell<()>,
        drops: Arc<AtomicUsize>,
    }

    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("test result destructor panic");
        }
    }

    impl Drop for SendOnlyGuard {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn file(allow_blocking_current_thread: bool) -> File {
        let file = tempfile::tempfile().unwrap();
        File::new(
            file,
            FsPerms::ReadWrite,
            OpenMode::READ | OpenMode::WRITE,
            allow_blocking_current_thread,
        )
    }

    fn dir(allow_blocking_current_thread: bool) -> (tempfile::TempDir, Dir) {
        let tempdir = tempfile::tempdir().unwrap();
        let dir = cap_primitives::fs::open_ambient_dir(
            tempdir.path(),
            cap_primitives::ambient_authority(),
        )
        .unwrap();
        (
            tempdir,
            Dir::new(
                dir,
                FsPerms::ReadWrite,
                OpenMode::READ | OpenMode::WRITE,
                allow_blocking_current_thread,
            ),
        )
    }

    #[tokio::test]
    async fn admission_rejection_precedes_internal_validation() {
        use super::{DescriptorFlags, ErrorCode, OpenFlags, PathFlags};

        for blocking in [false, true] {
            let admissions = Arc::new(AtomicUsize::new(0));
            let count = admissions.clone();
            let hook: super::FilesystemOperationHook = Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Err(FilesystemOperationError::Rejected)
            });
            let mut file = file(blocking);
            file.perms = FsPerms::ReadOnly;
            file.operation_hook = Some(hook.clone());
            let (_temp, mut dir) = dir(blocking);
            dir.perms = FsPerms::ReadOnly;
            dir.operation_hook = Some(hook);
            let file_descriptor = Descriptor::File(file.clone());
            let dir_descriptor = Descriptor::Dir(dir.clone());

            macro_rules! rejected {
                ($operation:expr) => {{
                    let before = admissions.load(Ordering::SeqCst);
                    assert!(matches!($operation.await, Err(ErrorCode::WouldBlock)));
                    assert_eq!(admissions.load(Ordering::SeqCst), before + 1);
                }};
            }

            rejected!(file.set_size(0));
            rejected!(file_descriptor.set_times(None, None));
            rejected!(dir_descriptor.set_times(None, None));
            rejected!(dir.create_directory_at("new".into()));
            rejected!(dir.set_times_at(PathFlags::empty(), "missing".into(), None, None));
            rejected!(dir.link_at(
                PathFlags::SYMLINK_FOLLOW,
                "missing".into(),
                &dir,
                "new".into()
            ));
            rejected!(dir.open_at(
                PathFlags::empty(),
                "missing".into(),
                OpenFlags::CREATE | OpenFlags::DIRECTORY,
                DescriptorFlags::FILE_INTEGRITY_SYNC,
                blocking,
            ));
            rejected!(dir.remove_directory_at("missing".into()));
            rejected!(dir.rename_at("missing".into(), &dir, "new".into()));
            rejected!(dir.symlink_at("missing".into(), "new".into()));
            rejected!(dir.unlink_file_at("missing".into()));
            rejected!(dir_descriptor.get_type());
            rejected!(dir_descriptor.sync());
            rejected!(dir_descriptor.sync_data());
            rejected!(dir.stat_at(PathFlags::empty(), "missing".into()));
            rejected!(dir.metadata_hash_at(PathFlags::empty(), "missing".into()));
        }
    }

    #[test]
    fn send_only_operation_value_drops_once_after_last_clone() {
        let drops = Arc::new(AtomicUsize::new(0));
        let operation = FilesystemOperation::new(SendOnlyGuard {
            _not_sync: Cell::new(()),
            drops: drops.clone(),
        });
        let clone = operation.clone();

        drop(operation);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(clone);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn default_context_does_not_create_an_operation() {
        let ctx = WasiFilesystemCtx::default();
        let mut table = wasmtime::component::ResourceTable::new();
        let fd = ctx
            .push_descriptor(&mut table, Descriptor::File(file(false)))
            .unwrap();
        assert!(matches!(
            table.get(&fd).unwrap().start_operation(),
            Ok(None)
        ));
    }

    #[test]
    fn context_preserves_typed_admission_rejection() {
        let mut ctx = WasiFilesystemCtx::default();
        ctx.set_operation_hook(|| Err(FilesystemOperationError::Rejected));

        let mut table = wasmtime::component::ResourceTable::new();
        let fd = ctx
            .push_descriptor(&mut table, Descriptor::File(file(false)))
            .unwrap();
        assert!(matches!(
            table.get(&fd).unwrap().start_operation(),
            Err(FilesystemOperationError::Rejected)
        ));
    }

    #[test]
    fn panicking_result_destructor_still_releases_operation() {
        let drops = Arc::new(AtomicUsize::new(0));
        let operation = FilesystemOperation::new(SendOnlyGuard {
            _not_sync: Cell::new(()),
            drops: drops.clone(),
        });
        let result = NativeOperation::new(PanicOnDrop, Some(operation));

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result)));

        assert!(panic.is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn direct_operation_lives_until_blocking_body_returns() {
        let file = file(true);
        let dropped = Arc::new(AtomicBool::new(false));
        let operation = FilesystemOperation::new(SetOnDrop(dropped.clone()));
        let body_dropped = dropped.clone();

        let result = file
            .run_blocking_with(Some(operation), move |_| {
                assert!(!body_dropped.load(Ordering::SeqCst));
                42
            })
            .await;

        assert_eq!(result, 42);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_stream_task_keeps_operation_alive_until_native_work_exits() {
        struct CheckFileClosedOnDrop {
            file: std::sync::Weak<std::fs::File>,
            dropped: mpsc::Sender<bool>,
        }

        impl Drop for CheckFileClosedOnDrop {
            fn drop(&mut self) {
                let _ = self.dropped.send(self.file.upgrade().is_none());
            }
        }

        let file = file(false);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let operation = FilesystemOperation::new(CheckFileClosedOnDrop {
            file: Arc::downgrade(&file.file),
            dropped: dropped_tx,
        });
        let task = file.spawn_blocking_with(Some(operation), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv_timeout(TIMEOUT).unwrap();
        drop(task);
        drop(file);

        assert!(dropped_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        assert!(dropped_rx.recv_timeout(TIMEOUT).unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_join_handle_keeps_operation_alive_until_native_work_exits() {
        let file = file(false);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let operation = FilesystemOperation::new(NotifyOnDrop(dropped_tx, "operation"));

        let file_for_task = file.clone();
        let task = tokio::spawn(async move {
            file_for_task
                .run_blocking_with(Some(operation), move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await;
        });
        started_rx.recv_timeout(TIMEOUT).unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(file);

        assert!(dropped_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        assert_eq!(dropped_rx.recv_timeout(TIMEOUT).unwrap(), "operation");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_directory_future_keeps_operation_alive_until_native_work_exits() {
        let (_tempdir, dir) = dir(false);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let operation = FilesystemOperation::new(NotifyOnDrop(dropped_tx, "operation"));

        let dir_for_task = dir.clone();
        let task = tokio::spawn(async move {
            dir_for_task
                .run_blocking_with(Some(operation), move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await;
        });
        started_rx.recv_timeout(TIMEOUT).unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(dir);

        assert!(dropped_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        assert_eq!(dropped_rx.recv_timeout(TIMEOUT).unwrap(), "operation");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_result_is_disposed_before_operation_is_released() {
        let file = file(false);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (events_tx, events_rx) = mpsc::channel();
        let operation = FilesystemOperation::new(NotifyOnDrop(events_tx.clone(), "operation"));

        let result_operation = operation.clone();
        let file_for_task = file.clone();
        let task = tokio::spawn(async move {
            file_for_task
                .run_blocking_with(Some(operation), move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    NativeOperation::new(NotifyOnDrop(events_tx, "result"), Some(result_operation))
                })
                .await;
        });
        started_rx.recv_timeout(TIMEOUT).unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(file);
        release_tx.send(()).unwrap();

        assert_eq!(events_rx.recv_timeout(TIMEOUT).unwrap(), "result");
        assert_eq!(events_rx.recv_timeout(TIMEOUT).unwrap(), "operation");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panic_releases_operation_only_after_native_work_unwinds() {
        let file = file(false);
        let (events_tx, events_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let operation = FilesystemOperation::new(NotifyOnDrop(events_tx.clone(), "operation"));

        let file_for_task = file.clone();
        let task = tokio::spawn(async move {
            file_for_task
                .run_blocking_with(Some(operation), move |_| {
                    let _body_local = NotifyOnDrop(events_tx.clone(), "body-local");
                    events_tx.send("started").unwrap();
                    release_rx.recv().unwrap();
                    panic!("test panic from native work");
                })
                .await;
        });
        assert_eq!(events_rx.recv_timeout(TIMEOUT).unwrap(), "started");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(file);

        assert!(events_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        assert_eq!(events_rx.recv_timeout(TIMEOUT).unwrap(), "body-local");
        assert_eq!(events_rx.recv_timeout(TIMEOUT).unwrap(), "operation");
    }
}

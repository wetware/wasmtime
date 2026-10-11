use crate::filesystem::sys;
use crate::filesystem::{Descriptor, WasiFilesystemCtxView};
use crate::p2::bindings::clocks::wall_clock;
use crate::p2::bindings::filesystem::preopens;
use crate::p2::bindings::filesystem::types::{
    self, ErrorCode, HostDescriptor, HostDirectoryEntryStream,
};
use crate::p2::filesystem::{FileInputStream, FileOutputStream, ReaddirIterator};
use crate::p2::{FsError, FsResult};
use std::time::SystemTime;
use wasmtime::component::Resource;
use wasmtime_wasi_io::streams::{DynInputStream, DynOutputStream};

mod sync;

impl preopens::Host for WasiFilesystemCtxView<'_> {
    fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        self.get_directories()
    }
}

impl types::Host for WasiFilesystemCtxView<'_> {
    fn convert_error_code(&mut self, err: FsError) -> wasmtime::Result<ErrorCode> {
        err.downcast()
    }

    fn filesystem_error_code(
        &mut self,
        err: Resource<wasmtime::Error>,
    ) -> wasmtime::Result<Option<ErrorCode>> {
        let err = self.table.get(&err)?;

        // Stream errors retain either the native I/O error or the typed
        // admission rejection that prevented the native operation.
        if let Some(err) = err.downcast_ref::<std::io::Error>() {
            return Ok(Some(ErrorCode::from(err)));
        }
        if err
            .downcast_ref::<crate::filesystem::FilesystemOperationError>()
            .is_some()
        {
            return Ok(Some(ErrorCode::WouldBlock));
        }

        Ok(None)
    }
}

impl WasiFilesystemCtxView<'_> {
    // P1 shares one admission across enumeration and its follow-up metadata calls.
    pub(crate) async fn read_directory_with_operation(
        &mut self,
        fd: Resource<types::Descriptor>,
        operation: Option<crate::filesystem::FilesystemOperation>,
    ) -> FsResult<Resource<types::DirectoryEntryStream>> {
        let d = self.table.get(&fd)?.dir()?;
        enum ReaddirError {
            Io(std::io::Error),
            IllegalSequence,
        }
        impl From<std::io::Error> for ReaddirError {
            fn from(e: std::io::Error) -> ReaddirError {
                ReaddirError::Io(e)
            }
        }

        let entries = d
            .run_blocking(operation, |d| {
                // Both `entries` and `metadata` perform syscalls, which is why they are done
                // within this `block` call, rather than delay calculating the metadata
                // for entries when they're demanded later in the iterator chain.
                Ok::<_, std::io::Error>(
                    cap_primitives::fs::read_base_dir(d)?
                        .map(|entry| {
                            let entry = entry?;
                            let meta = entry.metadata()?;
                            let type_ = descriptortype_from(meta.file_type());
                            let name = entry
                                .file_name()
                                .into_string()
                                .map_err(|_| ReaddirError::IllegalSequence)?;
                            Ok(types::DirectoryEntry { type_, name })
                        })
                        .collect::<Vec<Result<types::DirectoryEntry, ReaddirError>>>(),
                )
            })
            .await?
            .into_iter();

        // On windows, filter out files like `C:\DumpStack.log.tmp` which we
        // can't get full metadata for.
        #[cfg(windows)]
        let entries = entries.filter(|entry| {
            use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION};
            if let Err(ReaddirError::Io(err)) = entry {
                if err.raw_os_error() == Some(ERROR_SHARING_VIOLATION as i32)
                    || err.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32)
                {
                    return false;
                }
            }
            true
        });
        let entries = entries.map(|r| match r {
            Ok(r) => Ok(r),
            Err(ReaddirError::Io(e)) => Err(e.into()),
            Err(ReaddirError::IllegalSequence) => Err(ErrorCode::IllegalByteSequence.into()),
        });
        Ok(self.table.push(ReaddirIterator::new(entries))?)
    }
}

impl HostDescriptor for WasiFilesystemCtxView<'_> {
    async fn advise(
        &mut self,
        fd: Resource<types::Descriptor>,
        offset: types::Filesize,
        len: types::Filesize,
        advice: types::Advice,
    ) -> FsResult<()> {
        let f = self.table.get(&fd)?.file()?;
        let operation = self.ctx.start_operation()?;
        f.advise(operation, offset, len, advice.into()).await?;
        Ok(())
    }

    async fn sync_data(&mut self, fd: Resource<types::Descriptor>) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?;
        let operation = self.ctx.start_operation()?;
        descriptor.sync_data(operation).await?;
        Ok(())
    }

    async fn get_flags(
        &mut self,
        fd: Resource<types::Descriptor>,
    ) -> FsResult<types::DescriptorFlags> {
        let descriptor = self.table.get(&fd)?;
        let operation = self.ctx.start_operation()?;
        let flags = descriptor.get_flags(operation).await?;
        Ok(flags.into())
    }

    async fn get_type(
        &mut self,
        fd: Resource<types::Descriptor>,
    ) -> FsResult<types::DescriptorType> {
        let descriptor = self.table.get(&fd)?;
        let operation = self.ctx.start_operation()?;
        let ty = descriptor.get_type(operation).await?;
        Ok(ty.into())
    }

    async fn set_size(
        &mut self,
        fd: Resource<types::Descriptor>,
        size: types::Filesize,
    ) -> FsResult<()> {
        let f = self.table.get(&fd)?.file()?;
        let operation = self.ctx.start_operation()?;
        f.set_size(operation, size).await?;
        Ok(())
    }

    async fn set_times(
        &mut self,
        fd: Resource<types::Descriptor>,
        atim: types::NewTimestamp,
        mtim: types::NewTimestamp,
    ) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?;
        let atim = systemtimespec_from(atim)?;
        let mtim = systemtimespec_from(mtim)?;
        let operation = self.ctx.start_operation()?;
        descriptor.set_times(operation, atim, mtim).await?;
        Ok(())
    }

    async fn read(
        &mut self,
        fd: Resource<types::Descriptor>,
        len: types::Filesize,
        offset: types::Filesize,
    ) -> FsResult<(Vec<u8>, bool)> {
        let f = self.table.get(&fd)?.file()?;
        let operation = self.ctx.start_operation()?;

        let (mut buffer, r) = f
            .run_blocking(operation, move |f| {
                let mut buffer = vec![
                    0;
                    len.try_into()
                        .unwrap_or(usize::MAX)
                        .min(crate::MAX_READ_SIZE_ALLOC)
                ];
                let r = sys::read_at_cursor_unspecified(f, &mut buffer, offset);
                (buffer, r)
            })
            .await;

        let (bytes_read, state) = match r? {
            0 => (0, true),
            n => (n, false),
        };

        buffer.truncate(bytes_read);

        Ok((buffer, state))
    }

    async fn write(
        &mut self,
        fd: Resource<types::Descriptor>,
        buf: Vec<u8>,
        offset: types::Filesize,
    ) -> FsResult<types::Filesize> {
        let f = self.table.get(&fd)?.file()?;
        if f.perms.write_not_permitted() {
            return Err(ErrorCode::NotPermitted.into());
        }
        let operation = self.ctx.start_operation()?;

        let bytes_written = f
            .run_blocking(operation, move |f| {
                sys::write_at_cursor_unspecified(f, &buf, offset)
            })
            .await?;

        Ok(types::Filesize::try_from(bytes_written).expect("usize fits in Filesize"))
    }

    async fn read_directory(
        &mut self,
        fd: Resource<types::Descriptor>,
    ) -> FsResult<Resource<types::DirectoryEntryStream>> {
        self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        self.read_directory_with_operation(fd, operation).await
    }

    async fn sync(&mut self, fd: Resource<types::Descriptor>) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?;
        let operation = self.ctx.start_operation()?;
        descriptor.sync(operation).await?;
        Ok(())
    }

    async fn create_directory_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        d.create_directory_at(operation, path).await?;
        Ok(())
    }

    async fn stat(&mut self, fd: Resource<types::Descriptor>) -> FsResult<types::DescriptorStat> {
        let descriptor = self.table.get(&fd)?;
        let operation = self.ctx.start_operation()?;
        let stat = descriptor.stat(operation).await?;
        Ok(stat.try_into()?)
    }

    async fn stat_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path_flags: types::PathFlags,
        path: String,
    ) -> FsResult<types::DescriptorStat> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        let stat = d.stat_at(operation, path_flags.into(), path).await?;
        Ok(stat.try_into()?)
    }

    async fn set_times_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        atim: types::NewTimestamp,
        mtim: types::NewTimestamp,
    ) -> FsResult<()> {
        let d = self.table.get(&fd)?.dir()?;
        let atim = systemtimespec_from(atim)?;
        let mtim = systemtimespec_from(mtim)?;
        let operation = self.ctx.start_operation()?;
        d.set_times_at(operation, path_flags.into(), path, atim, mtim)
            .await?;
        Ok(())
    }

    async fn link_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        // TODO delete the path flags from this function
        old_path_flags: types::PathFlags,
        old_path: String,
        new_descriptor: Resource<types::Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let old_dir = self.table.get(&fd)?.dir()?;
        let new_dir = self.table.get(&new_descriptor)?.dir()?;
        let operation = self.ctx.start_operation()?;
        old_dir
            .link_at(
                operation,
                old_path_flags.into(),
                old_path,
                new_dir,
                new_path,
            )
            .await?;
        Ok(())
    }

    async fn open_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        oflags: types::OpenFlags,
        flags: types::DescriptorFlags,
    ) -> FsResult<Resource<types::Descriptor>> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        let fd = d
            .open_at(
                operation.clone(),
                path_flags.into(),
                path,
                oflags.into(),
                flags.into(),
                self.ctx.allow_blocking_current_thread,
            )
            .await?;
        let fd = self.table.push(fd)?;
        Ok(fd)
    }

    fn drop(&mut self, fd: Resource<types::Descriptor>) -> wasmtime::Result<()> {
        // The Drop will close the file/dir, but if the close syscall
        // blocks the thread, I will face god and walk backwards into hell.
        // tokio::fs::File just uses std::fs::File's Drop impl to close, so
        // it doesn't appear anyone else has found this to be a problem.
        // (Not that they could solve it without async drop...)
        self.table.delete(fd)?;

        Ok(())
    }

    async fn readlink_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path: String,
    ) -> FsResult<String> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        let path = d.readlink_at(operation, path).await?;
        Ok(path)
    }

    async fn remove_directory_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        d.remove_directory_at(operation, path).await?;
        Ok(())
    }

    async fn rename_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        old_path: String,
        new_fd: Resource<types::Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let old_dir = self.table.get(&fd)?.dir()?;
        let new_dir = self.table.get(&new_fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        old_dir
            .rename_at(operation, old_path, new_dir, new_path)
            .await?;
        Ok(())
    }

    async fn symlink_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        src_path: String,
        dest_path: String,
    ) -> FsResult<()> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        d.symlink_at(operation, src_path, dest_path).await?;
        Ok(())
    }

    async fn unlink_file_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        d.unlink_file_at(operation, path).await?;
        Ok(())
    }

    fn read_via_stream(
        &mut self,
        fd: Resource<types::Descriptor>,
        offset: types::Filesize,
    ) -> FsResult<Resource<DynInputStream>> {
        // Trap if fd lookup fails. A directory is is-directory, not
        // bad-descriptor (POSIX EISDIR on read).
        let f = match self.table.get(&fd)? {
            Descriptor::File(f) => f,
            Descriptor::Dir(_) => return Err(ErrorCode::IsDirectory.into()),
        };
        let operation_hook = self.ctx.operation_hook();

        // Create a stream view for it.
        let reader: DynInputStream =
            Box::new(FileInputStream::new_with_hook(f, offset, operation_hook));

        // Insert the stream view into the table. Trap if the table is full.
        let index = self.table.push(reader)?;

        Ok(index)
    }

    fn write_via_stream(
        &mut self,
        fd: Resource<types::Descriptor>,
        offset: types::Filesize,
    ) -> FsResult<Resource<DynOutputStream>> {
        // Trap if fd lookup fails:
        let f = self.table.get(&fd)?.file()?;

        if f.perms.write_not_permitted() {
            Err(types::ErrorCode::NotPermitted)?;
        }
        let operation_hook = self.ctx.operation_hook();

        // Create a stream view for it.
        let writer = FileOutputStream::write_at_with_hook(f, offset, operation_hook);
        let writer: DynOutputStream = Box::new(writer);

        // Insert the stream view into the table. Trap if the table is full.
        let index = self.table.push(writer)?;

        Ok(index)
    }

    fn append_via_stream(
        &mut self,
        fd: Resource<types::Descriptor>,
    ) -> FsResult<Resource<DynOutputStream>> {
        // Trap if fd lookup fails:
        let f = self.table.get(&fd)?.file()?;

        if f.perms.write_not_permitted() {
            Err(types::ErrorCode::NotPermitted)?;
        }
        let operation_hook = self.ctx.operation_hook();

        // Create a stream view for it.
        let appender = FileOutputStream::append_with_hook(f, operation_hook);
        let appender: DynOutputStream = Box::new(appender);

        // Insert the stream view into the table. Trap if the table is full.
        let index = self.table.push(appender)?;

        Ok(index)
    }

    async fn is_same_object(
        &mut self,
        a: Resource<types::Descriptor>,
        b: Resource<types::Descriptor>,
    ) -> wasmtime::Result<bool> {
        let descriptor_a = self.table.get(&a)?;
        let descriptor_b = self.table.get(&b)?;
        let operation = self.ctx.start_operation().map_err(wasmtime::Error::new)?;
        descriptor_a.is_same_object(operation, descriptor_b).await
    }
    async fn metadata_hash(
        &mut self,
        fd: Resource<types::Descriptor>,
    ) -> FsResult<types::MetadataHashValue> {
        let fd = self.table.get(&fd)?;
        let operation = self.ctx.start_operation()?;
        let meta = fd.metadata_hash(operation).await?;
        Ok(meta.into())
    }
    async fn metadata_hash_at(
        &mut self,
        fd: Resource<types::Descriptor>,
        path_flags: types::PathFlags,
        path: String,
    ) -> FsResult<types::MetadataHashValue> {
        let d = self.table.get(&fd)?.dir()?;
        let operation = self.ctx.start_operation()?;
        let meta = d
            .metadata_hash_at(operation, path_flags.into(), path)
            .await?;
        Ok(meta.into())
    }
}

impl HostDirectoryEntryStream for WasiFilesystemCtxView<'_> {
    async fn read_directory_entry(
        &mut self,
        stream: Resource<types::DirectoryEntryStream>,
    ) -> FsResult<Option<types::DirectoryEntry>> {
        let readdir = self.table.get(&stream)?;
        readdir.next()
    }

    fn drop(&mut self, stream: Resource<types::DirectoryEntryStream>) -> wasmtime::Result<()> {
        self.table.delete(stream)?;
        Ok(())
    }
}

impl From<types::Advice> for crate::filesystem::Advice {
    fn from(advice: types::Advice) -> Self {
        match advice {
            types::Advice::Normal => Self::Normal,
            types::Advice::Sequential => Self::Sequential,
            types::Advice::Random => Self::Random,
            types::Advice::WillNeed => Self::WillNeed,
            types::Advice::DontNeed => Self::DontNeed,
            types::Advice::NoReuse => Self::NoReuse,
        }
    }
}

impl From<types::OpenFlags> for crate::filesystem::OpenFlags {
    fn from(flags: types::OpenFlags) -> Self {
        let mut out = Self::empty();
        if flags.contains(types::OpenFlags::CREATE) {
            out |= Self::CREATE;
        }
        if flags.contains(types::OpenFlags::DIRECTORY) {
            out |= Self::DIRECTORY;
        }
        if flags.contains(types::OpenFlags::EXCLUSIVE) {
            out |= Self::EXCLUSIVE;
        }
        if flags.contains(types::OpenFlags::TRUNCATE) {
            out |= Self::TRUNCATE;
        }
        out
    }
}

impl From<types::PathFlags> for crate::filesystem::PathFlags {
    fn from(flags: types::PathFlags) -> Self {
        let mut out = Self::empty();
        if flags.contains(types::PathFlags::SYMLINK_FOLLOW) {
            out |= Self::SYMLINK_FOLLOW;
        }
        out
    }
}

impl From<crate::filesystem::DescriptorFlags> for types::DescriptorFlags {
    fn from(flags: crate::filesystem::DescriptorFlags) -> Self {
        let mut out = Self::empty();
        if flags.contains(crate::filesystem::DescriptorFlags::READ) {
            out |= Self::READ;
        }
        if flags.contains(crate::filesystem::DescriptorFlags::WRITE) {
            out |= Self::WRITE;
        }
        if flags.contains(crate::filesystem::DescriptorFlags::FILE_INTEGRITY_SYNC) {
            out |= Self::FILE_INTEGRITY_SYNC;
        }
        if flags.contains(crate::filesystem::DescriptorFlags::DATA_INTEGRITY_SYNC) {
            out |= Self::DATA_INTEGRITY_SYNC;
        }
        if flags.contains(crate::filesystem::DescriptorFlags::REQUESTED_WRITE_SYNC) {
            out |= Self::REQUESTED_WRITE_SYNC;
        }
        if flags.contains(crate::filesystem::DescriptorFlags::MUTATE_DIRECTORY) {
            out |= Self::MUTATE_DIRECTORY;
        }
        out
    }
}

impl From<types::DescriptorFlags> for crate::filesystem::DescriptorFlags {
    fn from(flags: types::DescriptorFlags) -> Self {
        let mut out = Self::empty();
        if flags.contains(types::DescriptorFlags::READ) {
            out |= Self::READ;
        }
        if flags.contains(types::DescriptorFlags::WRITE) {
            out |= Self::WRITE;
        }
        if flags.contains(types::DescriptorFlags::FILE_INTEGRITY_SYNC) {
            out |= Self::FILE_INTEGRITY_SYNC;
        }
        if flags.contains(types::DescriptorFlags::DATA_INTEGRITY_SYNC) {
            out |= Self::DATA_INTEGRITY_SYNC;
        }
        if flags.contains(types::DescriptorFlags::REQUESTED_WRITE_SYNC) {
            out |= Self::REQUESTED_WRITE_SYNC;
        }
        if flags.contains(types::DescriptorFlags::MUTATE_DIRECTORY) {
            out |= Self::MUTATE_DIRECTORY;
        }
        out
    }
}

impl From<crate::filesystem::MetadataHashValue> for types::MetadataHashValue {
    fn from(
        crate::filesystem::MetadataHashValue { lower, upper }: crate::filesystem::MetadataHashValue,
    ) -> Self {
        Self { lower, upper }
    }
}

impl TryFrom<crate::filesystem::DescriptorStat> for types::DescriptorStat {
    type Error = ErrorCode;

    fn try_from(
        crate::filesystem::DescriptorStat {
            type_,
            link_count,
            size,
            data_access_timestamp,
            data_modification_timestamp,
            status_change_timestamp,
        }: crate::filesystem::DescriptorStat,
    ) -> Result<Self, ErrorCode> {
        // Internal timestamps use i64 seconds; wasi:clocks/wall-clock uses u64
        // (non-negative). Times outside that range become missing rather than
        // failing the whole stat (e.g. host-clamped far-past mtimes on macOS).
        Ok(Self {
            type_: type_.into(),
            link_count,
            size,
            data_access_timestamp: data_access_timestamp.and_then(|t| t.try_into().ok()),
            data_modification_timestamp: data_modification_timestamp
                .and_then(|t| t.try_into().ok()),
            status_change_timestamp: status_change_timestamp.and_then(|t| t.try_into().ok()),
        })
    }
}

impl From<crate::filesystem::DescriptorType> for types::DescriptorType {
    fn from(ty: crate::filesystem::DescriptorType) -> Self {
        match ty {
            crate::filesystem::DescriptorType::Unknown => Self::Unknown,
            crate::filesystem::DescriptorType::BlockDevice => Self::BlockDevice,
            crate::filesystem::DescriptorType::CharacterDevice => Self::CharacterDevice,
            crate::filesystem::DescriptorType::Directory => Self::Directory,
            crate::filesystem::DescriptorType::SymbolicLink => Self::SymbolicLink,
            crate::filesystem::DescriptorType::RegularFile => Self::RegularFile,
        }
    }
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

        #[allow(
            unreachable_patterns,
            reason = "on some platforms, these have the same value as other errno values"
        )]
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

impl From<std::io::Error> for ErrorCode {
    fn from(err: std::io::Error) -> ErrorCode {
        ErrorCode::from(&err)
    }
}

impl<'a> From<&'a std::io::Error> for ErrorCode {
    fn from(err: &'a std::io::Error) -> ErrorCode {
        match from_raw_os_error(err.raw_os_error()) {
            Some(errno) => errno,
            None => {
                tracing::debug!("unknown raw os error: {err}");
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

impl From<std::num::TryFromIntError> for ErrorCode {
    fn from(_err: std::num::TryFromIntError) -> ErrorCode {
        ErrorCode::Overflow
    }
}

fn descriptortype_from(ft: cap_primitives::fs::FileType) -> types::DescriptorType {
    crate::filesystem::DescriptorType::from(ft).into()
}

fn systemtime_from(t: wall_clock::Datetime) -> Result<std::time::SystemTime, ErrorCode> {
    std::time::SystemTime::UNIX_EPOCH
        .checked_add(core::time::Duration::new(t.seconds, t.nanoseconds))
        .ok_or(ErrorCode::Overflow)
}

fn systemtimespec_from(t: types::NewTimestamp) -> Result<Option<SystemTime>, ErrorCode> {
    match t {
        types::NewTimestamp::NoChange => Ok(None),
        types::NewTimestamp::Now => Ok(Some(SystemTime::now())),
        types::NewTimestamp::Timestamp(st) => {
            let st = systemtime_from(st)?;
            Ok(Some(st))
        }
    }
}

impl From<crate::clocks::DatetimeError> for ErrorCode {
    fn from(_: crate::clocks::DatetimeError) -> ErrorCode {
        ErrorCode::Overflow
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::filesystem::{
        Dir, File, FilesystemOperation, FilesystemOperationError, FsPerms, OpenMode,
        WasiFilesystemCtx,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use wasmtime::component::ResourceTable;
    use wasmtime_wasi_io::streams::StreamError;

    struct CountDrop(Arc<AtomicUsize>);

    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn file_with_contents(contents: &[u8]) -> (tempfile::TempPath, File) {
        // Named files permit the handle reopen used by append on Windows.
        let (mut file, path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        std::io::Write::write_all(&mut file, contents).unwrap();
        (
            path,
            File::new(
                file,
                FsPerms::ReadWrite,
                OpenMode::READ | OpenMode::WRITE,
                true,
            ),
        )
    }

    fn dir(path: &std::path::Path) -> Dir {
        Dir::new(
            cap_primitives::fs::open_ambient_dir(path, cap_primitives::ambient_authority())
                .unwrap(),
            FsPerms::ReadWrite,
            OpenMode::READ | OpenMode::WRITE,
            true,
        )
    }

    fn borrowed<T: 'static>(resource: &Resource<T>) -> Resource<T> {
        Resource::new_borrow(resource.rep())
    }

    fn counting_hook(
        ctx: &mut WasiFilesystemCtx,
        admissions: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    ) {
        ctx.set_operation_hook(move || {
            admissions.fetch_add(1, Ordering::SeqCst);
            Ok(FilesystemOperation::new(CountDrop(drops.clone())))
        });
    }

    #[test]
    fn table_readdir_works() {
        let mut table = ResourceTable::new();
        let ix = table
            .push(ReaddirIterator::new(std::iter::empty()))
            .unwrap();
        let _ = table.get(&ix).unwrap();
        table.delete(ix).unwrap();
    }

    #[tokio::test]
    async fn stat_rejection_is_would_block() {
        let mut ctx = WasiFilesystemCtx::default();
        ctx.set_operation_hook(|| Err(FilesystemOperationError::Rejected));
        let (_path, file) = file_with_contents(b"contents");
        let mut table = ResourceTable::new();
        let file = table.push(Descriptor::File(file)).unwrap();
        let mut view = WasiFilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        let err = HostDescriptor::stat(&mut view, borrowed(&file))
            .await
            .unwrap_err();
        assert_eq!(err.downcast().unwrap(), ErrorCode::WouldBlock);
    }

    #[tokio::test]
    async fn is_same_object_rejection_is_a_typed_trap() {
        let mut ctx = WasiFilesystemCtx::default();
        ctx.set_operation_hook(|| Err(FilesystemOperationError::Rejected));
        let (_path, file) = file_with_contents(b"contents");
        let mut table = ResourceTable::new();
        let file = table.push(Descriptor::File(file)).unwrap();
        let mut view = WasiFilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        let err = HostDescriptor::is_same_object(&mut view, borrowed(&file), borrowed(&file))
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<FilesystemOperationError>()
                .is_some_and(|err| matches!(err, FilesystemOperationError::Rejected))
        );
    }

    #[tokio::test]
    async fn rename_and_link_each_admit_exactly_once() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("source"), b"contents").unwrap();

        let admissions = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut ctx = WasiFilesystemCtx::default();
        counting_hook(&mut ctx, admissions.clone(), drops.clone());
        let mut table = ResourceTable::new();
        let directory = table.push(Descriptor::Dir(dir(tempdir.path()))).unwrap();
        let mut view = WasiFilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        HostDescriptor::rename_at(
            &mut view,
            borrowed(&directory),
            "source".into(),
            borrowed(&directory),
            "renamed".into(),
        )
        .await
        .unwrap();
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        HostDescriptor::link_at(
            &mut view,
            borrowed(&directory),
            types::PathFlags::empty(),
            "renamed".into(),
            borrowed(&directory),
            "linked".into(),
        )
        .await
        .unwrap();
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(
            std::fs::read(tempdir.path().join("linked")).unwrap(),
            b"contents"
        );
    }

    #[tokio::test]
    async fn input_stream_admits_each_native_read_and_releases_at_eof() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut ctx = WasiFilesystemCtx::default();
        counting_hook(&mut ctx, admissions.clone(), drops.clone());
        let (_path, file) = file_with_contents(b"ab");
        let mut table = ResourceTable::new();
        let file = table.push(Descriptor::File(file)).unwrap();
        let mut view = WasiFilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        let stream = HostDescriptor::read_via_stream(&mut view, borrowed(&file), 0).unwrap();
        assert_eq!(admissions.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        let reader = view.table.get_mut(&stream).unwrap();
        assert_eq!(&reader.blocking_read(1).await.unwrap()[..], b"a");
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(&reader.blocking_read(1).await.unwrap()[..], b"b");
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(matches!(
            reader.blocking_read(1).await,
            Err(StreamError::Closed)
        ));
        assert_eq!(admissions.load(Ordering::SeqCst), 3);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert!(matches!(
            reader.blocking_read(1).await,
            Err(StreamError::Closed)
        ));
        assert_eq!(admissions.load(Ordering::SeqCst), 3);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn output_streams_admit_each_native_write() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut ctx = WasiFilesystemCtx::default();
        counting_hook(&mut ctx, admissions.clone(), drops.clone());
        let (_path, file) = file_with_contents(b"");
        let mut table = ResourceTable::new();
        let file = table.push(Descriptor::File(file)).unwrap();
        let mut view = WasiFilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        let writer = HostDescriptor::write_via_stream(&mut view, borrowed(&file), 0).unwrap();
        assert_eq!(admissions.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        {
            let writer = view.table.get_mut(&writer).unwrap();
            writer
                .blocking_write_and_flush(bytes::Bytes::from_static(b"a"))
                .await
                .unwrap();
            writer
                .blocking_write_and_flush(bytes::Bytes::from_static(b"b"))
                .await
                .unwrap();
        }
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);

        let appender = HostDescriptor::append_via_stream(&mut view, borrowed(&file)).unwrap();
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        view.table
            .get_mut(&appender)
            .unwrap()
            .blocking_write_and_flush(bytes::Bytes::from_static(b"c"))
            .await
            .unwrap();
        assert_eq!(admissions.load(Ordering::SeqCst), 3);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn open_at_releases_operation_when_result_table_is_full() {
        let tempdir = tempfile::tempdir().unwrap();
        std::fs::write(tempdir.path().join("file"), b"contents").unwrap();

        let admissions = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut ctx = WasiFilesystemCtx::default();
        counting_hook(&mut ctx, admissions.clone(), drops.clone());
        let mut table = ResourceTable::new();
        let directory = table.push(Descriptor::Dir(dir(tempdir.path()))).unwrap();
        table.set_max_capacity(1);
        let mut view = WasiFilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        HostDescriptor::open_at(
            &mut view,
            borrowed(&directory),
            types::PathFlags::empty(),
            "file".into(),
            types::OpenFlags::empty(),
            types::DescriptorFlags::READ,
        )
        .await
        .unwrap_err();
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

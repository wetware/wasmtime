# Descriptor factories on the Wetware v48 maintenance line

This semantic backport starts at `fe367749da9584f974b516f28c5c8055861d1512`
(Wasmtime 48.0.1). Its ownership reference is
`ee836bd2819bcde996cf3f879016eed69dc48828`. It does not upgrade Wasmtime,
its dependency graph, its filesystem primitives, or its P3 runtime interfaces.

## Public API delta from the pinned fork

- Remove `filesystem::FilesystemHandle`, including `new`, `try_clone`, `Deref`,
  and its native-file conversion.
- Restore `File::file` and `Dir::dir` to `Arc<std::fs::File>`.
- Restore the native parameter of `File::new` and `Dir::new` to `std::fs::File`.
- Add `WasiFilesystemCtx::push_descriptor(&self, &mut ResourceTable, Descriptor)`.
  It returns `Result<Resource<Descriptor>, ResourceTableError>`.
- Add `From<FilesystemOperationError> for std::io::Error`; rejection becomes
  `ErrorKind::WouldBlock` with the original typed error as its inner error.
- Under P3, add conversions from `FilesystemOperationError` to the P3
  `ErrorCode` and `FilesystemError`; rejection remains `Busy`.
- Keep `FilesystemOperation::new`, `FilesystemOperationError::Rejected`,
  `WasiFilesystemCtx::set_operation_hook`, and
  `WasiCtxBuilder::filesystem_operation_hook` signatures.

Hook registration changes meaning. Issued preopens, custom descriptors installed
with `push_descriptor`, descendants, and streams retain their attached factory.
Changing a context hook does not change factories on existing descriptors.
A factory can read shared admission state to support later rejection.
`ResourceTable::push` alone does not attach a factory to a newly constructed
custom descriptor. Reattaching through `push_descriptor` replaces its factory,
including when the context has no hook.

Guest WIT signatures, error codes, v48 stream constructors, buffer limits, and
P1 compound-call admission counts remain unchanged. No replacement public
native wrapper is provided.

## Two separate ownership intervals

A generation factory captures its backing-generation lease while descriptors
are idle. Each successful call must return a `FilesystemOperation` containing
both a clone of that lease and the admission receipt. Capturing the lease only
in the factory does not protect a primary worker after descriptor destruction.

Workers retain their native state through cancellation and Store destruction.
Comparison, link, and rename retain the complete secondary descriptor without
calling its factory. Child results retain operation ownership until factory
inheritance and installation or disposal. P3 sessions retain admission between
chunks and release admission at their terminal boundary. A terminal stream can
still retain its descriptor's factory while its native handle remains live.

Native state must close before its final backing owner is dropped. Completed
inert P2 results release admission without waiting for guest polling. Native
resource results retain admission until installation or disposal.

## Wetware migration recipe

The inspected consumer is the local Spokane checkout at
`4ece222a425bee71489d58a6afe1bde3b066fe30`. Relevant files are
`crates/cell/src/fs_intercept.rs`, `crates/cell/src/fs_intercept_tests.rs`, and
`crates/cache/src/fs_work.rs`. These instructions are a proposed consumer
migration; no consumer file or dependency pin was changed.

1. Remove the `FilesystemHandle` import and both wrapping branches in
   `open_read_only_path`. Construct `File` or `Dir` from the original native
   file. Keep the generation lease alive before opening native state. Change this
   helper to return `PreparedDescriptor`; move the lease into its attachment
   factory before returning. A bare descriptor return would release the lease
   before attachment.
2. Construct a factory for each independent generation and its intended
   `CellResources`/`FsScope` admission context. Do not share one factory across
   unrelated Cell admission scopes. Capture the existing
   `StagedContentLease`, `Arc<CellResources>`, and `FsScope`. Call the existing
   `admit_native_filesystem_operation` helper, then return a new
   `FilesystemOperation` that holds the generation lease and returned admission.
   Do not capture a Store. Do not retain admission in the factory itself.
3. Carry custom native results in a native-first prepared payload:

   ```rust,ignore
   struct PreparedDescriptor {
       descriptor: wasmtime_wasi::filesystem::Descriptor,
       attachment: wasmtime_wasi::filesystem::WasiFilesystemCtx,
   }
   ```

   Configure `attachment` with the generation factory before the payload escapes.
   A successful factory invocation can wrap a local value with fields ordered
   as `generation` then `admission`. Keep native handles before that value in
   any owning structure. The temporary context must have no unrelated preopens.
4. Change materialization return types from
   `FsResultEnvelope<Result<Descriptor, MaterializeError>>` to
   `FsResultEnvelope<Result<PreparedDescriptor, MaterializeError>>`, including
   the resolved, cache-path, and IPFS routes. Keep the existing envelope's
   `value`-before-transaction disposal. The envelope needs no new ownership
   registry or lifecycle mechanism.
5. Inside `FsResultEnvelope::install`, perform existing result validation while
   the prepared payload remains intact. Then call
   `prepared.attachment.push_descriptor(view.table, prepared.descriptor)`.
   Drop the attachment context after installation returns. Table-full errors
   drop the supplied native descriptor before the context and envelope release
   their backing/admission claims. Abandoned envelopes drop the prepared native
   descriptor before its attachment context. Do not use `into_inner` to release
   transaction ownership before installation. Do not replace this installation
   with the long-lived global context's hook.
6. Keep the global builder hook for ordinary preopens and non-generation paths.
   Custom descriptors now need explicit attachment too, including descriptors
   without a staged lease that still require operation admission. Existing
   children already inherit their factory; do not overwrite that factory with
   an empty or unrelated global hook.
7. Replace preserving `FilesystemHandle::try_clone` uses with a consumer-local
   native-first pair. For example, retain `native: std::fs::File` before
   `source: Descriptor`. Clone an already attached source descriptor and keep
   that unchanged descriptor until every native duplicate closes. Shared native
   `Arc` clones and independently opened iterators require the same explicit
   ownership. A known generation lease can replace the source descriptor.
   Never expose an escaping native clone without its paired owner.

A naked clone of the public native `Arc`, `std::fs::File::try_clone`, or an OS
handle duplicate does **not** retain external backing ownership automatically.
A long-lived context containing a generation factory also retains the generation;
use the temporary attachment handoff to allow final release.

## Consumer tests required before adoption

Adapt tests that construct `FilesystemHandle`, push custom descriptors directly,
or change a context hook after issuing descriptors. Rejection tests must change
shared state read by an attached factory, or explicitly install a new descriptor.
Preserving-duplicate tests must exercise the native-first pair rather than expect
naked native clones to retain content.

Run Wetware's real generation-eviction and cleanup tests with the prepared result
payload, including cancelled materialization, abandoned completed results,
failed table insertion, child opens, stream cancellation, and Store destruction.
Verify independent generations, one initiating admission, actual native closure,
and final generation cleanup. Wasmtime's modeled-generation tests do not replace
this consumer qualification. Do not change Wetware's Cargo pin or activate this
backport until that separate adoption validation is complete.

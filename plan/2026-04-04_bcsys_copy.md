# 2026-04-04 Implement `Service.Copy` in bcsys

`Service.Copy` in `src/internal/bcsys/bcsys.go:928` is currently a stub: it validates lengths, resolves the destination tx, then returns without updating `out` (it writes a local `ret` slice and drops it).

## Scope

Implement best-effort blob copying for `Copy(ctx, txh, srcTxns, cids, out)` so it attempts to materialize blobs in destination tx and records per-CID success in `out`, while preserving the API contract: per-item failures should usually be represented by `out[i]=false`; return an error only for true operational/internal failures.

## Required behavior changes

- Validate destination rights using `Action_TX_COPY_TO` (not `Action_TX_COPY_FROM`) per action docs in `src/blobcache/handle.go:196`.
- Preserve existing length validation (`len(cids) == len(out)`).
- Actually write results into `out` (remove dead `ret` slice behavior).
- Attempt copy from provided source transactions for each CID:
  - Resolve each source handle with `Action_TX_COPY_FROM`.
  - For each CID, try sources until one succeeds.
  - If no source can provide a valid copy, leave `out[i] = false`.

## Implementation plan

1. Update destination tx checks in `src/internal/bcsys/bcsys.go`:
- Resolve destination with `blobcache.Action_TX_COPY_TO`.
- Enforce mutability (`txn.backend.Params().Modify`) and return `blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "COPY"}` when needed.

2. Resolve and validate source tx handles once:
- Build a resolved source txn list up front via `resolveTx(src, true, blobcache.Action_TX_COPY_FROM)`.
- Return immediate error for invalid/unauthorized source handles (bad input, not per-CID miss).

3. Implement per-CID copy loop:
- Initialize all `out[i] = false`.
- For each `cid`:
  - Iterate source txns (prefer randomized order to match API comment in `src/blobcache/blobcache.go:271`).
  - `Get` blob from source into reusable buffer.
    - `ErrNotFound` => continue to next source.
    - Unexpected I/O/backend failures => return error (wrapped with source tx oid context where possible).
  - Validate content hashes to requested CID under destination hash function before post (avoid inserting wrong blob under hash-algo mismatch cases).
  - `Post` to destination tx.
    - If post succeeds and returns same CID: `out[i] = true`, stop trying sources for this CID.
    - If semantic mismatch (too large / hash mismatch / incompatible): continue with next source or leave false.
    - If true backend/internal failure: return error (wrapped with destination tx oid via `setErrTxOID`).

4. Buffering and limits:
- Reuse a scratch buffer sized safely for source reads (max relevant tx max-size).
- Avoid per-item allocations in hot loop.

5. Logging and consistency:
- Keep existing begin/done debug logs.
- Follow existing error wrapping style (`setErrTxOID`) in this file.

## Tests to add

Add transaction-level copy tests in `src/blobcache/blobcachetests/tx.go` so all service implementations exercising `blobcachetests.ServiceAPI` inherit coverage (`bclocal`, `bchttp`, `bcipc`, `bcremote`, `blobcachecmd` where applicable).

Recommended new cases:
- `CopyBasic`: blob exists in one source tx, copied to destination, `out[i] == true`, destination `Exists/Get` passes.
- `CopyMissingCID`: cid not present in any source => `out[i] == false`, no error.
- `CopyMixedBatch`: multiple CIDs with mixed outcomes.
- `CopyReadOnlyDestination`: destination tx read-only => error.
- `CopyLengthMismatch`: existing error path preserved.

Note: `blobcachecmd.Service.Copy` is currently unimplemented (`src/blobcachecmd/service.go:308`), so CLI-backed API tests may need skip logic for copy-specific cases or follow-up implementation there.

## Verification

Run targeted tests after implementation:
- `go test ./src/blobcache/blobcachetests/...`
- `go test ./src/bclocal ./src/bchttp ./src/bcipc ./src/bcremote`
- `go test ./src/blobcachecmd` (with expected copy-related limitations unless implemented)

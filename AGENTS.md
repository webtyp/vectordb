# AGENTS.md — webtyp/vectordb

Constraints for agents changing this library. Read before touching any file.

## What this repo is

Document store with kNN search, metadata filters and LRU eviction over any `storage.Conn` backend.

## It compiles to WASM

It compiles for the server and for `GOOS=js GOARCH=wasm` (TinyGo): `quota_wasm.go` / `quota_other.go` split the one platform-specific piece. Code that reaches the browser binary follows TinyGo's constraints:

- **No `map`** in code compiled to wasm: use a slice with a linear scan, or `[]fmt.KeyValue`.
- **No reflection, ever**: no `reflect`, no `errors.Is` / `errors.As`, no `sort.Slice`, and no
  `==` / `!=` / `switch` between interface values with non-nil operands. Under TinyGo each of them
  pulls `internal/reflectlite` into the binary (`err == ErrX` included: `error` is an interface).
  Detect sentinels with their `IsX(err)` function (`orm.IsNotFound`, `storage.IsNoRows`).
- Strings and conversions through `webtyp.com/fmt`, not the standard library's `fmt`/`strconv`.

## Reuse before writing

Persistence goes through `webtyp.com/orm` / `webtyp.com/storage` (backend-agnostic `storage.Conn`);
never import a concrete backend (`sqlite`, `postgres`, `indexdb`) outside tests. A missing piece
in a base library is fixed there, never re-implemented here.

## The build that defines "done"

```bash
go install webtyp.com/devflow/cmd/gotest@latest   # once
gotest
```

## Rules

- Existing tests are at the module root; new tests go in `tests/`. A new root-level test needs a top-of-file comment justifying the unexported identifier it uses. Never export a symbol so a test can reach it.
- Every repeated string (error text, keys) is a named constant.

# DataTable — Handoff / Context for Future Work

This document is a **baton-pass** for continuing development (or building downstream projects like **ContingencyTable**) without rereading the entire codebase.

## TL;DR (what this library is)

`DataTableLib::DataTable` parses a CSV into an on-disk, column-oriented representation:

- **Bit-packed** mapped integer ids per cell stored in `mapped_data/column_chunk_<first>_<last>.bin`.
- **Per-column** `int → string` dictionaries stored compactly in `meta_data/chunk_<i>_int_string_maps.bin`.
- A lightweight `load()` path that reads only minimal metadata and performs `O(1)` lookups from disk.

The intent is that after parsing, you can delete the CSV and keep only the output directory (meta + mapped data).

---

## Public API contract (stable surface)

From `include/DataTable/DataTable.h`:

### Lifecycle
- `parse(int threads = 1)`
- `parse(int threads, std::uint32_t chunkSize)`
- `load(const std::string& directory)`

### Header metadata
- `std::uint64_t getColumnCount() const`
- `std::string getColumnHeader(int col) const`
- `std::uint64_t getColumnIndex(const std::string& header) const`
- `std::string getColumnHeaderJson() const`

### Row metadata
- `std::uint64_t getRowCount() const`  
  Includes the header row (`row=0`). Data rows start at `row=1`.

> Note: **Row offsets are no longer a public API**.

### Value lookup
- `std::uint32_t lookupMap(std::uint64_t row, std::uint64_t col) const`
- `std::string getValue(std::uint64_t row, std::uint64_t col) const`

### Column dictionary access
- `std::string getColumnValue(std::uint64_t col, std::uint32_t featureId) const`
- `std::uint32_t getFeatureCount(std::uint64_t col) const`

---

## Key invariants / design rules (important)

These are assumptions baked into file formats and tests.

### Headers
- Header names must be **strict UTF-8**.
- Duplicate header names are **disallowed** (parse throws).
- Header strings are **0–255 bytes** (stored with a 1-byte length prefix).

### Mapped ids
- Mapping is **per-column**, not global.
- Mapped ids are **sequential within each column**.
- `featureId == 0` is reserved as the **empty sentinel** (`""`).
- `getFeatureCount(col)` returns `maxId` for that column, i.e. the number of distinct **non-empty** values.

### Chunking
- Columns are processed in contiguous **chunks** of `chunkSize` columns.
- Each chunk produces:
  - a single `mapped_data/column_chunk_<first>_<last>.bin` file
  - a single `meta_data/chunk_<chunkIndex>_int_string_maps.bin` file
  - one byte in `meta_data/column_chunk_width.bin` storing the bit width for that chunk

### Bit packing
- Values are written/read **LSB-first**.
- `lookupMap()` computes a bit offset and reads the exact `bitWidth` bits, even if the value crosses bytes.

### Row offsets (internal-only)
- Parsing computes `meta_data/row_start_offsets.bin` to correctly locate CSV rows (CSV-aware; newlines inside quotes).
- Row offsets are **written to disk** and tested, but not exposed as a public method.

### Memory footprint goals
- `load()` should be **very light**: no full dataset loaded.
- After a successful `parse()`, large temporary arrays are freed:
  - `rowOffsets_` freed after chunk parsing
  - `currentRowOffsets_` freed once counts are written

---

## On-disk file layout summary

Authoritative details are in `README.md`.

### `meta_data/`
- `counts.bin`
  - `uint64 rowCount` (includes header)
  - `uint64 columnCount`
- `hash.txt`
  - SHA-256 hex digest of the CSV used to generate this directory
  - If hash matches and metadata exists, `parse()` can fast-path (no reparse)
- `header_row.bin`
  - `uint64 columnCount`
  - repeated: `uint8 len` + `len bytes` header
- `row_start_offsets.bin`
  - flat array of `uint64` row start offsets into the original CSV (row 0 header offset is 0)
- `column_chunk_width.bin`
  - one byte per chunk; the bit-width used in that chunk’s mapped data
- `chunk_<i>_int_string_maps.bin`
  - container holding per-column dictionaries for the chunk

### `mapped_data/`
- `column_chunk_<first>_<last>.bin`
  - bit-packed ids in column-major order (for each column, all data rows)

---

## Tests (what they validate)

- Header parsing rules: trimming, quotes, escaped quotes
- Duplicate headers throw
- Invalid UTF-8 headers throw
- `lookupMap()` bit packing correctness including cross-byte boundaries
- Column dictionary correctness (`getColumnValue`, `getFeatureCount`)
- `row_start_offsets.bin` correctness is validated by reading the bin file directly

---

## Practical guidance for the next project (ContingencyTable)

Recommended dependency approach:

1) Treat DataTable as a black box and depend only on the public methods:
   - `load()`
   - `lookupMap()`
   - `getFeatureCount()`
   - `getColumnValue()`
   - header functions

2) If ContingencyTable needs row offsets, prefer reading the artifact file directly:
   - `meta_data/row_start_offsets.bin`
   - (but consider whether that should be a formal “format contract” before relying on it)

3) If you need long-term compatibility, consider adding a format version file (not implemented yet):
   - e.g. `meta_data/format_version.txt` or `meta_data/format.json`

---

## How to build & run tests

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

---

## Recent notable decisions / history

- Row offsets were previously exposed via a public `getRowOffset()`; this was removed to keep the public API clean and avoid implying offsets are needed for normal access.
- Offsets are still computed and persisted because they are important for parsing correctness.
- A newer chunk dictionary format packs **per-column** dictionaries into a single file per chunk (`chunk_<i>_int_string_maps.bin`).
- Parsing produces sequential per-column ids; the dictionary lookups assume that.

---

## Using this project from another project (CMake FetchContent)

If you want a downstream project (e.g. **ContingencyTable**) to fetch a specific version of DataTable from GitHub and rebuild it as part of the build, use CMake `FetchContent` pinned to a tag:

```cmake
include(FetchContent)

FetchContent_Declare(
  DataTable
  GIT_REPOSITORY https://github.com/<ORG_OR_USER>/DataTable.git
  GIT_TAG v1.0.0
)

# Optional: avoid building DataTable tests/examples as part of the dependency
set(DATATABLE_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(DATATABLE_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(DataTable)

add_executable(ContingencyTable main.cpp)
target_link_libraries(ContingencyTable PRIVATE DataTable::DataTable)
```

Notes:
- DataTable’s transitive dependencies are also fetched via `FetchContent`.
- HashLibrary is fetched via **HTTPS** (not SSH), so CI/build machines don’t need GitHub SSH keys.

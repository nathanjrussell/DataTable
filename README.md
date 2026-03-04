# DataTable

A C++ library for parsing RFC4180-style CSV files into a compact, on-disk, **column-oriented** representation with:

- fast `O(1)` cell lookup (`lookupMap()` / `getValue()`)
- per-column **sequential** categorical encodings (string → int) and a compact reverse map (int → string)
- persisted metadata and mapped data under an `outputDirectory/`

---

## Table of contents

- [Build](#build)
- [Quick start](#quick-start)
- [CSV parsing rules](#csv-parsing-rules)
  - [Quoting and escaping](#quoting-and-escaping)
  - [Whitespace normalization](#whitespace-normalization)
  - [Newline handling](#newline-handling)
  - [Header constraints (UTF-8 + no duplicates)](#header-constraints-utf-8--no-duplicates)
- [On-disk layout](#on-disk-layout)
  - [`meta_data/counts.bin`](#meta_datacountsbin)
  - [`meta_data/hash.txt`](#meta_datahashtxt)
  - [`meta_data/header_row.bin`](#meta_dataheader_rowbin)
  - [`meta_data/row_start_offsets.bin`](#meta_datarow_start_offsetsbin)
  - [`meta_data/column_chunk_width.bin`](#meta_datacolumn_chunk_widthbin)
  - [`meta_data/chunk_<i>_int_string_maps.bin`](#meta_datachunk_i_int_string_mapsbin)
  - [`mapped_data/column_chunk_<first>_<last>.bin`](#mapped_datacolumn_chunk_first_lastbin)
- [Public API](#public-api)
  - [Lifecycle (`parse`, `load`)](#lifecycle-parse-load)
  - [Header access](#header-access)
  - [Row counts](#row-counts)
  - [Mapped value lookup](#mapped-value-lookup)
  - [Column dictionaries](#column-dictionaries)
- [Examples](#examples)

---

## Build

Configure and build using CMake (out-of-source build):

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

Run unit tests:

```sh
ctest --test-dir build --output-on-failure
```

---

## Quick start

```cpp
#include <DataTable/DataTable.h>

DataTableLib::DataTable dt("/path/in.csv", "/path/out_dir");

// Parse CSV into /path/out_dir/{meta_data,mapped_data}
// chunkSize controls how many columns are grouped into one mapped_data chunk file.
dt.parse(/*threads=*/1, /*chunkSize=*/1000);

// Query
std::uint64_t col = dt.getColumnIndex("Age");
std::string v = dt.getValue(/*row=*/10, col);
std::uint32_t id = dt.lookupMap(/*row=*/10, col);
std::string label = dt.getColumnValue(col, id);
```

You can also load an already-parsed directory without re-parsing the CSV:

```cpp
DataTableLib::DataTable loaded;
loaded.load("/path/out_dir");
```

---

## CSV parsing rules

This project assumes RFC 4180-style CSV with project-specific normalization rules.

### Quoting and escaping

- Fields may be unquoted or double-quoted.
- Inside a double-quoted field, a literal double quote is represented by doubling it (`""`).
- Delimiters and newlines are treated as structural separators only when **not** inside a double-quoted field.

### Whitespace normalization

- For **unquoted** fields, ASCII whitespace immediately **around the comma delimiter** is removed.
  - Example: `a ,  b` parses as `a` and `b`.
- For **quoted** fields, whitespace inside the quotes is preserved exactly.
  - Example: `"  a  "` parses as `␠␠a␠␠`.
- For **unquoted** fields, ASCII whitespace immediately before a **record delimiter** (newline) is removed.
  - Example: `a␠␠\n` parses as `a`.
  - Example: `"a␠␠"\n` preserves trailing spaces because they are inside quotes.

### Newline handling

- Treat `\r\n` the same as `\n` both inside and outside quoted fields.
- If a newline occurs inside a quoted field, it is part of the field value and does not terminate the record.

### Header constraints (UTF-8 + no duplicates)

Header names (after the trimming rules above) must satisfy:

- **Strict UTF-8** (invalid UTF-8 sequences throw during `parse()`).
- **Unique** header names (duplicate names throw during `parse()`).
- **0–255 bytes** because header strings are stored using a 1-byte length prefix.

---

## On-disk layout

After parsing, the output directory contains:

```text
outputDirectory/
  meta_data/
    counts.bin
    hash.txt
    header_row.bin
    row_start_offsets.bin
    column_chunk_width.bin
    chunk_0_int_string_maps.bin
    chunk_1_int_string_maps.bin
    ...
  mapped_data/
    column_chunk_0_999.bin
    column_chunk_1000_1999.bin
    ...
```

All integer fields are stored in the machine's native little-endian representation (this project currently assumes local read/write on the same architecture).

### `meta_data/counts.bin`

Binary file containing two `uint64_t` values:

1. `rowCount` (includes the header row)
2. `columnCount`

### `meta_data/hash.txt`

A text file containing the SHA-256 hex digest of the input CSV used to generate the current output directory.

If `hash.txt` exists and matches the current input CSV hash, `parse()` will fast-path by loading existing metadata (instead of re-parsing).

### `meta_data/header_row.bin`

Stores the CSV header row in a compact length-prefixed format.

Format:

```text
uint64_t columnCount
repeat columnCount times:
  uint8_t  headerByteLength   (0..255)
  uint8_t[headerByteLength] headerBytes (UTF-8)
```

### `meta_data/row_start_offsets.bin`

Flat array of `uint64_t` byte offsets into the original CSV.

- Entry `i` is the byte offset (from the beginning of the CSV file) where CSV row `i` begins.
- Row `0` is the header row, so entry `0` is always `0`.

These offsets are computed by scanning in binary mode and treating newlines as row delimiters **only outside quotes**.

### `meta_data/column_chunk_width.bin`

One byte per column-chunk.

- Byte `i` is the **bit width** used to store mapped ids in `mapped_data/column_chunk_*.bin` for chunk `i`.
- `bitWidth` is computed from the maximum mapped id encountered in that chunk.

### `meta_data/chunk_<i>_int_string_maps.bin`

Per-chunk container file holding **per-column** `int → string` dictionaries.

Each chunk corresponds to the same chunk index used by `mapped_data/column_chunk_<first>_<last>.bin`.

Container header:

```text
uint64_t chunkCols
uint64_t firstCol
uint64_t offsets[chunkCols + 1]   // absolute file offsets (uint64_t)
```

- `chunkCols` is the number of columns in this chunk.
- `firstCol` is the global column index of the first column in this chunk.
- `offsets[k]` points to the start of the per-column dictionary block for `localCol = k`.

Per-column dictionary block at `offsets[localCol]`:

```text
uint32_t maxId
uint32_t stringOffsets[maxId + 2]
uint8_t  stringBlob[stringOffsets[maxId + 1]]
```

Where:

- ids are sequential per column: `0..maxId`
- id `0` is reserved for the empty value (`""`)
- `stringOffsets[id]` provides the start (in bytes) within `stringBlob` for that id's string
- the string length is `stringOffsets[id+1] - stringOffsets[id]`

### `mapped_data/column_chunk_<first>_<last>.bin`

The bit-packed mapped ids for a contiguous group of columns.

Chunk layout:

- Columns are stored **column-major**.
- For each column in the chunk, all data rows are written in order.
- The header row is not stored.

Given:

- `dataRowCount = rowCount - 1`
- `localCol = col - firstCol`
- `index = localCol * dataRowCount + (row - 1)`

The mapped id is stored at bit offset:

```text
bitOffset = index * bitWidth
```

Bits are written and read LSB-first.

---

## Public API

The primary public class is `DataTableLib::DataTable`.

### Lifecycle (`parse`, `load`)

- `void parse(int threads = 1)`
- `void parse(int threads, std::uint32_t chunkSize)`
- `void load(const std::string& directory)`

`parse()` reads the input CSV, writes metadata + mapped data into `outputDirectory`, and enables all getters.

`load()` loads an already-parsed directory so you can use lookup functions without re-parsing the CSV.

### Header access

- `std::uint64_t getColumnCount() const`
- `std::string getColumnHeader(int col) const`
- `std::uint64_t getColumnIndex(const std::string& header) const`
- `std::string getColumnHeaderJson() const`

### Row counts

- `std::uint64_t getRowCount() const`

Row indexing semantics:

- `row = 0` is the header row
- `row >= 1` are data rows

This library still writes `meta_data/row_start_offsets.bin` as internal metadata (used for parsing correctness and tooling),
but row offsets are intentionally **not part of the public API**.

### Mapped value lookup

- `std::uint32_t lookupMap(std::uint64_t row, std::uint64_t col) const`
- `std::string getValue(std::uint64_t row, std::uint64_t col) const`

Notes:

- `row = 0` is not supported by `lookupMap()` and `getValue()`.
- `lookupMap()` returns the per-column sequential id for that cell.
- `getValue()` returns the string by translating `lookupMap()` through the per-column dictionary.

### Column dictionaries

- `std::string getColumnValue(std::uint64_t col, std::uint32_t featureId) const`
- `std::uint32_t getFeatureCount(std::uint64_t col) const`

Where:

- `featureId = 0` is always the empty value (`""`).
- `getFeatureCount(col)` returns the maximum id in that column (i.e. the number of distinct non-empty values).

---

## Examples

### `example_row_offsets`

Build and run:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
./build/example_row_offsets
```

### CLion

Open the repository root (the folder containing `CMakeLists.txt`). CLion will load the CMake project and discover the `DataTableTests` tests automatically.

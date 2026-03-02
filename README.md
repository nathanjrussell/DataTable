# DataTable

C++ library scaffold with CMake and GoogleTest.

# Parsing Assumptions

This project assumes RFC 4180-style CSV with the following project-specific normalization rules.

## Quoting and escaping

- Fields may be unquoted or double-quoted.
- Inside a double-quoted field, a literal double quote is represented by doubling it (`""`).
- Delimiters and newlines are treated as structural separators only when **not** inside a double-quoted field.

## Whitespace normalization

- For **unquoted** fields, whitespace immediately **around the comma delimiter** is removed.
  - Example: `a ,  b` parses as `a` and `b`.
- For **quoted** fields, whitespace inside the quotes is preserved exactly (no trimming inside quotes).
  - Example: `"  a  "` parses as `␠␠a␠␠`.
- For **unquoted** fields, whitespace immediately before a **record delimiter** (newline) is removed.
  - Example: `a␠␠\n` parses as `a`.
  - Example: `a␠␠\r\n` parses as `a`.
  - Example: `"a␠␠"\n` preserves the trailing spaces because they are inside quotes.

## Newline handling

- Treat `\r\n` the same as `\n` both inside and outside quoted fields.
- If a newline occurs inside a quoted field, it is part of the field value and does not terminate the record.

## Header row encoding constraints

- Column header strings are stored in `meta_data/header_row.bin` using a **1-byte length prefix** followed by the header bytes.
- Therefore, each header string (after applying the trimming rules above) must be **0–255 bytes**.

# Parsing Goal

The ultimate goal for `DataTable` is to:

1. Accept a CSV file.
2. Map categorical string values to integer ids (for compact/storage-efficient representation).
3. Transpose the encoded data so downstream chi-square analysis can be performed columnwise.
4. Support reverse mapping from integer ids back to the original strings so results can be reported in human-readable form.

## Row offsets metadata (`meta_data/row_start_offsets.bin`)

During `DataTable::parse()`, a metadata file is written at:

- `outputDirectory/meta_data/row_start_offsets.bin`

This file is a flat array of 64-bit unsigned integers (`uint64_t`) stored in native binary form.

- The *i-th* 64-bit integer is the byte offset (from the start of the original CSV file) where CSV row *i* begins.
- `row 0` is the header row, so the first entry is always `0`.
- `row 1` is the first data row.

These offsets are computed by scanning the CSV in binary mode and counting bytes exactly as stored in the file.
Newlines are treated as row delimiters only when they occur **outside** double-quoted fields (RFC 4180 behavior),
so embedded newlines inside quoted fields do not split rows.

The public API reflects these semantics:

- `getRowCount()` returns the number of rows for which offsets were recorded (including the header row).
- `getRowOffset(row)` returns the byte offset for that row. If you `seekg()` to that offset in the original CSV,
  the stream position will be at the first byte of the row.

Row offsets are also used to validate input correctness during parsing: as each row boundary is found, the parser
counts comma delimiters outside quotes and throws if any row does not have the same number of columns as the header.

## Build

Configure and build using CMake (out-of-source build):

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

### Run unit tests (ctest)

After building, run the test suite via CTest:

```sh
ctest --test-dir build --output-on-failure
```

Notes:
- If you change CMake options (or switch Debug/Release), re-run the `cmake -S ...` configure step.
- You can also run a single test by using CTest's `-R` regex filter.

### Build and run the `example_row_offsets` example

The example is built when `DATATABLE_BUILD_EXAMPLES` is ON (it is ON by default in `CMakeLists.txt`).

1) Configure + build (same as above):

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

2) Run the example executable:

```sh
./build/example_row_offsets
```

If you built with a multi-config generator (common on Windows) or you selected `-DCMAKE_BUILD_TYPE=Release`,
adjust the binary path accordingly.

## CLion

Open the repository root (the folder containing `CMakeLists.txt`). CLion will load the CMake project and discover the `DataTableTests` tests automatically.

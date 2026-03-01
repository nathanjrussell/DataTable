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

## Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

## Run tests

```sh
ctest --test-dir build --output-on-failure
```

## CLion

Open the repository root (the folder containing `CMakeLists.txt`). CLion will load the CMake project and discover the `DataTableTests` tests automatically.

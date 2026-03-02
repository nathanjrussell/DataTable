#!/usr/bin/env python3
"""Generate a very large CSV dataset.

Default output:
- 60,000 columns
- 20,000 data rows (plus 1 header row)

This generator is designed to be streaming and low-memory:
- Writes row-by-row.
- Does NOT build the full row in memory.
- Uses a small, deterministic set of categorical values.

WARNING: The resulting file will be extremely large.
Rough sizes (very approximate):
- Header alone is ~ ("col_59999" + commas) ~ hundreds of KB.
- Each row is tens/hundreds of KB depending on value format.
- Total output can easily be multiple GB.

Usage examples:
  python3 generate_large_csv_60k_20k.py --out huge.csv
  python3 generate_large_csv_60k_20k.py --rows 200 --cols 1000 --out small.csv

By default this does NOT generate an index file (it would also be huge).
"""

from __future__ import annotations

import argparse
from pathlib import Path


def csv_escape(field: str) -> str:
    """RFC-4180 escaping.

    Wrap in quotes if field contains comma, quote, CR, or LF.
    Double internal quotes.
    """

    if any(c in field for c in [",", '"', "\n", "\r"]):
        field = field.replace('"', '""')
        return f'"{field}"'
    return field


def build_field(row: int, col: int, alphabet_size: int) -> str:
    """Deterministic categorical field.

    Keep fields short to reduce file size and speed up generation.
    """

    # Empty field sometimes.
    if (row + col) % 97 == 0:
        return ""

    # Small categorical alphabet (A00..A{alphabet_size-1}).
    # This keeps the dataset realistic for dictionary encoding.
    v = (row * 1315423911 + col * 2654435761) % alphabet_size
    return f"A{v:03d}"


def write_header(f, cols: int) -> None:
    # Use simple column names to keep header size reasonable.
    # Stream it to avoid building a huge list.
    for c in range(cols):
        if c:
            f.write(b",")
        f.write(f"col_{c}".encode("utf-8"))
    f.write(b"\n")


def generate_csv(out_path: Path, rows: int, cols: int, alphabet_size: int, report_every: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("wb") as f:
        write_header(f, cols)

        for r in range(1, rows + 1):
            # Stream each row: write field-by-field.
            for c in range(cols):
                if c:
                    f.write(b",")

                val = build_field(r, c, alphabet_size)
                # fields are small and safe, but keep it correct CSV.
                f.write(csv_escape(val).encode("utf-8"))

            f.write(b"\n")

            if report_every > 0 and (r % report_every == 0):
                print(f"wrote row {r}/{rows}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("test_dataset_60k_20k.csv"))
    ap.add_argument("--rows", type=int, default=20_000, help="number of data rows (not including header)")
    ap.add_argument("--cols", type=int, default=60_000, help="number of columns")
    ap.add_argument(
        "--alphabet-size",
        type=int,
        default=256,
        help="number of distinct categorical values used across the table",
    )
    ap.add_argument(
        "--report-every",
        type=int,
        default=100,
        help="print progress every N rows (0 disables)",
    )

    args = ap.parse_args()

    if args.rows <= 0:
        raise SystemExit("--rows must be > 0")
    if args.cols <= 0:
        raise SystemExit("--cols must be > 0")
    if args.alphabet_size <= 1:
        raise SystemExit("--alphabet-size must be > 1")

    print(f"Generating CSV: {args.rows} rows × {args.cols} columns")
    print(f"Output: {args.out}")

    generate_csv(args.out, args.rows, args.cols, args.alphabet_size, args.report_every)

    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


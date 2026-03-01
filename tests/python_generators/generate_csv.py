import random
import sys
from pathlib import Path

ROWS = 10_000
COLS = 40
OUTPUT_FILE = "test_dataset_10k_40cols.csv"
INDEX_FILE = "test_dataset_10k_40cols.index"

random.seed(42)  # deterministic reproducibility


def csv_escape(field: str) -> str:
    """RFC-4180 escaping:
    - Wrap in quotes if field contains comma, quote, CR, or LF
    - Double internal quotes
    """
    if any(c in field for c in [',', '"', '\n', '\r']):
        field = field.replace('"', '""')
        return f'"{field}"'
    return field


def build_field(row: int, col: int) -> str:
    """Return a deterministic field value.

    Requirements:
    - Include escaped quotes
    - Include quoted fields with embedded newlines
    - Keep output deterministic
    """

    # Make most columns simple to keep file size reasonable.
    if col % 7 == 0:
        # numeric-ish
        return str((row * 17 + col * 3) % 100000)

    # Ensure we exercise escaped quotes and embedded newlines.
    if col % 7 == 1:
        # Contains quotes -> will be escaped
        return f'He said "hello" at r{row}c{col}'

    if col % 7 == 2:
        # Contains embedded newline(s) -> must be quoted
        # Mix \n and \r\n in data; the generator writes \n row delimiters.
        if row % 2 == 0:
            return f"multi-line field r{row}c{col}\nline2\nline3"
        return f"multi-line field r{row}c{col}\r\nline2"

    if col % 7 == 3:
        # Contains comma and quotes and newline (stress)
        return f"comma, and quote: \"Q\" and newline\nend r{row}c{col}"

    if col % 7 == 4:
        # missing
        return ""

    if col % 7 == 5:
        # lots of whitespace (kept as-is; parsing rules later may trim)
        return f"   padded   r{row}c{col}   "

    # default categorical-ish
    return f"cat_r{row}_c{col}"


def recompute_offsets_csv_aware(csv_path: Path) -> list[int]:
    """Recompute logical CSV row start byte offsets.

    Row delimiter is newline outside quoted fields.
    Handles escaped quotes "" inside quoted fields.
    Treats CRLF as a single newline delimiter outside quotes.
    Inside quotes, CRLF/CR are treated as data (but bytes are still counted).
    """

    b = csv_path.read_bytes()
    n = len(b)

    offsets = [0]

    pos = 0
    in_quotes = False
    while pos < n:
        c = b[pos]

        if c == 0x22:  # '"'
            if in_quotes:
                # Escaped quote "" inside quoted field.
                if pos + 1 < n and b[pos + 1] == 0x22:
                    pos += 2
                    continue
                in_quotes = False
                pos += 1
                continue
            else:
                in_quotes = True
                pos += 1
                continue

        if not in_quotes:
            if c == 0x0A:  # \n
                pos += 1
                if pos < n:
                    offsets.append(pos)
                continue

            if c == 0x0D:  # \r or \r\n
                if pos + 1 < n and b[pos + 1] == 0x0A:
                    pos += 2
                else:
                    pos += 1
                if pos < n:
                    offsets.append(pos)
                continue
        else:
            # Inside quotes: treat CRLF as data; advance over LF so scanning stays consistent.
            if c == 0x0D and pos + 1 < n and b[pos + 1] == 0x0A:
                pos += 2
                continue

        pos += 1

    # drop any offset == EOF
    offsets = [x for x in offsets if x < n]
    return offsets


def load_index_offsets(index_path: Path) -> list[int]:
    offsets = []
    with index_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            _, off_s = line.split(",", 1)
            offsets.append(int(off_s))
    return offsets


def verify(csv_path: Path, index_path: Path) -> None:
    expected = load_index_offsets(index_path)
    computed = recompute_offsets_csv_aware(csv_path)

    if len(expected) != len(computed):
        raise RuntimeError(f"Index count mismatch: index={len(expected)} computed={len(computed)}")

    for i, (e, c) in enumerate(zip(expected, computed)):
        if e != c:
            raise RuntimeError(f"Offset mismatch at row {i}: index={e} computed={c}")


with open(OUTPUT_FILE, "wb") as f, open(INDEX_FILE, "w", encoding="utf-8") as idx:
    offset = 0

    # Header
    header = [f"column_{i+1}" for i in range(COLS)]
    header_line = ",".join(header) + "\n"
    encoded = header_line.encode("utf-8")

    idx.write(f"0,{offset}\n")
    f.write(encoded)
    offset += len(encoded)

    # Rows
    for row_number in range(1, ROWS + 1):
        idx.write(f"{row_number},{offset}\n")

        row = []
        for col_index in range(COLS):  # col_index is 0-based
            row.append(build_field(row_number, col_index))

        escaped_row = [csv_escape(str(field)) for field in row]
        line = ",".join(escaped_row) + "\n"
        encoded = line.encode("utf-8")

        f.write(encoded)
        offset += len(encoded)

print(f"Generated {ROWS} rows × {COLS} columns")
print(f"CSV file: {OUTPUT_FILE}")
print(f"Row index file (row_number, byte_offset): {INDEX_FILE}")

# Optional self-check (default on). Disable with --no-verify.
if "--no-verify" not in sys.argv:
    try:
        verify(Path(OUTPUT_FILE), Path(INDEX_FILE))
        print("VERIFY PASS: index matches CSV offsets")
    except Exception as e:
        print(f"VERIFY FAIL: {e}")
        raise SystemExit(1)

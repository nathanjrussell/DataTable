#include <DataTable/DataTable.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace DataTableLib {

namespace {

std::string trimAsciiWhitespace(const std::string& s) {
  std::size_t start = 0;
  while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start])) != 0) {
    ++start;
  }

  std::size_t end = s.size();
  while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
    --end;
  }
  return s.substr(start, end - start);
}

// Parses the first CSV record (header row) from an input stream.
// Implements:
// - RFC4180-style quoting with doubled quotes
// - comma delimiter
// - supports \n and \r\n (\r treated as \n)
// - trims unquoted fields (both around delimiter and before newline) while preserving whitespace inside quotes
std::vector<std::string> parseHeaderRow(std::istream& in) {
  std::vector<std::string> fields;
  std::string field;
  bool inQuotes = false;
  bool fieldWasQuoted = false;
  bool afterClosingQuote = false;

  auto flushField = [&]() {
    if (!fieldWasQuoted) {
      fields.emplace_back(trimAsciiWhitespace(field));
    } else {
      fields.emplace_back(field);
    }
    field.clear();
    fieldWasQuoted = false;
    afterClosingQuote = false;
  };

  while (true) {
    const int ci = in.get();
    if (ci == EOF) {
      flushField();
      break;
    }

    char c = static_cast<char>(ci);

    // Normalize CRLF/CR to LF.
    if (!inQuotes && c == '\r') {
      if (in.peek() == '\n') {
        in.get();
      }
      flushField();
      break;
    }
    if (!inQuotes && c == '\n') {
      flushField();
      break;
    }

    if (inQuotes) {
      if (c == '"') {
        if (in.peek() == '"') {
          in.get();
          field.push_back('"');
        } else {
          inQuotes = false;
          afterClosingQuote = true;
        }
      } else if (c == '\r') {
        // Inside quotes: treat CRLF/CR as LF in data.
        if (in.peek() == '\n') {
          in.get();
        }
        field.push_back('\n');
      } else {
        field.push_back(c);
      }
      continue;
    }

    // Not in quotes.
    if (afterClosingQuote) {
      // Ignore whitespace between closing quote and delimiter/newline.
      if (std::isspace(static_cast<unsigned char>(c)) != 0) {
        continue;
      }
      // Next should be delimiter or newline (already handled above). If not, be permissive and treat as data.
      afterClosingQuote = false;
    }

    if (c == ',') {
      flushField();
      continue;
    }

    if (c == '"') {
      // Start quoted field only if we're at field start or we've only seen whitespace.
      // Since we trim unquoted fields anyway, tolerating leading whitespace before opening quote is useful.
      if (trimAsciiWhitespace(field).empty()) {
        field.clear();
        inQuotes = true;
        fieldWasQuoted = true;
        continue;
      }
      // Otherwise treat it as a literal quote in unquoted data.
      field.push_back('"');
      continue;
    }

    field.push_back(c);
  }

  return fields;
}

void writeU64(std::ostream& out, std::uint64_t v) {
  out.write(reinterpret_cast<const char*>(&v), sizeof(v));
  if (!out) throw std::runtime_error("Failed writing uint64");
}

void writeU8(std::ostream& out, std::uint8_t v) {
  out.write(reinterpret_cast<const char*>(&v), sizeof(v));
  if (!out) throw std::runtime_error("Failed writing uint8");
}

std::uint64_t readU64(std::istream& in) {
  std::uint64_t v = 0;
  in.read(reinterpret_cast<char*>(&v), sizeof(v));
  if (in.gcount() != static_cast<std::streamsize>(sizeof(v))) {
    throw std::runtime_error("Corrupt header_row.bin: missing column count");
  }
  return v;
}

std::string readLenPrefixedString(std::istream& in) {
  std::uint8_t len = 0;
  in.read(reinterpret_cast<char*>(&len), 1);
  if (in.gcount() != 1) {
    throw std::runtime_error("Corrupt header_row.bin: missing string length");
  }

  std::string s(static_cast<std::size_t>(len), '\0');
  if (len > 0) {
    in.read(s.data(), static_cast<std::streamsize>(len));
    if (in.gcount() != static_cast<std::streamsize>(len)) {
      throw std::runtime_error("Corrupt header_row.bin: missing string bytes");
    }
  }
  return s;
}

std::filesystem::path headerRowBinPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "meta_data" / "header_row.bin";
}

}  // namespace

DataTable::DataTable(std::string inputFilePath, std::string outputDirectory)
    : inputFilePath_(std::move(inputFilePath)),
      outputDirectory_(std::move(outputDirectory)) {}

void DataTable::setInputFilePath(const std::string& inputFilePath) {
  inputFilePath_ = inputFilePath;
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();
}

void DataTable::setOutputDirectory(const std::string& outputDirectory) {
  outputDirectory_ = outputDirectory;
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();
}

const std::string& DataTable::inputFilePath() const noexcept {
  return inputFilePath_;
}

const std::string& DataTable::outputDirectory() const noexcept {
  return outputDirectory_;
}

void DataTable::ensureParsed() const {
  if (!parseCompleted_) {
    throw std::runtime_error("parse() must be called successfully before accessing metadata");
  }
}

std::uint64_t DataTable::getColumnCount() const {
  ensureParsed();
  return columnCount_;
}

std::string DataTable::getColumnHeader(int col) const {
  ensureParsed();
  if (col < 0) throw std::out_of_range("col must be >= 0");

  const std::uint64_t ncols = columnCount_;
  if (static_cast<std::uint64_t>(col) >= ncols) {
    throw std::out_of_range("col out of range");
  }

  std::ifstream in(headerRowBinPath(outputDirectory_), std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open header_row.bin");

  // Skip column count.
  (void)readU64(in);

  for (std::uint64_t i = 0; i < ncols; ++i) {
    std::string s = readLenPrefixedString(in);
    if (static_cast<std::uint64_t>(col) == i) return s;
  }

  throw std::runtime_error("Corrupt header_row.bin: unexpected EOF");
}

std::string DataTable::getColumnHeaderJson() const {
  ensureParsed();

  const std::uint64_t ncols = columnCount_;

  std::ifstream in(headerRowBinPath(outputDirectory_), std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open header_row.bin");

  // Skip column count.
  (void)readU64(in);

  nlohmann::json j = nlohmann::json::array();
  for (std::uint64_t i = 0; i < ncols; ++i) {
    j.push_back(readLenPrefixedString(in));
  }

  return j.dump();
}

namespace {

// Parses a data row starting at the stream's current position and returns the column count.
// Stops after consuming the row delimiter (or EOF). This is used to validate column counts.
std::uint64_t parseRowAndCountColumns(std::istream& in) {
  std::uint64_t cols = 0;
  bool inQuotes = false;
  bool afterClosingQuote = false;

  bool fieldHasNonWhitespace = false;
  bool seenAnyCharInField = false;

  auto finishField = [&]() {
    ++cols;
    // reset field state
    fieldHasNonWhitespace = false;
    seenAnyCharInField = false;
    afterClosingQuote = false;
  };

  while (true) {
    const int ci = in.get();
    if (ci == EOF) {
      // If EOF occurs, count the last field if we have started a row at all.
      if (seenAnyCharInField || cols > 0) {
        finishField();
      }
      break;
    }

    char c = static_cast<char>(ci);

    if (inQuotes) {
      if (c == '"') {
        if (in.peek() == '"') {
          in.get();
          seenAnyCharInField = true;
          fieldHasNonWhitespace = true;
        } else {
          inQuotes = false;
          afterClosingQuote = true;
        }
      } else {
        // Normalize CRLF/CR to LF inside quotes.
        if (c == '\r') {
          if (in.peek() == '\n') in.get();
          c = '\n';
        }
        seenAnyCharInField = true;
        if (std::isspace(static_cast<unsigned char>(c)) == 0) {
          fieldHasNonWhitespace = true;
        }
      }
      continue;
    }

    // Not in quotes.
    if (afterClosingQuote) {
      // Ignore whitespace between closing quote and delimiter/newline.
      if (std::isspace(static_cast<unsigned char>(c)) != 0) {
        continue;
      }
      afterClosingQuote = false;
    }

    // Row delimiter
    if (c == '\r') {
      if (in.peek() == '\n') in.get();
      finishField();
      break;
    }
    if (c == '\n') {
      finishField();
      break;
    }

    // Field delimiter
    if (c == ',') {
      finishField();
      continue;
    }

    // Quote: only start quoted field if we've only seen whitespace so far in this field.
    if (c == '"' && !fieldHasNonWhitespace) {
      inQuotes = true;
      seenAnyCharInField = true;
      continue;
    }

    // Regular character
    seenAnyCharInField = true;
    if (std::isspace(static_cast<unsigned char>(c)) == 0) {
      fieldHasNonWhitespace = true;
    }
  }

  return cols;
}

std::filesystem::path rowOffsetsBinPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "meta_data" / "row_start_offsets.bin";
}

class Barrier {
public:
  explicit Barrier(std::size_t count) : count_(count), initial_(count) {}

  void arrive_and_wait() {
    std::unique_lock<std::mutex> lk(m_);
    if (--count_ == 0) {
      count_ = initial_;
      ++generation_;
      cv_.notify_all();
      return;
    }
    const std::size_t gen = generation_;
    cv_.wait(lk, [&] { return gen != generation_; });
  }

private:
  std::mutex m_;
  std::condition_variable cv_;
  std::size_t count_;
  const std::size_t initial_;
  std::size_t generation_ = 0;
};

// From a given offset, scan forward to the next row start (the byte after an unescaped newline).
// To determine quote-state correctly, we first back up to the previous physical newline and scan forward.
std::uint64_t findNextRowStart(const std::string& filePath, std::uint64_t startOffset) {
  std::ifstream in(filePath, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open input CSV during row scan: " + filePath);

  const std::uint64_t fileSize = static_cast<std::uint64_t>(std::filesystem::file_size(filePath));
  if (startOffset > fileSize) startOffset = fileSize;

  // Seek backwards to a likely record boundary so quote-state can be reconstructed.
  // We search for a '\n' or '\r' before startOffset.
  std::uint64_t scanStart = 0;
  if (startOffset > 0) {
    const std::uint64_t maxBack = 1024 * 1024; // 1MiB safety window
    const std::uint64_t begin = (startOffset > maxBack) ? (startOffset - maxBack) : 0;
    const std::size_t window = static_cast<std::size_t>(startOffset - begin);

    std::vector<char> buf(window);
    in.seekg(static_cast<std::streamoff>(begin), std::ios::beg);
    in.read(buf.data(), static_cast<std::streamsize>(window));

    // Find last newline char in the buffer.
    for (std::size_t i = window; i-- > 0;) {
      if (buf[i] == '\n' || buf[i] == '\r') {
        scanStart = begin + static_cast<std::uint64_t>(i + 1);
        break;
      }
    }
  }

  // Now scan forward from scanStart, tracking quote state, until we pass startOffset and see
  // the next unescaped newline.
  in.clear();
  in.seekg(static_cast<std::streamoff>(scanStart), std::ios::beg);
  if (!in) throw std::runtime_error("Failed to seek input CSV during row scan");

  bool inQuotes = false;
  while (true) {
    const std::uint64_t pos = static_cast<std::uint64_t>(in.tellg());
    const int ci = in.get();
    if (ci == EOF) return pos;

    char c = static_cast<char>(ci);

    if (inQuotes) {
      if (c == '"') {
        if (in.peek() == '"') {
          in.get();
        } else {
          inQuotes = false;
        }
      }
      continue;
    }

    if (c == '"') {
      inQuotes = true;
      continue;
    }

    // Only treat newline as row delimiter if we're not in quotes.
    if (c == '\r') {
      if (in.peek() == '\n') in.get();
      const std::uint64_t candidate = static_cast<std::uint64_t>(in.tellg());
      if (candidate >= startOffset) return candidate;
      continue;
    }
    if (c == '\n') {
      const std::uint64_t candidate = static_cast<std::uint64_t>(in.tellg());
      if (candidate >= startOffset) return candidate;
      continue;
    }
  }
}

} // namespace

void DataTable::parse(int threads) {
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();

  if (threads <= 0) {
    throw std::runtime_error("threads must be >= 1");
  }

  if (inputFilePath_.empty()) {
    throw std::runtime_error("Input file path is empty");
  }
  if (outputDirectory_.empty()) {
    throw std::runtime_error("Output directory is empty");
  }

  std::ifstream in(inputFilePath_, std::ios::binary);
  if (!in) {
    throw std::runtime_error("Failed to open input CSV: " + inputFilePath_);
  }

  const std::vector<std::string> headers = parseHeaderRow(in);
  const std::uint64_t ncols = static_cast<std::uint64_t>(headers.size());

  const std::filesystem::path metaDir = std::filesystem::path(outputDirectory_) / "meta_data";
  std::filesystem::create_directories(metaDir);

  const std::filesystem::path headerPath = metaDir / "header_row.bin";
  {
    std::ofstream out(headerPath, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("Failed to open header output file: " + headerPath.string());
    }

    writeU64(out, ncols);

    for (const auto& h : headers) {
      if (h.size() > 255) {
        throw std::runtime_error("Column header exceeds 255 bytes after trimming: '" + h + "'");
      }
      writeU8(out, static_cast<std::uint8_t>(h.size()));
      if (!h.empty()) {
        out.write(h.data(), static_cast<std::streamsize>(h.size()));
        if (!out) throw std::runtime_error("Failed writing header bytes");
      }
    }
  }

  columnCount_ = ncols;

  // Locate row offsets and validate per-row column counts in the same pass.
  locateRowOffsets(threads);

  parseCompleted_ = true;
}

void DataTable::locateRowOffsets(int /*threads*/) {
  // CSV-aware scan of the original CSV file.
  // - Counts every byte exactly as stored.
  // - Tracks whether we're inside a quoted field.
  // - Treats newlines as row delimiters only when not in quotes.
  // - Row 0 is header at offset 0.
  // - Validates that every row has exactly columnCount_ columns by counting commas outside quotes.

  std::ifstream in(inputFilePath_, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open input CSV for row scan");

  std::vector<std::uint64_t> offsets;
  offsets.reserve(1024);

  std::uint64_t pos = 0;
  offsets.push_back(0);

  bool inQuotes = false;

  // Column validation state.
  std::uint64_t rowNumber1Based = 1; // header is row 1
  std::uint64_t commasThisRow = 0;
  bool sawAnyByteThisRow = false;

  auto validateAndResetRow = [&]() {
    // Even an empty physical line represents one empty field.
    const std::uint64_t colsThisRow = (sawAnyByteThisRow ? (commasThisRow + 1) : 1);
    if (colsThisRow != columnCount_) {
      throw std::runtime_error(
          "Row has wrong number of columns. Expected " + std::to_string(columnCount_) +
          ", got " + std::to_string(colsThisRow) +
          " at row " + std::to_string(rowNumber1Based));
    }

    ++rowNumber1Based;
    commasThisRow = 0;
    sawAnyByteThisRow = false;
  };

  while (true) {
    const int ci = in.get();
    if (ci == EOF) break;

    char c = static_cast<char>(ci);
    ++pos;
    sawAnyByteThisRow = true;

    if (c == '"') {
      if (inQuotes) {
        // Escaped quote "" inside quoted field.
        if (in.peek() == '"') {
          in.get();
          ++pos;
          // still inside quotes
        } else {
          inQuotes = false;
        }
      } else {
        inQuotes = true;
      }
      continue;
    }

    if (!inQuotes) {
      if (c == ',') {
        ++commasThisRow;
        continue;
      }

      bool newline = false;
      if (c == '\n') {
        newline = true;
      } else if (c == '\r') {
        if (in.peek() == '\n') {
          in.get();
          ++pos;
        }
        newline = true;
      }

      if (newline) {
        validateAndResetRow();
        offsets.push_back(pos); // start of next row
      }
    } else {
      // In quotes: CRLF should be consumed as two bytes for pos accuracy.
      if (c == '\r' && in.peek() == '\n') {
        in.get();
        ++pos;
      }
    }
  }

  // Handle EOF: if file ended with a newline, last offset points at EOF.
  const std::uint64_t fileSize = static_cast<std::uint64_t>(std::filesystem::file_size(inputFilePath_));
  if (!offsets.empty() && offsets.back() >= fileSize) {
    offsets.pop_back();
    // last row already validated when newline was seen
  } else if (fileSize > 0) {
    // file ended without newline: validate final row now
    validateAndResetRow();
  }

  rowCount_ = static_cast<std::uint64_t>(offsets.size());
  rowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
  for (std::uint64_t i = 0; i < rowCount_; ++i) {
    rowOffsets_[static_cast<std::size_t>(i)] = offsets[static_cast<std::size_t>(i)];
  }

  const auto path = rowOffsetsBinPath(outputDirectory_);
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) throw std::runtime_error("Failed to open row_start_offsets.bin for writing");

  out.write(reinterpret_cast<const char*>(rowOffsets_.get()),
            static_cast<std::streamsize>(rowCount_ * sizeof(std::uint64_t)));
  if (!out) throw std::runtime_error("Failed writing row_start_offsets.bin");
}

std::uint64_t DataTable::getRowCount() const {
  ensureParsed();
  return rowCount_;
}

std::uint64_t DataTable::getRowOffset(int row) const {
  ensureParsed();
  if (row < 0) throw std::out_of_range("row must be >= 0");
  if (static_cast<std::uint64_t>(row) >= rowCount_) throw std::out_of_range("row out of range");
  return rowOffsets_[static_cast<std::size_t>(row)];
}

}  // namespace DataTableLib


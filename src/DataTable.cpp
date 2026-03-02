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
#include <sstream>
#include <unordered_map>
#include <limits>

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

std::filesystem::path rowOffsetsBinPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "meta_data" / "row_start_offsets.bin";
}

std::filesystem::path mappedDataDirPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "mapped_data";
}

// Store per-chunk bit widths in meta_data/ (1 byte per chunk).
std::filesystem::path columnChunkWidthPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "meta_data" / "column_chunk_width.bin";
}

std::filesystem::path columnChunkBinPath(const std::string& outputDirectory,
                                         std::uint64_t firstCol,
                                         std::uint64_t lastCol) {
  std::ostringstream name;
  name << "column_chunk_" << firstCol << "_" << lastCol << ".bin";
  return mappedDataDirPath(outputDirectory) / name.str();
}

std::filesystem::path chunkIntStringMapPath(const std::string& outputDirectory, std::uint64_t chunkIndex) {
  std::ostringstream name;
  name << "chunk_" << chunkIndex << "_int_string_map.bin";
  return std::filesystem::path(outputDirectory) / "meta_data" / name.str();
}

std::uint32_t readBitsAt(std::ifstream& in, std::uint64_t bitOffset, std::uint8_t bitWidth) {
  const std::uint64_t byteOffset = bitOffset / 8;
  const std::uint32_t bitInByte = static_cast<std::uint32_t>(bitOffset % 8);

  const std::uint32_t totalBits = bitInByte + bitWidth;
  const std::uint32_t bytesToRead = (totalBits + 7) / 8; // <= 5 for bitWidth<=32

  std::uint8_t buf[8] = {0};
  in.clear();
  in.seekg(static_cast<std::streamoff>(byteOffset), std::ios::beg);
  if (!in) throw std::runtime_error("Failed seeking mapped_data chunk file");

  in.read(reinterpret_cast<char*>(buf), static_cast<std::streamsize>(bytesToRead));
  if (!in) throw std::runtime_error("Failed reading mapped_data chunk file");

  std::uint64_t acc = 0;
  for (std::uint32_t i = 0; i < bytesToRead; ++i) {
    acc |= (static_cast<std::uint64_t>(buf[i]) << (8U * i));
  }

  acc >>= bitInByte;

  const std::uint64_t mask = (bitWidth == 64) ? ~0ULL : ((1ULL << bitWidth) - 1ULL);
  return static_cast<std::uint32_t>(acc & mask);
}

} // namespace

namespace {

class BitWriter {
public:
  explicit BitWriter(const std::filesystem::path& path)
      : out_(path, std::ios::binary | std::ios::trunc) {
    if (!out_) throw std::runtime_error("Failed to open output file: " + path.string());
  }

  void write(std::uint32_t value, std::uint8_t bitWidth) {
    if (bitWidth == 0 || bitWidth > 32) {
      throw std::runtime_error("Invalid bitWidth");
    }

    const std::uint64_t mask = (bitWidth == 32) ? 0xFFFFFFFFULL : ((1ULL << bitWidth) - 1ULL);
    std::uint64_t v = static_cast<std::uint64_t>(value) & mask;

    for (std::uint8_t i = 0; i < bitWidth; ++i) {
      const std::uint8_t bit = static_cast<std::uint8_t>((v >> i) & 1ULL);
      currentByte_ |= static_cast<std::uint8_t>(bit << bitPos_);
      ++bitPos_;
      if (bitPos_ == 8) {
        out_.put(static_cast<char>(currentByte_));
        if (!out_) throw std::runtime_error("Failed writing output file");
        currentByte_ = 0;
        bitPos_ = 0;
      }
    }
  }

  void flush() {
    if (bitPos_ != 0) {
      out_.put(static_cast<char>(currentByte_));
      if (!out_) throw std::runtime_error("Failed writing output file");
      currentByte_ = 0;
      bitPos_ = 0;
    }
    out_.flush();
    if (!out_) throw std::runtime_error("Failed flushing output file");
  }

private:
  std::ofstream out_;
  std::uint8_t currentByte_ = 0;
  std::uint8_t bitPos_ = 0;
};

} // namespace

DataTable::DataTable(std::string inputFilePath, std::string outputDirectory)
    : inputFilePath_(std::move(inputFilePath)),
      outputDirectory_(std::move(outputDirectory)) {}

void DataTable::setInputFilePath(const std::string& inputFilePath) {
  inputFilePath_ = inputFilePath;
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();
  currentRowOffsets_.reset();
}

void DataTable::setOutputDirectory(const std::string& outputDirectory) {
  outputDirectory_ = outputDirectory;
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();
  currentRowOffsets_.reset();
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

// Parses a data row starting at the stream's current position and returns the column count.
// Stops after consuming the row delimiter (or EOF). This is used to validate column counts.
static std::uint64_t parseRowAndCountColumns(std::istream& in) {
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

void DataTable::parse(int threads) {
  parse(threads, chunkSize_);
}

void DataTable::parse(int threads, std::uint32_t chunkSize) {
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();
  currentRowOffsets_.reset();

  if (threads <= 0) {
    throw std::runtime_error("threads must be >= 1");
  }
  if (chunkSize == 0) {
    throw std::runtime_error("chunkSize must be >= 1");
  }
  chunkSize_ = chunkSize;

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

  // Initialize per-row cursors for chunk parsing.
  currentRowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
  for (std::uint64_t r = 0; r < rowCount_; ++r) {
    currentRowOffsets_[static_cast<std::size_t>(r)] = rowOffsets_[static_cast<std::size_t>(r)];
  }

  // Prepare mapped_data outputs.
  std::filesystem::create_directories(mappedDataDirPath(outputDirectory_));
  {
    // Overwrite widths file in meta_data/; parseChunks() will append 1 byte per chunk.
    std::ofstream widths(columnChunkWidthPath(outputDirectory_), std::ios::binary | std::ios::trunc);
    if (!widths) throw std::runtime_error("Failed to create meta_data/column_chunk_width.bin");
  }

  // Chunk parsing and mapped output.
  parseChunks(threads);

  parseCompleted_ = true;
}

namespace {

static std::string normalizeFieldToKey(const std::string& raw, bool wasQuoted) {
  if (wasQuoted) {
    // Quoted fields preserve whitespace.
    // Treat quoted empty and quoted whitespace-only as empty.
    bool anyNonSpace = false;
    for (unsigned char ch : raw) {
      if (std::isspace(ch) == 0) {
        anyNonSpace = true;
        break;
      }
    }
    return anyNonSpace ? raw : std::string();
  }

  // Unquoted fields are trimmed.
  std::string t = trimAsciiWhitespace(raw);
  return t;
}

struct FieldToken {
  std::string key;   // normalized key used for mapping ("" means empty)
  bool isEmpty = true;
};

// Parse a single CSV field starting at the current stream position.
// Assumes the stream is positioned at the start of a field (not on a delimiter).
// Consumes up to (but not including) the delimiter/newline/EOF.
static FieldToken readOneField(std::istream& in, char& delimOut) {
  std::string raw;
  raw.reserve(64);

  bool inQuotes = false;
  bool wasQuoted = false;
  bool afterClosingQuote = false;

  // Skip leading whitespace before a field (for unquoted trimming behavior).
  // But if the field is quoted and preceded by whitespace, that's allowed and the whitespace is ignored.
  while (true) {
    int ci = in.peek();
    if (ci == EOF) {
      delimOut = '\0';
      return FieldToken{std::string(), true};
    }
    char c = static_cast<char>(ci);
    if (c == ' ' || c == '\t') {
      in.get();
      continue;
    }
    break;
  }

  // Quoted field?
  if (in.peek() == '"') {
    in.get();
    inQuotes = true;
    wasQuoted = true;
  }

  while (true) {
    const int ci = in.get();
    if (ci == EOF) {
      delimOut = '\0';
      break;
    }

    char c = static_cast<char>(ci);

    if (inQuotes) {
      if (c == '"') {
        if (in.peek() == '"') {
          in.get();
          raw.push_back('"');
        } else {
          inQuotes = false;
          afterClosingQuote = true;
        }
        continue;
      }

      // Keep newlines as actual newlines in the raw key.
      if (c == '\r') {
        if (in.peek() == '\n') in.get();
        c = '\n';
      }
      raw.push_back(c);
      continue;
    }

    // Not in quotes.
    if (afterClosingQuote) {
      // Ignore whitespace after closing quote until delimiter/newline.
      if (std::isspace(static_cast<unsigned char>(c)) != 0) {
        continue;
      }
      afterClosingQuote = false;
    }

    if (c == ',') {
      delimOut = ',';
      break;
    }

    if (c == '\n') {
      delimOut = '\n';
      break;
    }

    if (c == '\r') {
      if (in.peek() == '\n') in.get();
      delimOut = '\n';
      break;
    }

    raw.push_back(c);
  }

  std::string key = normalizeFieldToKey(raw, wasQuoted);
  const bool isEmpty = key.empty();
  return FieldToken{std::move(key), isEmpty};
}

static std::uint8_t bitsRequired(std::uint32_t maxValue) {
  // Need at least 1 bit to represent 0.
  std::uint8_t bits = 1;
  while ((maxValue >> bits) != 0 && bits < 32) {
    ++bits;
  }
  return bits;
}

} // namespace

void DataTable::parseChunks(int /*threads*/) {
  if (columnCount_ == 0) throw std::runtime_error("Column count is 0");
  if (rowCount_ < 2) throw std::runtime_error("No data rows present");
  if (chunkSize_ == 0) throw std::runtime_error("chunkSize is 0");

  const std::uint64_t dataRowCount = rowCount_ - 1; // exclude header row

  const std::uint64_t totalChunks =
      (columnCount_ + static_cast<std::uint64_t>(chunkSize_) - 1) /
      static_cast<std::uint64_t>(chunkSize_);

  std::ofstream widthOut(columnChunkWidthPath(outputDirectory_), std::ios::binary | std::ios::trunc);
  if (!widthOut) throw std::runtime_error("Failed to open meta_data/column_chunk_width.bin");

  // We'll read from the original CSV using rowOffsets_ to avoid scanning from the beginning repeatedly.
  // Current implementation treats row 0 as header and ignores it for mapped output.

  for (std::uint64_t chunkIndex = 0; chunkIndex < totalChunks; ++chunkIndex) {
    const std::uint64_t firstCol = chunkIndex * static_cast<std::uint64_t>(chunkSize_);
    const std::uint64_t lastCol =
        std::min(firstCol + static_cast<std::uint64_t>(chunkSize_) - 1, columnCount_ - 1);
    const std::uint64_t chunkCols = lastCol - firstCol + 1;

    // Chunk-level dictionary:
    // - id 0 is reserved for empty
    // - ids are consistent across all columns in the chunk
    std::unordered_map<std::string, std::uint32_t> chunkMap;
    chunkMap.reserve(1024);
    std::vector<std::string> idToString;
    idToString.reserve(1024);
    idToString.emplace_back(std::string());

    // For writing transposed, we need per-column sequences.
    std::vector<std::vector<std::uint32_t>> colValues;
    colValues.resize(static_cast<std::size_t>(chunkCols));
    for (auto& v : colValues) v.resize(static_cast<std::size_t>(dataRowCount));

    std::uint32_t maxIdThisChunk = 0;

    for (std::uint64_t dataRow = 0; dataRow < dataRowCount; ++dataRow) {
      const std::uint64_t csvRow = dataRow + 1;
      const std::uint64_t rowStart = rowOffsets_[static_cast<std::size_t>(csvRow)];

      std::ifstream in(inputFilePath_, std::ios::binary);
      if (!in) throw std::runtime_error("Failed to open input CSV during chunk parse");
      in.seekg(static_cast<std::streamoff>(rowStart), std::ios::beg);
      if (!in) throw std::runtime_error("Failed seeking input CSV during chunk parse");

      char delim = 0;
      for (std::uint64_t c = 0; c < firstCol; ++c) {
        (void)readOneField(in, delim);
        if (delim == '\0' || delim == '\n') {
          throw std::runtime_error("Unexpected end of row while skipping columns");
        }
      }

      for (std::uint64_t localCol = 0; localCol < chunkCols; ++localCol) {
        FieldToken tok = readOneField(in, delim);

        std::uint32_t id = 0;
        if (!tok.isEmpty) {
          auto it = chunkMap.find(tok.key);
          if (it == chunkMap.end()) {
            const std::uint32_t nextId = static_cast<std::uint32_t>(idToString.size());
            it = chunkMap.emplace(tok.key, nextId).first;
            idToString.push_back(tok.key);
            if (nextId > maxIdThisChunk) maxIdThisChunk = nextId;
          }
          id = it->second;
        }

        colValues[static_cast<std::size_t>(localCol)][static_cast<std::size_t>(dataRow)] = id;

        if (localCol + 1 < chunkCols) {
          if (delim != ',') {
            throw std::runtime_error("Row ended early while reading chunk columns");
          }
        }
      }
    }

    // Write chunk-level int->string map in the requested format:
    //   uint32_t maxId
    //   uint32_t offsets[maxId + 2]  (includes terminal offset)
    //   blob bytes of all strings in id order
    {
      const auto path = chunkIntStringMapPath(outputDirectory_, chunkIndex);
      std::ofstream out(path, std::ios::binary | std::ios::trunc);
      if (!out) throw std::runtime_error("Failed to open chunk map file: " + path.string());

      const std::uint32_t maxId = static_cast<std::uint32_t>(idToString.size() - 1);
      out.write(reinterpret_cast<const char*>(&maxId), sizeof(maxId));
      if (!out) throw std::runtime_error("Failed writing maxId");

      const std::size_t offsetsCount = static_cast<std::size_t>(maxId) + 2;
      std::vector<std::uint32_t> offsets(offsetsCount, 0u);

      std::uint64_t cursor = 0;
      for (std::uint32_t id = 0; id <= maxId; ++id) {
        offsets[static_cast<std::size_t>(id)] = static_cast<std::uint32_t>(cursor);
        cursor += static_cast<std::uint64_t>(idToString[static_cast<std::size_t>(id)].size());
        if (cursor > std::numeric_limits<std::uint32_t>::max()) {
          throw std::runtime_error("chunk int->string blob exceeds 4GB");
        }
      }
      offsets[static_cast<std::size_t>(maxId) + 1] = static_cast<std::uint32_t>(cursor);

      out.write(reinterpret_cast<const char*>(offsets.data()),
                static_cast<std::streamsize>(offsets.size() * sizeof(std::uint32_t)));
      if (!out) throw std::runtime_error("Failed writing offsets");

      for (std::uint32_t id = 0; id <= maxId; ++id) {
        const auto& s = idToString[static_cast<std::size_t>(id)];
        if (!s.empty()) {
          out.write(s.data(), static_cast<std::streamsize>(s.size()));
          if (!out) throw std::runtime_error("Failed writing string bytes");
        }
      }
    }

    const std::uint8_t bitWidth = bitsRequired(maxIdThisChunk);
    widthOut.write(reinterpret_cast<const char*>(&bitWidth), 1);
    if (!widthOut) throw std::runtime_error("Failed writing chunk bit width");

    BitWriter bw(columnChunkBinPath(outputDirectory_, firstCol, lastCol));

    for (std::uint64_t localCol = 0; localCol < chunkCols; ++localCol) {
      const auto& vals = colValues[static_cast<std::size_t>(localCol)];
      for (std::uint64_t dataRow = 0; dataRow < dataRowCount; ++dataRow) {
        bw.write(vals[static_cast<std::size_t>(dataRow)], bitWidth);
      }
    }

    bw.flush();
  }

  widthOut.flush();
  if (!widthOut) throw std::runtime_error("Failed flushing meta_data/column_chunk_width.bin");
}

std::uint32_t DataTable::lookupMap(std::uint64_t row, std::uint64_t col) const {
  ensureParsed();

  if (row >= rowCount_) throw std::out_of_range("row out of range");
  if (col >= columnCount_) throw std::out_of_range("col out of range");
  if (row == 0) throw std::runtime_error("lookupMap() is not supported for header row");

  const std::uint64_t dataRowIndex = row - 1;
  const std::uint64_t dataRowCount = rowCount_ - 1;

  const std::uint64_t chunkIndex = col / static_cast<std::uint64_t>(chunkSize_);
  const std::uint64_t firstCol = chunkIndex * static_cast<std::uint64_t>(chunkSize_);
  const std::uint64_t lastCol =
      std::min(firstCol + static_cast<std::uint64_t>(chunkSize_) - 1, columnCount_ - 1);
  const std::uint64_t localCol = col - firstCol;

  std::ifstream widthIn(columnChunkWidthPath(outputDirectory_), std::ios::binary);
  if (!widthIn) throw std::runtime_error("Failed to open meta_data/column_chunk_width.bin");

  widthIn.seekg(static_cast<std::streamoff>(chunkIndex), std::ios::beg);
  if (!widthIn) throw std::runtime_error("Failed seeking mapped_data/column_chunk_width.bin");

  std::uint8_t bitWidth = 0;
  widthIn.read(reinterpret_cast<char*>(&bitWidth), 1);
  if (!widthIn) throw std::runtime_error("Failed reading chunk bit width");
  if (bitWidth == 0 || bitWidth > 32) throw std::runtime_error("Invalid chunk bit width");

  // Transposed layout: for each column in the chunk, store all data rows sequentially.
  // idx = localCol * dataRowCount + dataRowIndex
  const std::uint64_t idx = localCol * dataRowCount + dataRowIndex;
  const std::uint64_t bitOffset = idx * static_cast<std::uint64_t>(bitWidth);

  std::ifstream in(columnChunkBinPath(outputDirectory_, firstCol, lastCol), std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open mapped_data chunk file");

  return readBitsAt(in, bitOffset, bitWidth);
}

std::string DataTable::getValue(std::uint64_t row, std::uint64_t col) const {
  ensureParsed();
  if (row >= rowCount_) throw std::out_of_range("row out of range");
  if (col >= columnCount_) throw std::out_of_range("col out of range");
  if (row == 0) throw std::runtime_error("getValue() is not supported for header row");

  const std::uint32_t id = lookupMap(row, col);

  const std::uint64_t chunkIndex = col / static_cast<std::uint64_t>(chunkSize_);
  const auto path = chunkIntStringMapPath(outputDirectory_, chunkIndex);

  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open chunk map file: " + path.string());

  std::uint32_t maxId = 0;
  in.read(reinterpret_cast<char*>(&maxId), sizeof(maxId));
  if (!in) throw std::runtime_error("Failed reading maxId");
  if (id > maxId) throw std::runtime_error("id out of range for this chunk");

  const std::uint64_t offsetsCount = static_cast<std::uint64_t>(maxId) + 2;
  const std::uint64_t offsetsBase = sizeof(std::uint32_t);

  std::uint32_t start = 0;
  std::uint32_t end = 0;

  in.seekg(static_cast<std::streamoff>(offsetsBase + static_cast<std::uint64_t>(id) * sizeof(std::uint32_t)),
           std::ios::beg);
  in.read(reinterpret_cast<char*>(&start), sizeof(start));
  in.seekg(static_cast<std::streamoff>(offsetsBase + static_cast<std::uint64_t>(id + 1) * sizeof(std::uint32_t)),
           std::ios::beg);
  in.read(reinterpret_cast<char*>(&end), sizeof(end));
  if (!in) throw std::runtime_error("Failed reading offsets");
  if (end < start) throw std::runtime_error("corrupt offsets");

  const std::uint32_t len = end - start;
  const std::uint64_t blobBase = sizeof(std::uint32_t) + offsetsCount * sizeof(std::uint32_t);

  in.seekg(static_cast<std::streamoff>(blobBase + start), std::ios::beg);
  if (!in) throw std::runtime_error("Failed seeking blob");

  std::string s;
  s.resize(len);
  if (len > 0) {
    in.read(s.data(), static_cast<std::streamsize>(len));
    if (!in) throw std::runtime_error("Failed reading string");
  }

  return s;
}

}  // namespace DataTableLib


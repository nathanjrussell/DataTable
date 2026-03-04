#include <DataTable/DataTable.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <limits>
#include <array>
#include <iomanip>

#include <nlohmann/json.hpp>

// Progress bars (single instance for this library; ProgressBars is internally synchronized)
#include <progressbar/progress_bars.hpp>

#include <HashLibrary/sha256.hpp>

namespace DataTableLib {

namespace {

// Strict UTF-8 validation (RFC 3629).
// Accepts ASCII and well-formed multi-byte sequences.
// Rejects overlong encodings, surrogate halves, and code points > U+10FFFF.
static bool isValidUtf8(const std::string& s) {
  const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
  std::size_t i = 0;
  const std::size_t n = s.size();

  while (i < n) {
    const unsigned char c0 = p[i];
    if (c0 <= 0x7F) {
      ++i;
      continue;
    }

    // 2-byte sequence
    if (c0 >= 0xC2 && c0 <= 0xDF) {
      if (i + 1 >= n) return false;
      const unsigned char c1 = p[i + 1];
      if ((c1 & 0xC0) != 0x80) return false;
      i += 2;
      continue;
    }

    // 3-byte sequences
    if (c0 == 0xE0) {
      if (i + 2 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      if (c1 < 0xA0 || c1 > 0xBF) return false; // no overlong
      if ((c2 & 0xC0) != 0x80) return false;
      i += 3;
      continue;
    }
    if (c0 >= 0xE1 && c0 <= 0xEC) {
      if (i + 2 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      if ((c1 & 0xC0) != 0x80) return false;
      if ((c2 & 0xC0) != 0x80) return false;
      i += 3;
      continue;
    }
    if (c0 == 0xED) {
      // U+D800..U+DFFF surrogates are invalid in UTF-8
      if (i + 2 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      if (c1 < 0x80 || c1 > 0x9F) return false;
      if ((c2 & 0xC0) != 0x80) return false;
      i += 3;
      continue;
    }
    if (c0 >= 0xEE && c0 <= 0xEF) {
      if (i + 2 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      if ((c1 & 0xC0) != 0x80) return false;
      if ((c2 & 0xC0) != 0x80) return false;
      i += 3;
      continue;
    }

    // 4-byte sequences
    if (c0 == 0xF0) {
      if (i + 3 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      const unsigned char c3 = p[i + 3];
      if (c1 < 0x90 || c1 > 0xBF) return false; // no overlong
      if ((c2 & 0xC0) != 0x80) return false;
      if ((c3 & 0xC0) != 0x80) return false;
      i += 4;
      continue;
    }
    if (c0 >= 0xF1 && c0 <= 0xF3) {
      if (i + 3 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      const unsigned char c3 = p[i + 3];
      if ((c1 & 0xC0) != 0x80) return false;
      if ((c2 & 0xC0) != 0x80) return false;
      if ((c3 & 0xC0) != 0x80) return false;
      i += 4;
      continue;
    }
    if (c0 == 0xF4) {
      if (i + 3 >= n) return false;
      const unsigned char c1 = p[i + 1];
      const unsigned char c2 = p[i + 2];
      const unsigned char c3 = p[i + 3];
      if (c1 < 0x80 || c1 > 0x8F) return false; // <= U+10FFFF
      if ((c2 & 0xC0) != 0x80) return false;
      if ((c3 & 0xC0) != 0x80) return false;
      i += 4;
      continue;
    }

    return false;
  }

  return true;
}

static void validateHeadersUniqueUtf8OrThrow(const std::vector<std::string>& headers) {
  std::unordered_map<std::string, std::size_t> seen;
  seen.reserve(headers.size());

  for (std::size_t i = 0; i < headers.size(); ++i) {
    const std::string& h = headers[i];
    if (!isValidUtf8(h)) {
      throw std::runtime_error("Invalid UTF-8 in column header at index " + std::to_string(i));
    }

    auto it = seen.find(h);
    if (it != seen.end()) {
      throw std::runtime_error("Duplicate column header name '" + h + "' at index " + std::to_string(i) +
                               " (already seen at index " + std::to_string(it->second) + ")");
    }

    seen.emplace(h, i);
  }
}

// One instance for the whole library (this translation unit).
std::unique_ptr<progressbar::ProgressBars> g_progressBars;

progressbar::ProgressBars& progressBars() {
  if (!g_progressBars) {
    progressbar::ProgressBars::Options options;
    options.enabled = true;
    // Don’t render progress bars unless we’re on an interactive terminal.
    options.onlyRenderOnTty = true;
    options.minRedrawInterval = std::chrono::milliseconds(40);
    options.barWidth = 28;
    options.removeCompletedAfter = std::chrono::milliseconds(2000);
    g_progressBars = std::make_unique<progressbar::ProgressBars>(options);
  }
  return *g_progressBars;
}

int progressCreate(std::uint64_t total,
                   const std::string& label,
                   progressbar::ProgressBars::Color color = progressbar::ProgressBars::Color::Cyan) {
  return progressBars().createProgressBar(total, label, color);
}

void progressTick(int id) {
  progressBars().updateProgressBar(id);
}

void progressSet(int id, std::uint64_t value) {
  progressBars().updateProgressBar(id, value);
}

void progressComplete(int id) {
  progressBars().markProgressBarComplete(id);
}

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

std::filesystem::path countsBinPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "meta_data" / "counts.bin";
}

std::filesystem::path hashTxtPath(const std::string& outputDirectory) {
  return std::filesystem::path(outputDirectory) / "meta_data" / "hash.txt";
}

void writeCountsBin(const std::string& outputDirectory, std::uint64_t rows, std::uint64_t cols) {
  const auto path = countsBinPath(outputDirectory);
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) throw std::runtime_error("Failed to open counts.bin for writing: " + path.string());
  writeU64(out, rows);
  writeU64(out, cols);
}

bool tryReadCountsBin(const std::string& outputDirectory, std::uint64_t& rowsOut, std::uint64_t& colsOut) {
  const auto path = countsBinPath(outputDirectory);
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  in.read(reinterpret_cast<char*>(&rowsOut), sizeof(rowsOut));
  in.read(reinterpret_cast<char*>(&colsOut), sizeof(colsOut));
  if (!in) return false;
  return true;
}

std::string readTextFileTrimmed(const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in) throw std::runtime_error("Failed to open file: " + path.string());
  std::ostringstream ss;
  ss << in.rdbuf();
  return trimAsciiWhitespace(ss.str());
}

void writeTextFile(const std::filesystem::path& path, const std::string& text) {
  std::ofstream out(path, std::ios::trunc);
  if (!out) throw std::runtime_error("Failed to open file for writing: " + path.string());
  out << text;
  if (!out) throw std::runtime_error("Failed writing file: " + path.string());
}

// Compute SHA-256 of a file and return the lowercase hex digest.
static std::string sha256FileHex(const std::string& path) {
  // HashLibrary reads the file incrementally; keep memory bounded even for huge inputs.
  constexpr std::size_t kChunkSizeBytes = 8u * 1024u * 1024u; // 8 MiB
  HashLibrary::Sha256 sha(path, kChunkSizeBytes);
  return sha.hexdigest();
}

// Infer the last-used chunkSize from the width metadata file.
// If the last chunk is smaller (because columnCount isn't a multiple), we still recover the original chunkSize.
static std::uint32_t inferChunkSizeFromWidths(const std::string& outputDirectory, std::uint64_t columnCount) {
  const auto path = columnChunkWidthPath(outputDirectory);
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("Failed to open meta_data/column_chunk_width.bin for inference");
  }

  in.seekg(0, std::ios::end);
  const auto sz = static_cast<std::uint64_t>(in.tellg());
  if (sz == 0) {
    throw std::runtime_error("column_chunk_width.bin is empty");
  }

  const std::uint64_t chunkCount = sz; // 1 byte per chunk
  const std::uint64_t inferred = (columnCount + chunkCount - 1) / chunkCount;
  if (inferred == 0 || inferred > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("Failed to infer chunkSize from metadata");
  }
  return static_cast<std::uint32_t>(inferred);
}

// Read an unsigned value of bitWidth bits at a given bitOffset from a binary stream.
// Bits are encoded LSB-first (matching BitWriter::write()).
static std::uint32_t readBitsAt(std::istream& in, std::uint64_t bitOffset, std::uint8_t bitWidth) {
  if (bitWidth == 0 || bitWidth > 32) throw std::runtime_error("Invalid bitWidth");

  const std::uint64_t byteOffset = bitOffset / 8ULL;
  const std::uint8_t startBit = static_cast<std::uint8_t>(bitOffset % 8ULL);

  const std::uint64_t needBits = static_cast<std::uint64_t>(startBit) + bitWidth;
  const std::uint64_t needBytes = (needBits + 7ULL) / 8ULL;

  std::array<std::uint8_t, 8> bytes{}; // bitWidth<=32 => needBytes<=5
  if (needBytes > bytes.size()) throw std::runtime_error("Internal error: needBytes too large");

  in.clear();
  in.seekg(static_cast<std::streamoff>(byteOffset), std::ios::beg);
  if (!in) throw std::runtime_error("Failed seeking mapped_data chunk file");

  in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(needBytes));
  if (in.gcount() != static_cast<std::streamsize>(needBytes)) {
    throw std::runtime_error("Failed reading mapped_data chunk file");
  }

  std::uint64_t accum = 0;
  for (std::uint64_t i = 0; i < needBytes; ++i) {
    accum |= (static_cast<std::uint64_t>(bytes[static_cast<std::size_t>(i)]) << (8ULL * i));
  }

  accum >>= startBit;
  const std::uint64_t mask = (bitWidth == 32) ? 0xFFFFFFFFULL : ((1ULL << bitWidth) - 1ULL);
  return static_cast<std::uint32_t>(accum & mask);
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

std::uint64_t DataTable::getColumnIndex(const std::string& header) const {
  ensureParsed();

  std::ifstream in(headerRowBinPath(outputDirectory_), std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open header_row.bin");

  const std::uint64_t ncols = readU64(in);

  for (std::uint64_t i = 0; i < ncols; ++i) {
    const std::string s = readLenPrefixedString(in);
    if (s == header) return i;
  }

  throw std::out_of_range("Column header not found");
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
      // Ignore whitespace after closing quote until delimiter/newline.
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

void DataTable::locateRowOffsets(int threads) {
  // Parallel, CSV-aware scan of the original CSV file.
  // Preserves the single-thread semantics exactly:
  // - Counts every byte exactly as stored.
  // - Tracks whether we're inside a quoted field.
  // - Treats newlines as row delimiters only when not in quotes.
  // - Row 0 is header at offset 0.
  // - Validates that every row has exactly columnCount_ columns by counting commas outside quotes.

  if (threads <= 0) {
    throw std::runtime_error("threads must be >= 1");
  }

  const std::uint64_t fileSize =
      static_cast<std::uint64_t>(std::filesystem::file_size(inputFilePath_));

  // Offsets always include the header row start.
  std::vector<std::uint64_t> allOffsets;
  allOffsets.reserve(1024);
  allOffsets.push_back(0);

  if (fileSize == 0) {
    // Empty file: treat as single empty row (header).
    rowCount_ = static_cast<std::uint64_t>(allOffsets.size());
    rowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
    rowOffsets_[0] = 0;

    const auto path = rowOffsetsBinPath(outputDirectory_);
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to open row_start_offsets.bin for writing");
    out.write(reinterpret_cast<const char*>(rowOffsets_.get()),
              static_cast<std::streamsize>(rowCount_ * sizeof(std::uint64_t)));
    if (!out) throw std::runtime_error("Failed writing row_start_offsets.bin");
    return;
  }

  // Choose a chunk count: avoid too many tiny chunks.
  const std::uint64_t hw = std::max<std::uint64_t>(1, static_cast<std::uint64_t>(threads));
  const std::uint64_t desiredChunks = hw * 4ULL;
  const std::uint64_t minChunkBytes = 8ULL * 1024ULL * 1024ULL; // 8 MiB
  const std::uint64_t chunkBytes =
      std::max(minChunkBytes, (fileSize + desiredChunks - 1) / desiredChunks);
  const std::uint64_t chunkCount = (fileSize + chunkBytes - 1) / chunkBytes;

  // Progress: pass-1 is parallel and naturally chunk-based.
  const int pbLocateRowOffsetsPass1 = progressCreate(
      chunkCount,
      "Row offsets (pass 1)",
      progressbar::ProgressBars::Color::Cyan);

  // Progress: pass-2 is sequential; update at chunk boundaries (not per byte).
  const int pbLocateRowOffsetsPass2 = progressCreate(
      chunkCount,
      "Row offsets (pass 2)",
      progressbar::ProgressBars::Color::Yellow);

  struct ChunkPass1 {
    bool parityAssumingStartOut = false; // quote parity over this chunk with RFC4180 "" handling
    bool pendingQuoteInQuotesAtEndAssumingStartOut = false;
  };

  auto scanChunkParity = [&](std::uint64_t chunkIndex) -> ChunkPass1 {
    const std::uint64_t begin = chunkIndex * chunkBytes;
    const std::uint64_t end = std::min(begin + chunkBytes, fileSize);

    ChunkPass1 r;

    std::ifstream in(inputFilePath_, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open input CSV for row scan");
    in.seekg(static_cast<std::streamoff>(begin), std::ios::beg);
    if (!in) throw std::runtime_error("Failed seeking input CSV for row scan");

    constexpr std::size_t BUF_SIZE = 1u << 20; // 1 MiB
    std::vector<char> buf(BUF_SIZE);

    bool inQuotes = false;
    bool pendingQuote = false;

    std::uint64_t pos = begin;

    auto resolvePending = [&](char c) {
      if (!pendingQuote) return false;
      pendingQuote = false;
      if (inQuotes && c == '"') {
        // "" escape across buffer boundary
        return true;
      }
      // pending quote was a closing quote; toggle already applied at boundary (we deferred it)
      return false;
    };

    while (pos < end) {
      const std::uint64_t remaining = end - pos;
      const std::size_t toRead = static_cast<std::size_t>(
          std::min<std::uint64_t>(remaining, static_cast<std::uint64_t>(buf.size())));

      in.read(buf.data(), static_cast<std::streamsize>(toRead));
      const std::streamsize got = in.gcount();
      if (got <= 0) break;

      for (std::streamsize i = 0; i < got; ++i, ++pos) {
        const char c = buf[static_cast<std::size_t>(i)];

        if (resolvePending(c)) {
          continue;
        }

        if (c != '"') {
          continue;
        }

        if (inQuotes) {
          if (pos + 1 >= end) {
            // May be first of "" spanning boundary; defer decision.
            pendingQuote = true;
            continue;
          }

          const int pi = in.peek();
          if (pi != EOF && static_cast<char>(pi) == '"') {
            // Escaped quote: consume next quote.
            in.get();
            ++pos;
            continue;
          }

          // Closing quote.
          inQuotes = false;
          r.parityAssumingStartOut = !r.parityAssumingStartOut;
        } else {
          // Opening quote.
          inQuotes = true;
          r.parityAssumingStartOut = !r.parityAssumingStartOut;
        }
      }
    }

    r.pendingQuoteInQuotesAtEndAssumingStartOut = pendingQuote && inQuotes;
    return r;
  };

  // Pass 1: scan parity per chunk in parallel.
  std::vector<ChunkPass1> chunks(static_cast<std::size_t>(chunkCount));
  std::atomic<std::uint64_t> next{0};
  std::exception_ptr eptr;
  std::mutex eptrMu;

  auto worker = [&]() {
    try {
      while (true) {
        const std::uint64_t idx = next.fetch_add(1);
        if (idx >= chunkCount) break;
        chunks[static_cast<std::size_t>(idx)] = scanChunkParity(idx);
        // Chunk-level progress update (once per completed chunk).
        progressTick(pbLocateRowOffsetsPass1);
      }
    } catch (...) {
      std::lock_guard<std::mutex> g(eptrMu);
      if (!eptr) eptr = std::current_exception();
    }
  };

  std::vector<std::thread> pool;
  pool.reserve(static_cast<std::size_t>(threads));
  for (int t = 0; t < threads; ++t) pool.emplace_back(worker);
  for (auto& th : pool) th.join();
  if (eptr) std::rethrow_exception(eptr);

  progressComplete(pbLocateRowOffsetsPass1);

  // Fix up pending "" across chunk boundary: if chunk i ended with pending quote inside quotes
  // and the next chunk begins with a quote, then that next quote should not toggle parity.
  for (std::uint64_t i = 0; i + 1 < chunkCount; ++i) {
    if (!chunks[static_cast<std::size_t>(i)].pendingQuoteInQuotesAtEndAssumingStartOut) continue;

    const std::uint64_t beginNext = (i + 1) * chunkBytes;
    if (beginNext >= fileSize) continue;

    std::ifstream in(inputFilePath_, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open input CSV for boundary quote check");
    in.seekg(static_cast<std::streamoff>(beginNext), std::ios::beg);
    if (!in) throw std::runtime_error("Failed seeking input CSV for boundary quote check");
    const int ci = in.get();
    if (ci != EOF && static_cast<char>(ci) == '"') {
      chunks[static_cast<std::size_t>(i + 1)].parityAssumingStartOut =
          !chunks[static_cast<std::size_t>(i + 1)].parityAssumingStartOut;
    }
  }

  // Compute actual start-in-quotes for each chunk by prefix XOR over chunk parities.
  std::vector<bool> startInQuotes(static_cast<std::size_t>(chunkCount), false);
  for (std::uint64_t i = 1; i < chunkCount; ++i) {
    startInQuotes[static_cast<std::size_t>(i)] =
        startInQuotes[static_cast<std::size_t>(i - 1)] ^
        chunks[static_cast<std::size_t>(i - 1)].parityAssumingStartOut;
  }

  // Pass 2: scan for row delimiters and validate column counts.
  // NOTE: We must preserve the original single-pass semantics, which are purely sequential.
  // We therefore scan the file from byte 0..fileSize once, but we jump the inQuotes state at
  // chunk boundaries using startInQuotes[].

  // Initialize pass-2 progress at 0 completed chunks.
  progressSet(pbLocateRowOffsetsPass2, 0);

  std::uint64_t rowNumber1Based = 1; // header is row 1
  std::uint64_t commasThisRow = 0;
  bool sawAnyByteThisRow = false;

  auto validateAndResetRow = [&]() {
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

  bool inQuotes = false;

  constexpr std::size_t BUF_SIZE = 1u << 20; // 1 MiB
  std::vector<char> buf(BUF_SIZE);

  // Single sequential scan with chunk-boundary state correction.
  std::ifstream in(inputFilePath_, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open input CSV for row scan");

  std::uint64_t pos = 0;
  std::uint64_t currentChunk = 0;
  std::uint64_t nextBoundary = std::min(chunkBytes, fileSize);

  // Carry byte for buffer-boundary lookahead.
  bool hasCarry = false;
  char carry = '\0';

  // Track how many chunk boundaries we've crossed so far.
  // Completed chunks in pass-2 is always (currentChunk) while within that chunk,
  // and (currentChunk+1) right after we advance to the next chunk.
  std::uint64_t lastReportedCompletedChunks = 0;

  auto getNextByteFromStream = [&]() -> std::pair<bool, char> {
    char ch;
    in.read(&ch, 1);
    if (in.gcount() != 1) return {false, '\0'};
    return {true, ch};
  };

  while (pos < fileSize) {
    // Ensure correct inQuotes at chunk boundaries.
    while (pos >= nextBoundary && currentChunk + 1 < chunkCount) {
      ++currentChunk;
      inQuotes = startInQuotes[static_cast<std::size_t>(currentChunk)];
      nextBoundary = std::min((currentChunk + 1) * chunkBytes, fileSize);

      const std::uint64_t completed = currentChunk;
      if (completed != lastReportedCompletedChunks) {
        progressSet(pbLocateRowOffsetsPass2, completed);
        lastReportedCompletedChunks = completed;
      }
    }

    const std::uint64_t remaining = fileSize - pos;
    const std::size_t toRead = static_cast<std::size_t>(
        std::min<std::uint64_t>(remaining, static_cast<std::uint64_t>(buf.size())));

    in.read(buf.data(), static_cast<std::streamsize>(toRead));
    const std::streamsize got = in.gcount();
    if (got <= 0) break;

    std::streamsize i = 0;

    // If we have a carry byte from the end of the previous buffer, process it first.
    if (hasCarry) {
      // We'll process carry as if it was at buf[-1]; its position is current pos.
      // To do that, we treat carry as the first byte and then continue with buf[0].
      const char c = carry;
      hasCarry = false;

      // Boundary correction at exact boundary.
      if (pos == nextBoundary && currentChunk + 1 < chunkCount) {
        ++currentChunk;
        inQuotes = startInQuotes[static_cast<std::size_t>(currentChunk)];
        nextBoundary = std::min((currentChunk + 1) * chunkBytes, fileSize);
      }

      sawAnyByteThisRow = true;

      // For carry, we can safely lookahead into buf[0] if present, else stream.
      auto lookahead = [&]() -> std::pair<bool, char> {
        if (got > 0) return {true, buf[0]};
        return getNextByteFromStream();
      };

      const auto next = lookahead();

      if (c == '"') {
        if (inQuotes && next.first && next.second == '"') {
          // escaped quote: consume the next quote
          if (got > 0) {
            // consume buf[0]
            ++i;
            ++pos;
          } else {
            (void)getNextByteFromStream();
            ++pos;
          }
        } else {
          inQuotes = !inQuotes;
        }
      } else if (!inQuotes) {
        if (c == ',') {
          ++commasThisRow;
        } else if (c == '\n') {
          validateAndResetRow();
          allOffsets.push_back(pos + 1);
        } else if (c == '\r') {
          // CRLF?
          if (next.first && next.second == '\n') {
            if (got > 0) {
              ++i;
              ++pos;
            } else {
              (void)getNextByteFromStream();
              ++pos;
            }
          }
          validateAndResetRow();
          allOffsets.push_back(pos + 1);
        }
      } else {
        // in quotes
        if (c == '\r' && next.first && next.second == '\n') {
          if (got > 0) {
            ++i;
            ++pos;
          } else {
            (void)getNextByteFromStream();
            ++pos;
          }
        }
      }

      ++pos;
    }

    for (; i < got; ++i, ++pos) {
      // Boundary correction may be needed inside a buffer.
      if (pos == nextBoundary && currentChunk + 1 < chunkCount) {
        ++currentChunk;
        inQuotes = startInQuotes[static_cast<std::size_t>(currentChunk)];
        nextBoundary = std::min((currentChunk + 1) * chunkBytes, fileSize);

        const std::uint64_t completed = currentChunk;
        if (completed != lastReportedCompletedChunks) {
          progressSet(pbLocateRowOffsetsPass2, completed);
          lastReportedCompletedChunks = completed;
        }
      }

      const char c = buf[static_cast<std::size_t>(i)];
      sawAnyByteThisRow = true;

      // Lookahead is either next byte in the buffer, or (if at end of buffer) read-ahead via stream.
      bool nextOk = false;
      char nextChar = '\0';
      if (i + 1 < got) {
        nextOk = true;
        nextChar = buf[static_cast<std::size_t>(i + 1)];
      } else if (pos + 1 < fileSize) {
        auto nxt = getNextByteFromStream();
        if (nxt.first) {
          nextOk = true;
          nextChar = nxt.second;
          // stash for next outer iteration
          hasCarry = true;
          carry = nxt.second;
        }
      }

      if (c == '"') {
        if (inQuotes && nextOk && nextChar == '"') {
          // escaped quote: consume next quote if it is actually the next in buffer.
          if (i + 1 < got) {
            ++i;
            ++pos;
          } else {
            // next quote is in carry; it will be skipped by clearing it.
            hasCarry = false;
          }
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }

      if (!inQuotes) {
        if (c == ',') {
          ++commasThisRow;
          continue;
        }

        if (c == '\n') {
          validateAndResetRow();
          allOffsets.push_back(pos + 1);
          continue;
        }

        if (c == '\r') {
          if (nextOk && nextChar == '\n') {
            if (i + 1 < got) {
              ++i;
              ++pos;
            } else {
              hasCarry = false;
            }
          }
          validateAndResetRow();
          allOffsets.push_back(pos + 1);
          continue;
        }
      } else {
        // In quotes: CRLF should be consumed as two bytes for pos accuracy.
        if (c == '\r' && nextOk && nextChar == '\n') {
          if (i + 1 < got) {
            ++i;
            ++pos;
          } else {
            hasCarry = false;
          }
        }
      }
    }

    // If we stashed a carry byte by reading ahead from the stream, back up the stream by 1 byte.
    // We can't unread from ifstream portably, so instead we keep carry and adjust logical pos.
    // The loop above advances pos accordingly; carry will be processed on the next iteration.
  }

  // Ensure pass-2 progress is complete.
  progressSet(pbLocateRowOffsetsPass2, chunkCount);
  progressComplete(pbLocateRowOffsetsPass2);

  // Handle EOF: if file ended with a newline, last offset points at EOF.
  if (!allOffsets.empty() && allOffsets.back() >= fileSize) {
    allOffsets.pop_back();
    // last row already validated when newline was seen
  } else if (fileSize > 0) {
    // file ended without newline: validate final row now
    validateAndResetRow();
  }

  std::sort(allOffsets.begin(), allOffsets.end());
  allOffsets.erase(std::unique(allOffsets.begin(), allOffsets.end()), allOffsets.end());

  rowCount_ = static_cast<std::uint64_t>(allOffsets.size());
  rowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
  for (std::uint64_t i = 0; i < rowCount_; ++i) {
    rowOffsets_[static_cast<std::size_t>(i)] = allOffsets[static_cast<std::size_t>(i)];
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

  // Ensure meta_data directory exists early (we may read hash/counts for a fast-path).
  const std::filesystem::path metaDir = std::filesystem::path(outputDirectory_) / "meta_data";
  std::filesystem::create_directories(metaDir);

  // 1) Always compute SHA256 of input (requirement).
  const std::string computedHash = sha256FileHex(inputFilePath_);

  // 2) If meta_data/hash.txt exists and matches, skip parsing and just load counts/offsets.
  {
    const auto hashPath = hashTxtPath(outputDirectory_);
    if (std::filesystem::exists(hashPath)) {
      const std::string existingHash = readTextFileTrimmed(hashPath);
      if (!existingHash.empty() && existingHash == computedHash) {
        // Load counts first (preferred). Fallback to header_row.bin + row_start_offsets.bin.
        std::uint64_t rows = 0;
        std::uint64_t cols = 0;
        if (tryReadCountsBin(outputDirectory_, rows, cols)) {
          rowCount_ = rows;
          columnCount_ = cols;
        } else {
          // Column count from header_row.bin
          {
            std::ifstream hin(headerRowBinPath(outputDirectory_), std::ios::binary);
            if (!hin) throw std::runtime_error("hash matched but header_row.bin missing");
            columnCount_ = readU64(hin);
          }
          // Row count from row_start_offsets.bin size
          {
            const auto roPath = rowOffsetsBinPath(outputDirectory_);
            const std::uint64_t sz = static_cast<std::uint64_t>(std::filesystem::file_size(roPath));
            if (sz % sizeof(std::uint64_t) != 0) throw std::runtime_error("Corrupt row_start_offsets.bin");
            rowCount_ = sz / sizeof(std::uint64_t);
          }
        }

        // Load row offsets into memory for getters and getValue/lookupMap.
        {
          const auto roPath = rowOffsetsBinPath(outputDirectory_);
          std::ifstream rin(roPath, std::ios::binary);
          if (!rin) throw std::runtime_error("hash matched but row_start_offsets.bin missing");
          rowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
          rin.read(reinterpret_cast<char*>(rowOffsets_.get()),
                   static_cast<std::streamsize>(rowCount_ * sizeof(std::uint64_t)));
          if (!rin) throw std::runtime_error("Failed reading row_start_offsets.bin");
        }

        // Also set up currentRowOffsets_ so parseChunks-related methods that might rely on it later are safe.
        currentRowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
        for (std::uint64_t r = 0; r < rowCount_; ++r) {
          currentRowOffsets_[static_cast<std::size_t>(r)] = rowOffsets_[static_cast<std::size_t>(r)];
        }

        parseCompleted_ = true;
        return;
      }
    }
  }

  // Normal parse path (hash mismatch or no prior hash).
  std::ifstream in(inputFilePath_, std::ios::binary);
  if (!in) {
    throw std::runtime_error("Failed to open input CSV: " + inputFilePath_);
  }

  const std::vector<std::string> headers = parseHeaderRow(in);
  validateHeadersUniqueUtf8OrThrow(headers);
  const std::uint64_t ncols = static_cast<std::uint64_t>(headers.size());

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

  // As soon as both counts are known, write counts.bin.
  writeCountsBin(outputDirectory_, rowCount_, columnCount_);

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

  // Write/update hash.txt last, after a successful parse.
  writeTextFile(hashTxtPath(outputDirectory_), computedHash + "\n");

  parseCompleted_ = true;
}

void DataTable::load(const std::string& directory) {
  // Reset state.
  inputFilePath_.clear();
  outputDirectory_ = directory;
  parseCompleted_ = false;
  columnCount_ = 0;
  rowCount_ = 0;
  rowOffsets_.reset();
  currentRowOffsets_.reset();

  if (outputDirectory_.empty()) {
    throw std::runtime_error("Output directory is empty");
  }

  const std::filesystem::path outDir(outputDirectory_);
  const std::filesystem::path metaDir = outDir / "meta_data";
  const std::filesystem::path mappedDir = outDir / "mapped_data";

  if (!std::filesystem::exists(metaDir)) {
    throw std::runtime_error("Missing meta_data directory: " + metaDir.string());
  }
  if (!std::filesystem::exists(mappedDir)) {
    throw std::runtime_error("Missing mapped_data directory: " + mappedDir.string());
  }

  // 1) Read counts.bin (rows, cols).
  {
    std::uint64_t rows = 0;
    std::uint64_t cols = 0;
    if (!tryReadCountsBin(outputDirectory_, rows, cols)) {
      throw std::runtime_error("Failed to read meta_data/counts.bin");
    }
    if (rows == 0) throw std::runtime_error("Invalid counts.bin: rowCount is 0");
    if (cols == 0) throw std::runtime_error("Invalid counts.bin: columnCount is 0");
    rowCount_ = rows;
    columnCount_ = cols;
  }

  // 2) Load row offsets into memory.
  {
    const auto roPath = rowOffsetsBinPath(outputDirectory_);
    if (!std::filesystem::exists(roPath)) {
      throw std::runtime_error("Missing meta_data/row_start_offsets.bin");
    }

    const std::uint64_t sz = static_cast<std::uint64_t>(std::filesystem::file_size(roPath));
    const std::uint64_t expected = rowCount_ * static_cast<std::uint64_t>(sizeof(std::uint64_t));
    if (sz != expected) {
      throw std::runtime_error("row_start_offsets.bin size does not match rowCount from counts.bin");
    }

    std::ifstream rin(roPath, std::ios::binary);
    if (!rin) throw std::runtime_error("Failed to open meta_data/row_start_offsets.bin");

    rowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
    rin.read(reinterpret_cast<char*>(rowOffsets_.get()),
             static_cast<std::streamsize>(rowCount_ * sizeof(std::uint64_t)));
    if (!rin) throw std::runtime_error("Failed reading meta_data/row_start_offsets.bin");
  }

  // 3) Infer chunkSize_ from the widths file so lookup/getValue can locate the right chunk.
  chunkSize_ = inferChunkSizeFromWidths(outputDirectory_, columnCount_);

  // 4) Initialize per-row cursors.
  currentRowOffsets_ = std::make_unique<std::uint64_t[]>(static_cast<std::size_t>(rowCount_));
  for (std::uint64_t r = 0; r < rowCount_; ++r) {
    currentRowOffsets_[static_cast<std::size_t>(r)] = rowOffsets_[static_cast<std::size_t>(r)];
  }

  parseCompleted_ = true;
}

// ---- Chunk column int->string maps container (one file per chunk) ----

static std::filesystem::path chunkColumnMapsContainerPath(const std::string& outputDirectory, std::uint64_t chunkIndex) {
  std::ostringstream name;
  name << "chunk_" << chunkIndex << "_int_string_maps.bin";
  return std::filesystem::path(outputDirectory) / "meta_data" / name.str();
}

struct IntStringMapBlockIndex {
  std::uint64_t chunkCols = 0;
  std::uint64_t firstCol = 0;
  std::uint64_t lastCol = 0;
  std::vector<std::uint64_t> offsets; // size = chunkCols + 1
};

static IntStringMapBlockIndex readChunkColumnMapsIndexOrThrow(std::istream& in) {
  IntStringMapBlockIndex idx;
  idx.chunkCols = readU64(in);
  idx.firstCol = readU64(in);
  if (idx.chunkCols == 0) throw std::runtime_error("Corrupt chunk int->string container: chunkCols=0");

  idx.offsets.resize(static_cast<std::size_t>(idx.chunkCols) + 1);
  in.read(reinterpret_cast<char*>(idx.offsets.data()),
          static_cast<std::streamsize>(idx.offsets.size() * sizeof(std::uint64_t)));
  if (!in) throw std::runtime_error("Corrupt chunk int->string container: missing offsets");

  for (std::size_t i = 0; i + 1 < idx.offsets.size(); ++i) {
    if (idx.offsets[i + 1] < idx.offsets[i]) {
      throw std::runtime_error("Corrupt chunk int->string container: offsets not monotonic");
    }
  }

  idx.lastCol = idx.firstCol + idx.chunkCols - 1;
  return idx;
}

static void writeU64Raw(std::ostream& out, std::uint64_t v) {
  out.write(reinterpret_cast<const char*>(&v), sizeof(v));
  if (!out) throw std::runtime_error("Failed writing uint64");
}

static std::uint64_t tellpU64(std::ostream& out) {
  const auto pos = out.tellp();
  if (pos < 0) throw std::runtime_error("tellp failed");
  return static_cast<std::uint64_t>(pos);
}

static void writeIntStringMapBlock(std::ostream& out, const std::vector<std::string>& idToString) {
  if (idToString.empty()) throw std::runtime_error("idToString empty");

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
      throw std::runtime_error("int->string blob exceeds 4GB");
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

static std::string readStringFromIntStringMapBlock(std::istream& in, std::uint64_t blockStart, std::uint32_t id) {
  in.clear();
  in.seekg(static_cast<std::streamoff>(blockStart), std::ios::beg);
  if (!in) throw std::runtime_error("Failed seeking to column map block");

  std::uint32_t maxId = 0;
  in.read(reinterpret_cast<char*>(&maxId), sizeof(maxId));
  if (!in) throw std::runtime_error("Failed reading maxId");
  if (id > maxId) throw std::runtime_error("id out of range for this column");

  const std::uint64_t offsetsCount = static_cast<std::uint64_t>(maxId) + 2;
  const std::uint64_t offsetsBase = sizeof(std::uint32_t);

  std::uint32_t start = 0;
  std::uint32_t end = 0;

  in.seekg(static_cast<std::streamoff>(blockStart + offsetsBase + static_cast<std::uint64_t>(id) * sizeof(std::uint32_t)),
           std::ios::beg);
  in.read(reinterpret_cast<char*>(&start), sizeof(start));
  in.seekg(static_cast<std::streamoff>(blockStart + offsetsBase + static_cast<std::uint64_t>(id + 1) * sizeof(std::uint32_t)),
           std::ios::beg);
  in.read(reinterpret_cast<char*>(&end), sizeof(end));
  if (!in) throw std::runtime_error("Failed reading offsets");
  if (end < start) throw std::runtime_error("corrupt offsets");

  const std::uint32_t len = end - start;
  const std::uint64_t blobBase = sizeof(std::uint32_t) + offsetsCount * sizeof(std::uint32_t);

  in.seekg(static_cast<std::streamoff>(blockStart + blobBase + start), std::ios::beg);
  if (!in) throw std::runtime_error("Failed seeking blob");

  std::string s;
  s.resize(len);
  if (len > 0) {
    in.read(s.data(), static_cast<std::streamsize>(len));
    if (!in) throw std::runtime_error("Failed reading string");
  }
  return s;
}

static void writeChunkColumnMapsContainer(const std::string& outputDirectory,
                                         std::uint64_t chunkIndex,
                                         std::uint64_t firstCol,
                                         const std::vector<std::vector<std::string>>& colIdToString) {
  const std::uint64_t chunkCols = static_cast<std::uint64_t>(colIdToString.size());
  if (chunkCols == 0) throw std::runtime_error("chunkCols is 0");

  const auto path = chunkColumnMapsContainerPath(outputDirectory, chunkIndex);
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) throw std::runtime_error("Failed to open chunk column maps container: " + path.string());

  writeU64Raw(out, chunkCols);
  writeU64Raw(out, firstCol);

  const std::uint64_t offsetsTablePos = tellpU64(out);
  for (std::uint64_t i = 0; i < chunkCols + 1; ++i) {
    writeU64Raw(out, 0);
  }

  std::vector<std::uint64_t> offsets(static_cast<std::size_t>(chunkCols) + 1, 0);

  for (std::uint64_t i = 0; i < chunkCols; ++i) {
    offsets[static_cast<std::size_t>(i)] = tellpU64(out);
    writeIntStringMapBlock(out, colIdToString[static_cast<std::size_t>(i)]);
  }
  offsets[static_cast<std::size_t>(chunkCols)] = tellpU64(out);

  out.seekp(static_cast<std::streamoff>(offsetsTablePos), std::ios::beg);
  if (!out) throw std::runtime_error("Failed seeking to offsets table for patch");

  out.write(reinterpret_cast<const char*>(offsets.data()),
            static_cast<std::streamsize>(offsets.size() * sizeof(std::uint64_t)));
  if (!out) throw std::runtime_error("Failed writing patched offsets table");

  out.flush();
  if (!out) throw std::runtime_error("Failed flushing chunk column maps container");
}

// ----------------- Chunk parsing + lookup/getValue -----------------

namespace {

static std::string normalizeFieldToKey(const std::string& raw, bool wasQuoted) {
  if (wasQuoted) {
    bool anyNonSpace = false;
    for (unsigned char ch : raw) {
      if (std::isspace(ch) == 0) {
        anyNonSpace = true;
        break;
      }
    }
    return anyNonSpace ? raw : std::string();
  }
  return trimAsciiWhitespace(raw);
}

struct FieldToken {
  std::string key;
  bool isEmpty = true;
};

static FieldToken readOneField(std::istream& in, char& delimOut) {
  std::string raw;
  raw.reserve(64);

  bool inQuotes = false;
  bool wasQuoted = false;
  bool afterClosingQuote = false;

  while (true) {
    const int ci = in.peek();
    if (ci == EOF) {
      delimOut = '\0';
      return FieldToken{std::string(), true};
    }
    const char c = static_cast<char>(ci);
    if (c == ' ' || c == '\t') {
      in.get();
      continue;
    }
    break;
  }

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

      if (c == '\r') {
        if (in.peek() == '\n') in.get();
        c = '\n';
      }
      raw.push_back(c);
      continue;
    }

    if (afterClosingQuote) {
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
  std::uint8_t bits = 1;
  while ((maxValue >> bits) != 0 && bits < 32) ++bits;
  return bits;
}

} // namespace

void DataTable::parseChunks(int /*threads*/) {
  if (columnCount_ == 0) throw std::runtime_error("Column count is 0");
  if (rowCount_ < 2) throw std::runtime_error("No data rows present");
  if (chunkSize_ == 0) throw std::runtime_error("chunkSize is 0");

  const std::uint64_t dataRowCount = rowCount_ - 1;
  const std::uint64_t totalChunks =
      (columnCount_ + static_cast<std::uint64_t>(chunkSize_) - 1) / static_cast<std::uint64_t>(chunkSize_);

  const int pbColumnChunks = progressCreate(totalChunks, "Column chunks", progressbar::ProgressBars::Color::Green);
  progressSet(pbColumnChunks, 0);

  std::ofstream widthOut(columnChunkWidthPath(outputDirectory_), std::ios::binary | std::ios::trunc);
  if (!widthOut) throw std::runtime_error("Failed to open meta_data/column_chunk_width.bin");

  for (std::uint64_t chunkIndex = 0; chunkIndex < totalChunks; ++chunkIndex) {
    const std::uint64_t firstCol = chunkIndex * static_cast<std::uint64_t>(chunkSize_);
    const std::uint64_t lastCol =
        std::min(firstCol + static_cast<std::uint64_t>(chunkSize_) - 1, columnCount_ - 1);
    const std::uint64_t chunkCols = lastCol - firstCol + 1;

    struct ColDict {
      std::unordered_map<std::string, std::uint32_t> map;
      std::vector<std::string> idToString;
    };

    std::vector<ColDict> dicts(static_cast<std::size_t>(chunkCols));
    for (auto& d : dicts) {
      d.map.reserve(1024);
      d.idToString.reserve(1024);
      d.idToString.emplace_back(std::string());
      d.map.emplace(d.idToString[0], 0u);
    }

    std::vector<std::vector<std::uint32_t>> colValues(static_cast<std::size_t>(chunkCols));
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
          auto& d = dicts[static_cast<std::size_t>(localCol)];
          auto it = d.map.find(tok.key);
          if (it == d.map.end()) {
            const std::uint32_t nextId = static_cast<std::uint32_t>(d.idToString.size());
            d.idToString.push_back(tok.key);
            it = d.map.emplace(d.idToString.back(), nextId).first;
            if (nextId > maxIdThisChunk) maxIdThisChunk = nextId;
          }
          id = it->second;
        }

        colValues[static_cast<std::size_t>(localCol)][static_cast<std::size_t>(dataRow)] = id;

        if (localCol + 1 < chunkCols && delim != ',') {
          throw std::runtime_error("Row ended early while reading chunk columns");
        }
      }
    }

    // Write per-column int->string maps to one file.
    {
      std::vector<std::vector<std::string>> perCol;
      perCol.reserve(static_cast<std::size_t>(chunkCols));
      for (std::uint64_t localCol = 0; localCol < chunkCols; ++localCol) {
        perCol.push_back(dicts[static_cast<std::size_t>(localCol)].idToString);
      }
      writeChunkColumnMapsContainer(outputDirectory_, chunkIndex, firstCol, perCol);
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

    progressTick(pbColumnChunks);
  }

  progressComplete(pbColumnChunks);

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
  if (!widthIn) throw std::runtime_error("Failed seeking meta_data/column_chunk_width.bin");

  std::uint8_t bitWidth = 0;
  widthIn.read(reinterpret_cast<char*>(&bitWidth), 1);
  if (!widthIn) throw std::runtime_error("Failed reading chunk bit width");
  if (bitWidth == 0 || bitWidth > 32) throw std::runtime_error("Invalid chunk bit width");

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

  // New per-column map container.
  {
    const auto containerPath = chunkColumnMapsContainerPath(outputDirectory_, chunkIndex);
    if (std::filesystem::exists(containerPath)) {
      std::ifstream in(containerPath, std::ios::binary);
      if (!in) throw std::runtime_error("Failed to open chunk maps container: " + containerPath.string());

      const auto idx = readChunkColumnMapsIndexOrThrow(in);
      if (col < idx.firstCol || col > idx.lastCol) {
        throw std::runtime_error("Corrupt chunk maps container: column out of chunk range");
      }

      const std::uint64_t localCol = col - idx.firstCol;
      const std::uint64_t blockStart = idx.offsets[static_cast<std::size_t>(localCol)];

      return readStringFromIntStringMapBlock(in, blockStart, id);
    }
  }

  // Old fallback.
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

std::string DataTable::getColumnValue(std::uint64_t col, std::uint32_t featureId) const {
  ensureParsed();
  if (col >= columnCount_) throw std::out_of_range("col out of range");

  const std::uint64_t chunkIndex = col / static_cast<std::uint64_t>(chunkSize_);

  // New per-column map container.
  {
    const auto containerPath = chunkColumnMapsContainerPath(outputDirectory_, chunkIndex);
    if (std::filesystem::exists(containerPath)) {
      std::ifstream in(containerPath, std::ios::binary);
      if (!in) throw std::runtime_error("Failed to open chunk maps container: " + containerPath.string());

      const auto idx = readChunkColumnMapsIndexOrThrow(in);
      if (col < idx.firstCol || col > idx.lastCol) {
        throw std::runtime_error("Corrupt chunk maps container: column out of chunk range");
      }

      const std::uint64_t localCol = col - idx.firstCol;
      const std::uint64_t blockStart = idx.offsets[static_cast<std::size_t>(localCol)];

      // Validate bounds quickly so we throw out_of_range for bad featureId.
      in.clear();
      in.seekg(static_cast<std::streamoff>(blockStart), std::ios::beg);
      if (!in) throw std::runtime_error("Failed seeking to column map block");
      std::uint32_t maxId = 0;
      in.read(reinterpret_cast<char*>(&maxId), sizeof(maxId));
      if (!in) throw std::runtime_error("Failed reading maxId");
      if (featureId > maxId) throw std::out_of_range("featureId out of range");

      return readStringFromIntStringMapBlock(in, blockStart, featureId);
    }
  }

  // Old fallback.
  {
    const auto path = chunkIntStringMapPath(outputDirectory_, chunkIndex);
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open chunk map file: " + path.string());

    std::uint32_t maxId = 0;
    in.read(reinterpret_cast<char*>(&maxId), sizeof(maxId));
    if (!in) throw std::runtime_error("Failed reading maxId");
    if (featureId > maxId) throw std::out_of_range("featureId out of range");

    const std::uint64_t offsetsCount = static_cast<std::uint64_t>(maxId) + 2;
    const std::uint64_t offsetsBase = sizeof(std::uint32_t);

    std::uint32_t start = 0;
    std::uint32_t end = 0;

    in.seekg(static_cast<std::streamoff>(offsetsBase + static_cast<std::uint64_t>(featureId) * sizeof(std::uint32_t)),
             std::ios::beg);
    in.read(reinterpret_cast<char*>(&start), sizeof(start));
    in.seekg(static_cast<std::streamoff>(offsetsBase + static_cast<std::uint64_t>(featureId + 1) * sizeof(std::uint32_t)),
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
}

std::uint32_t DataTable::getFeatureCount(std::uint64_t col) const {
  ensureParsed();
  if (col >= columnCount_) throw std::out_of_range("col out of range");

  const std::uint64_t chunkIndex = col / static_cast<std::uint64_t>(chunkSize_);

  // New per-column map container.
  {
    const auto containerPath = chunkColumnMapsContainerPath(outputDirectory_, chunkIndex);
    if (std::filesystem::exists(containerPath)) {
      std::ifstream in(containerPath, std::ios::binary);
      if (!in) throw std::runtime_error("Failed to open chunk maps container: " + containerPath.string());

      const auto idx = readChunkColumnMapsIndexOrThrow(in);
      if (col < idx.firstCol || col > idx.lastCol) {
        throw std::runtime_error("Corrupt chunk maps container: column out of chunk range");
      }

      const std::uint64_t localCol = col - idx.firstCol;
      const std::uint64_t blockStart = idx.offsets[static_cast<std::size_t>(localCol)];

      in.clear();
      in.seekg(static_cast<std::streamoff>(blockStart), std::ios::beg);
      if (!in) throw std::runtime_error("Failed seeking to column map block");

      std::uint32_t maxId = 0;
      in.read(reinterpret_cast<char*>(&maxId), sizeof(maxId));
      if (!in) throw std::runtime_error("Failed reading maxId");

      // Distinct non-empty features; id 0 is the empty sentinel.
      return maxId;
    }
  }

  // Old fallback.
  {
    const auto path = chunkIntStringMapPath(outputDirectory_, chunkIndex);
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open chunk map file: " + path.string());

    std::uint32_t maxId = 0;
    in.read(reinterpret_cast<char*>(&maxId), sizeof(maxId));
    if (!in) throw std::runtime_error("Failed reading maxId");
    return maxId;
  }
}

} // namespace


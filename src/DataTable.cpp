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

namespace DataTableLib {

namespace {

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

class Sha256 {
public:
  Sha256() { reset(); }

  void reset() {
    totalLen_ = 0;
    bufferLen_ = 0;
    state_ = {0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
              0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U};
  }

  void update(const std::uint8_t* data, std::size_t len) {
    if (len == 0) return;
    totalLen_ += static_cast<std::uint64_t>(len);

    std::size_t idx = 0;
    if (bufferLen_ > 0) {
      const std::size_t need = 64 - bufferLen_;
      const std::size_t take = std::min(need, len);
      std::copy(data, data + take, buffer_.begin() + static_cast<std::ptrdiff_t>(bufferLen_));
      bufferLen_ += take;
      idx += take;
      if (bufferLen_ == 64) {
        transform(buffer_.data());
        bufferLen_ = 0;
      }
    }

    while (idx + 64 <= len) {
      transform(data + idx);
      idx += 64;
    }

    const std::size_t rem = len - idx;
    if (rem > 0) {
      std::copy(data + idx, data + len, buffer_.begin());
      bufferLen_ = rem;
    }
  }

  std::array<std::uint8_t, 32> digest() {
    // Padding: 0x80 then zeros, then 64-bit big-endian length in bits.
    std::array<std::uint8_t, 64> pad{};
    pad[0] = 0x80;

    const std::uint64_t totalBits = totalLen_ * 8ULL;

    const std::size_t padLen = (bufferLen_ < 56) ? (56 - bufferLen_) : (56 + 64 - bufferLen_);
    update(pad.data(), padLen);

    std::array<std::uint8_t, 8> lenBytes{};
    for (int i = 0; i < 8; ++i) {
      lenBytes[static_cast<std::size_t>(7 - i)] = static_cast<std::uint8_t>((totalBits >> (i * 8)) & 0xFFULL);
    }
    update(lenBytes.data(), lenBytes.size());

    // Now buffer should be aligned and fully processed.
    std::array<std::uint8_t, 32> out{};
    for (int i = 0; i < 8; ++i) {
      const std::uint32_t w = state_[static_cast<std::size_t>(i)];
      out[static_cast<std::size_t>(i * 4 + 0)] = static_cast<std::uint8_t>((w >> 24) & 0xFFU);
      out[static_cast<std::size_t>(i * 4 + 1)] = static_cast<std::uint8_t>((w >> 16) & 0xFFU);
      out[static_cast<std::size_t>(i * 4 + 2)] = static_cast<std::uint8_t>((w >> 8) & 0xFFU);
      out[static_cast<std::size_t>(i * 4 + 3)] = static_cast<std::uint8_t>((w >> 0) & 0xFFU);
    }

    return out;
  }

  std::string hexdigest() {
    const auto d = digest();
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (std::uint8_t b : d) {
      ss << std::setw(2) << static_cast<int>(b);
    }
    return ss.str();
  }

private:
  static constexpr std::array<std::uint32_t, 64> K = {
      0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U,
      0x923f82a4U, 0xab1c5ed5U, 0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
      0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U, 0xe49b69c1U, 0xefbe4786U,
      0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
      0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U,
      0x06ca6351U, 0x14292967U, 0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
      0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U, 0xa2bfe8a1U, 0xa81a664bU,
      0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
      0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU,
      0x5b9cca4fU, 0x682e6ff3U, 0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
      0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U};

  static std::uint32_t rotr(std::uint32_t x, std::uint32_t n) {
    return (x >> n) | (x << (32 - n));
  }

  static std::uint32_t ch(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
    return (x & y) ^ (~x & z);
  }

  static std::uint32_t maj(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
    return (x & y) ^ (x & z) ^ (y & z);
  }

  static std::uint32_t bigSigma0(std::uint32_t x) {
    return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22);
  }

  static std::uint32_t bigSigma1(std::uint32_t x) {
    return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25);
  }

  static std::uint32_t smallSigma0(std::uint32_t x) {
    return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3);
  }

  static std::uint32_t smallSigma1(std::uint32_t x) {
    return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10);
  }

  static std::uint32_t readBe32(const std::uint8_t* p) {
    return (static_cast<std::uint32_t>(p[0]) << 24) | (static_cast<std::uint32_t>(p[1]) << 16) |
           (static_cast<std::uint32_t>(p[2]) << 8) | (static_cast<std::uint32_t>(p[3]));
  }

  void transform(const std::uint8_t* block) {
    std::array<std::uint32_t, 64> w{};
    for (int i = 0; i < 16; ++i) {
      w[static_cast<std::size_t>(i)] = readBe32(block + i * 4);
    }
    for (int i = 16; i < 64; ++i) {
      w[static_cast<std::size_t>(i)] =
          smallSigma1(w[static_cast<std::size_t>(i - 2)]) + w[static_cast<std::size_t>(i - 7)] +
          smallSigma0(w[static_cast<std::size_t>(i - 15)]) + w[static_cast<std::size_t>(i - 16)];
    }

    std::uint32_t a = state_[0];
    std::uint32_t b = state_[1];
    std::uint32_t c = state_[2];
    std::uint32_t d = state_[3];
    std::uint32_t e = state_[4];
    std::uint32_t f = state_[5];
    std::uint32_t g = state_[6];
    std::uint32_t h = state_[7];

    for (int i = 0; i < 64; ++i) {
      const std::uint32_t t1 = h + bigSigma1(e) + ch(e, f, g) + K[static_cast<std::size_t>(i)] +
                               w[static_cast<std::size_t>(i)];
      const std::uint32_t t2 = bigSigma0(a) + maj(a, b, c);
      h = g;
      g = f;
      f = e;
      e = d + t1;
      d = c;
      c = b;
      b = a;
      a = t1 + t2;
    }

    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
    state_[4] += e;
    state_[5] += f;
    state_[6] += g;
    state_[7] += h;
  }

  std::uint64_t totalLen_ = 0;
  std::size_t bufferLen_ = 0;
  std::array<std::uint8_t, 64> buffer_{};
  std::array<std::uint32_t, 8> state_{};
};

// Compute SHA-256 of a file and return the lowercase hex digest.
static std::string sha256FileHex(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open file for hashing: " + path);

  Sha256 sha;
  std::array<std::uint8_t, 1u << 20> buf{}; // 1 MiB
  while (true) {
    in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
    const std::streamsize got = in.gcount();
    if (got > 0) {
      sha.update(buf.data(), static_cast<std::size_t>(got));
    }
    if (!in) {
      if (in.eof()) break;
      throw std::runtime_error("Error while reading file for hashing: " + path);
    }
  }

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
      // To do that, we treat it as the first byte and then continue with buf[0].
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

  const int pbColumnChunks = progressCreate(
      totalChunks,
      "Column chunks",
      progressbar::ProgressBars::Color::Green);
  progressSet(pbColumnChunks, 0);

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

      for (std::uint32_t id = 0; id <= maxId; id++) {
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

    // Progress update: one tick per completed column chunk.
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


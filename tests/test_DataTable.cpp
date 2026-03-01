#include <gtest/gtest.h>

#include <DataTable/DataTable.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

using DataTableLib::DataTable;

namespace {

std::filesystem::path makeTempDir(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / ("DataTableTests_" + name);
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

void writeTextFile(const std::filesystem::path& p, const std::string& content) {
  std::ofstream out(p, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(static_cast<bool>(out));
  out.write(content.data(), static_cast<std::streamsize>(content.size()));
  ASSERT_TRUE(static_cast<bool>(out));
}

static void writeExact(std::ofstream& out, const std::string& s) {
  out.write(s.data(), static_cast<std::streamsize>(s.size()));
  ASSERT_TRUE(static_cast<bool>(out));
}

struct GeneratedCsvOffsets {
  std::filesystem::path csvPath;
  std::vector<std::uint64_t> offsets;  // data-row start offsets (0-based)
};

// Generates a CSV with uneven byte-sized data rows while keeping the column count constant.
// Offsets are recorded via tellp() right before each logical CSV record is written.
GeneratedCsvOffsets generateUnevenCsvWithQuotesNewlines(const std::filesystem::path& dir,
                                                       std::uint64_t approxTargetBytes) {
  GeneratedCsvOffsets gen;
  gen.csvPath = dir / "uneven.csv";

  std::ofstream out(gen.csvPath, std::ios::binary | std::ios::trunc);
  EXPECT_TRUE(static_cast<bool>(out));

  // Header row.
  writeExact(out, "Text,Value\n");

  while (static_cast<std::uint64_t>(out.tellp()) < approxTargetBytes) {
    const auto rowStart = static_cast<std::uint64_t>(out.tellp());
    gen.offsets.push_back(rowStart);

    const std::uint64_t i = static_cast<std::uint64_t>(gen.offsets.size() - 1);

    // Create a quoted field with variability:
    // - leading/trailing whitespace
    // - escaped quotes via "" (CSV escape)
    // - embedded newlines (sometimes \n, sometimes \r\n) -- inside quotes
    std::string text;
    text.reserve(256);

    text += "\"  row ";
    text += std::to_string(i);
    text += "  :  He said \"\"hi\"\"";

    const int variant = static_cast<int>(i % 5);
    if (variant == 0) {
      text += "  \n  line2  \"\"Q\"\"  ";
    } else if (variant == 1) {
      text += "  \r\n  CRLF line2  ";
    } else if (variant == 2) {
      text += "  \n  line2\n  line3  ";
    } else if (variant == 3) {
      text += "        lots   of   spaces      ";
    } else {
      text += "  \r\n  mixed\r\n  newlines  ";
    }

    text += "\"";  // closing quote

    // Exactly two columns.
    writeExact(out, text);
    writeExact(out, ",");
    writeExact(out, std::to_string(1000000 + i));
    writeExact(out, "\n");
  }

  return gen;
}

}  // namespace

TEST(DataTable, ConstructorAndSetters_StorePaths) {
  DataTable dt("input.csv", "out_dir");
  EXPECT_EQ(dt.inputFilePath(), "input.csv");
  EXPECT_EQ(dt.outputDirectory(), "out_dir");

  dt.setInputFilePath("a.csv");
  dt.setOutputDirectory("cache");
  EXPECT_EQ(dt.inputFilePath(), "a.csv");
  EXPECT_EQ(dt.outputDirectory(), "cache");
}

TEST(DataTable, MetadataAccess_ThrowsBeforeParse) {
  DataTable dt;
  EXPECT_FALSE(dt.parseCompleted());

  EXPECT_THROW(dt.getColumnCount(), std::runtime_error);
  EXPECT_THROW(dt.getColumnHeader(0), std::runtime_error);
  EXPECT_THROW(dt.getColumnHeaderJson(), std::runtime_error);
  EXPECT_THROW(dt.getRowCount(), std::runtime_error);
  EXPECT_THROW(dt.getRowOffset(0), std::runtime_error);
}

TEST(DataTable, Parse_WritesHeaderRowBin_AndGettersReadItBack) {
  const auto outDir = makeTempDir("basic");
  const auto csvPath = outDir / "in.csv";

  // Deliberate whitespace around delimiters and before newline.
  writeTextFile(csvPath, " Name , Age,  City  \r\n1,2,3\n4,5,6\n");

  DataTable dt(csvPath.string(), outDir.string());
  EXPECT_FALSE(dt.parseCompleted());

  dt.parse(2);
  EXPECT_TRUE(dt.parseCompleted());

  EXPECT_EQ(dt.getColumnCount(), 3u);

  EXPECT_EQ(dt.getColumnHeader(0), "Name");
  EXPECT_EQ(dt.getColumnHeader(1), "Age");
  EXPECT_EQ(dt.getColumnHeader(2), "City");

  const auto j = nlohmann::json::parse(dt.getColumnHeaderJson());
  ASSERT_TRUE(j.is_array());
  ASSERT_EQ(j.size(), 3);
  EXPECT_EQ(j[0], "Name");
  EXPECT_EQ(j[1], "Age");
  EXPECT_EQ(j[2], "City");

  // Row 0 is header, rows 1.. are data.
  EXPECT_EQ(dt.getRowCount(), 3u);
  EXPECT_EQ(dt.getRowOffset(0), 0u);
  EXPECT_LT(dt.getRowOffset(0), dt.getRowOffset(1));
  EXPECT_LT(dt.getRowOffset(1), dt.getRowOffset(2));

  EXPECT_THROW(dt.getRowOffset(-1), std::out_of_range);
  EXPECT_THROW(dt.getRowOffset(3), std::out_of_range);
}

TEST(DataTable, Parse_PreservesWhitespaceInsideQuotes) {
  const auto outDir = makeTempDir("quotes_whitespace");
  const auto csvPath = outDir / "in.csv";

  writeTextFile(csvPath, "\"  A  \" ,B\n1,2\n");

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(1);

  EXPECT_EQ(dt.getColumnHeader(0), "  A  ");
  EXPECT_EQ(dt.getColumnHeader(1), "B");
}

TEST(DataTable, Parse_HandlesEscapedQuotesInHeader) {
  const auto outDir = makeTempDir("escaped_quotes");
  const auto csvPath = outDir / "in.csv";

  writeTextFile(csvPath, "\"He said \"\"hi\"\"\",X\n1,2\n");

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(1);

  EXPECT_EQ(dt.getColumnHeader(0), "He said \"hi\"");
  EXPECT_EQ(dt.getColumnHeader(1), "X");
}

TEST(DataTable, GetColumnHeader_OutOfRangeThrows) {
  const auto outDir = makeTempDir("oor");
  const auto csvPath = outDir / "in.csv";

  writeTextFile(csvPath, "A,B\n1,2\n");

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(1);

  EXPECT_THROW(dt.getColumnHeader(-1), std::out_of_range);
  EXPECT_THROW(dt.getColumnHeader(2), std::out_of_range);
}

TEST(DataTable, Parse_HeaderLongerThan255BytesThrows) {
  const auto outDir = makeTempDir("too_long");
  const auto csvPath = outDir / "in.csv";

  std::string h(256, 'a');
  writeTextFile(csvPath, h + ",B\n1,2\n");

  DataTable dt(csvPath.string(), outDir.string());
  EXPECT_THROW(dt.parse(1), std::runtime_error);
}

TEST(DataTable, Parse_RowOffsetsPredictable_UnevenRowsWithQuotesNewlines_Threads1To10) {
  const auto outDir = makeTempDir("uneven_rows_quotes_newlines");

  // Keep test runtime reasonable but still non-trivial.
  const std::uint64_t targetBytes = 2ull * 1024ull * 1024ull;
  const auto gen = generateUnevenCsvWithQuotesNewlines(outDir, targetBytes);

  ASSERT_FALSE(gen.offsets.empty());

  // Expected offsets now include the header row at offset 0.
  std::vector<std::uint64_t> expectedOffsets;
  expectedOffsets.reserve(gen.offsets.size() + 1);
  expectedOffsets.push_back(0);
  expectedOffsets.insert(expectedOffsets.end(), gen.offsets.begin(), gen.offsets.end());

  for (int threads = 1; threads <= 10; ++threads) {
    DataTable dt(gen.csvPath.string(), outDir.string());
    dt.parse(threads);

    ASSERT_TRUE(dt.parseCompleted());
    ASSERT_EQ(dt.getColumnCount(), 2u);
    ASSERT_EQ(dt.getRowCount(), static_cast<std::uint64_t>(expectedOffsets.size()));

    for (std::uint64_t r = 0; r < dt.getRowCount(); ++r) {
      ASSERT_EQ(dt.getRowOffset(static_cast<int>(r)), expectedOffsets[static_cast<std::size_t>(r)])
          << "threads=" << threads << " row=" << r;
    }
  }
}

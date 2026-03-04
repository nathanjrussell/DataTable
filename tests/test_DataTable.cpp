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

TEST(DataTable, GetColumnIndex_By_String) {
  const auto outDir = makeTempDir("get_column_index");
  const auto csvPath = outDir / "in.csv";

  writeTextFile(csvPath, "A,B,C\n1,2,3\n");

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(1);

  EXPECT_EQ(dt.getColumnIndex("A"), 0u);
  EXPECT_EQ(dt.getColumnIndex("B"), 1u);
  EXPECT_EQ(dt.getColumnIndex("C"), 2u);
  EXPECT_THROW(dt.getColumnIndex("Nonexistent"), std::out_of_range);
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
  EXPECT_EQ(dt.getColumnIndex("Name"), 0u);
  EXPECT_EQ(dt.getColumnIndex("Age"), 1u);
  EXPECT_EQ(dt.getColumnIndex("City"), 2u);
  EXPECT_THROW(dt.getColumnIndex("Nonexistent"), std::out_of_range);

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

TEST(DataTable, Load_ReusesParsedDirectory_MetadataAndGetValueWork) {
  const auto outDir = makeTempDir("load_basic");
  const auto csvPath = outDir / "in.csv";

  writeTextFile(csvPath, "A,B,C\n1,2,3\n4,,6\n");

  // Parse into outDir.
  {
    DataTable dt(csvPath.string(), outDir.string());
    dt.parse(/*threads=*/1, /*chunkSize=*/2);
    ASSERT_TRUE(dt.parseCompleted());
    ASSERT_EQ(dt.getRowCount(), 3u);
    ASSERT_EQ(dt.getColumnCount(), 3u);
    ASSERT_EQ(dt.getValue(1, 0), "1");
    ASSERT_EQ(dt.getValue(2, 1), "");
  }

  // Now load from disk without parsing.
  DataTable loaded;
  loaded.load(outDir.string());

  ASSERT_TRUE(loaded.parseCompleted());
  EXPECT_EQ(loaded.outputDirectory(), outDir.string());

  EXPECT_EQ(loaded.getRowCount(), 3u);
  EXPECT_EQ(loaded.getColumnCount(), 3u);

  EXPECT_EQ(loaded.getColumnHeader(0), "A");
  EXPECT_EQ(loaded.getColumnHeader(1), "B");
  EXPECT_EQ(loaded.getColumnHeader(2), "C");

  EXPECT_EQ(loaded.getValue(1, 0), "1");
  EXPECT_EQ(loaded.getValue(1, 1), "2");
  EXPECT_EQ(loaded.getValue(1, 2), "3");

  EXPECT_EQ(loaded.getValue(2, 0), "4");
  EXPECT_EQ(loaded.getValue(2, 1), "");
  EXPECT_EQ(loaded.getValue(2, 2), "6");
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

  for (int threads = 1; threads <= 10; threads+=2) {
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

TEST(DataTable, Parse_ThrowsWhenRowHasMissingColumn_Threads1) {
  const auto outDir = makeTempDir("missing_col_threads1");

  const auto csvPath = std::filesystem::path(DATATABLE_SOURCE_DIR) / "tests" / "test_data_sets" /
                       "missing_col_in_row.csv";
  ASSERT_TRUE(std::filesystem::exists(csvPath)) << csvPath;

  DataTable dt(csvPath.string(), outDir.string());
  EXPECT_THROW(dt.parse(1), std::runtime_error);
}

TEST(DataTable, Parse_ThrowsWhenRowHasMissingColumn_Threads2To5) {
  const auto csvPath = std::filesystem::path(DATATABLE_SOURCE_DIR) / "tests" / "test_data_sets" /
                       "missing_col_in_row.csv";
  ASSERT_TRUE(std::filesystem::exists(csvPath)) << csvPath;

  for (int threads = 2; threads <= 5; ++threads) {
    const auto outDir = makeTempDir("missing_col_threads" + std::to_string(threads));
    DataTable dt(csvPath.string(), outDir.string());
    EXPECT_THROW(dt.parse(threads), std::runtime_error) << "threads=" << threads;
  }
}

TEST(DataTable, Parse_DuplicateHeadersThrows) {
  const auto outDir = makeTempDir("dup_headers");
  const auto csvPath = outDir / "in.csv";

  writeTextFile(csvPath, "A,B,A\n1,2,3\n");

  DataTable dt(csvPath.string(), outDir.string());
  EXPECT_THROW(dt.parse(1), std::runtime_error);
}

TEST(DataTable, Parse_InvalidUtf8InHeaderThrows) {
  const auto outDir = makeTempDir("bad_utf8_header");
  const auto csvPath = outDir / "in.csv";

  // Write an invalid UTF-8 byte sequence in the header: 0xC3 0x28 is invalid.
  std::string badHeader;
  badHeader.push_back('A');
  badHeader.push_back(',');
  badHeader.push_back(static_cast<char>(0xC3));
  badHeader.push_back(static_cast<char>(0x28));
  badHeader += ",C\n1,2,3\n";

  writeTextFile(csvPath, badHeader);

  DataTable dt(csvPath.string(), outDir.string());
  EXPECT_THROW(dt.parse(1), std::runtime_error);
}

TEST(DataTable, ColumnDictionary_GetColumnValue_And_GetFeatureCount) {
  const auto outDir = makeTempDir("column_dictionary");
  const auto csvPath = outDir / "in.csv";

  // 2 columns, 3 data rows.
  // Column A distinct values: "x", "y" => maxId=2 (id 0 is empty sentinel)
  // Column B distinct values: "u", "v" => maxId=2
  writeTextFile(csvPath, "A,B\n"
                        "x,u\n"
                        "y,v\n"
                        "x,u\n");

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(/*threads=*/1, /*chunkSize=*/1); // force separate chunks per column

  ASSERT_TRUE(dt.parseCompleted());
  ASSERT_EQ(dt.getColumnCount(), 2u);
  ASSERT_EQ(dt.getRowCount(), 4u);

  // Grab actual feature ids from persisted mapped_data.
  const std::uint32_t idAx = dt.lookupMap(/*row=*/1, /*col=*/0);
  const std::uint32_t idAy = dt.lookupMap(/*row=*/2, /*col=*/0);
  const std::uint32_t idBu = dt.lookupMap(/*row=*/1, /*col=*/1);
  const std::uint32_t idBv = dt.lookupMap(/*row=*/2, /*col=*/1);

  // Ensure ids are sequential per column (order depends on first occurrence).
  EXPECT_NE(idAx, 0u);
  EXPECT_NE(idAy, 0u);
  EXPECT_NE(idBu, 0u);
  EXPECT_NE(idBv, 0u);
  EXPECT_NE(idAx, idAy);
  EXPECT_NE(idBu, idBv);

  // getColumnValue resolves the per-column dictionary id.
  EXPECT_EQ(dt.getColumnValue(0, idAx), "x");
  EXPECT_EQ(dt.getColumnValue(0, idAy), "y");
  EXPECT_EQ(dt.getColumnValue(1, idBu), "u");
  EXPECT_EQ(dt.getColumnValue(1, idBv), "v");

  // Cross-check against getValue().
  EXPECT_EQ(dt.getValue(1, 0), dt.getColumnValue(0, idAx));
  EXPECT_EQ(dt.getValue(2, 0), dt.getColumnValue(0, idAy));
  EXPECT_EQ(dt.getValue(1, 1), dt.getColumnValue(1, idBu));
  EXPECT_EQ(dt.getValue(2, 1), dt.getColumnValue(1, idBv));

  // Feature counts are per-column maxId; with 2 distinct values => maxId == 2.
  EXPECT_EQ(dt.getFeatureCount(0), 2u);
  EXPECT_EQ(dt.getFeatureCount(1), 2u);

  // Out-of-range featureId throws.
  EXPECT_THROW(dt.getColumnValue(0, 3u), std::out_of_range);
  EXPECT_THROW(dt.getColumnValue(1, 999u), std::out_of_range);
}

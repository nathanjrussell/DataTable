#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <DataTable/DataTable.h>

namespace fs = std::filesystem;

namespace {

static fs::path makeTempDir(const std::string& name) {
  auto dir = fs::temp_directory_path() / ("DataTableTests_" + name);
  std::error_code ec;
  fs::remove_all(dir, ec);
  fs::create_directories(dir, ec);
  if (ec) throw std::runtime_error("Failed to create temp dir: " + dir.string());
  return dir;
}

static void writeTextFile(const fs::path& p, const std::string& content) {
  std::ofstream out(p, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(static_cast<bool>(out)) << "Failed to open for write: " << p;
  out.write(content.data(), static_cast<std::streamsize>(content.size()));
  ASSERT_TRUE(static_cast<bool>(out)) << "Failed to write: " << p;
}

} // namespace

TEST(DataSet1, GetValue_MatchesEveryCell) {
  const fs::path outDir = makeTempDir("data_set_1_out");
  const fs::path csvPath = outDir / "data_set_1.csv";

  // Match the same semantics as the historical checked-in dataset, including header whitespace/quotes.
  const std::string csv =
      "obs_id,first_name, last_name,age, city, \"\"\"state \"\"\", \"country\"\n"
      "1,John,Doe,28,New York,NY,USA\n"
      "2,Jane,Smith,34,Los Angeles,CA,USA\n"
      "3,Emily,Jones,22,Chicago,IL,USA\n"
      "4,Michael,Brown,45,Houston,TX,USA\n"
      "5,Jessica,Davis,31,Phoenix,AZ,USA\n"
      "6,David,Wilson,29,Philadelphia,PA,USA\n"
      "7,Sarah,Garcia,27,San Antonio,TX,USA\n"
      "8,Daniel,Miller,40,San Diego,CA,USA\n"
      "9,Laura,Martinez,26,Dallas,TX,USA\n"
      "10,James,Anderson,33,San Jose,CA,USA\n"
      "11,Emily,Johnson,24,Austin,TX,USA\n"
      "12,Michael,Smith,,Jacksonville,Fl,USA\n"
      "13,Jessica,Brown,30,Fort Worth,TX,USAA\n"
      "14,David,Garcia,27,Columbus,OH,USA\n"
      "15,Sarah,Miller,,Charlotte,NC,USA\n"
      "16,Daniel,Martinez,35,San Francisco,CA,USA\n"
      "17,Laura,Anderson,28,Indianapolis,IN,USA\n";

  writeTextFile(csvPath, csv);

  DataTableLib::DataTable t;
  t.setInputFilePath(csvPath.string());
  t.setOutputDirectory(outDir.string());
  t.parse(/*threads=*/1, /*chunkSize=*/1);

  ASSERT_EQ(t.getRowCount(), 18);
  ASSERT_EQ(t.getColumnCount(), 7);

  const std::vector<std::vector<std::string>> expected = {
      {"1", "John", "Doe", "28", "New York", "NY", "USA"},
      {"2", "Jane", "Smith", "34", "Los Angeles", "CA", "USA"},
      {"3", "Emily", "Jones", "22", "Chicago", "IL", "USA"},
      {"4", "Michael", "Brown", "45", "Houston", "TX", "USA"},
      {"5", "Jessica", "Davis", "31", "Phoenix", "AZ", "USA"},
      {"6", "David", "Wilson", "29", "Philadelphia", "PA", "USA"},
      {"7", "Sarah", "Garcia", "27", "San Antonio", "TX", "USA"},
      {"8", "Daniel", "Miller", "40", "San Diego", "CA", "USA"},
      {"9", "Laura", "Martinez", "26", "Dallas", "TX", "USA"},
      {"10", "James", "Anderson", "33", "San Jose", "CA", "USA"},
      {"11", "Emily", "Johnson", "24", "Austin", "TX", "USA"},
      {"12", "Michael", "Smith", "", "Jacksonville", "Fl", "USA"},
      {"13", "Jessica", "Brown", "30", "Fort Worth", "TX", "USAA"},
      {"14", "David", "Garcia", "27", "Columbus", "OH", "USA"},
      {"15", "Sarah", "Miller", "", "Charlotte", "NC", "USA"},
      {"16", "Daniel", "Martinez", "35", "San Francisco", "CA", "USA"},
      {"17", "Laura", "Anderson", "28", "Indianapolis", "IN", "USA"},
  };

  for (std::uint64_t r = 1; r <= 17; ++r) {
    for (std::uint64_t c = 0; c <= 6; ++c) {
      const std::string got = t.getValue(r, c);
      const std::string& want = expected[static_cast<std::size_t>(r - 1)][static_cast<std::size_t>(c)];
      ASSERT_EQ(got, want) << "Mismatch at row " << r << ", col " << c;

      // Extra invariant: mapping round-trip for every single value.
      const std::uint32_t mapped = t.lookupMap(r, c);
      const std::string roundTrip = t.getColumnValue(c, mapped);
      ASSERT_EQ(roundTrip, want) << "Round-trip mismatch at row " << r << ", col " << c
                                << " (mapped=" << mapped << ")";
    }
  }

  // ---- Verify ALL per-column feature dictionaries + ALL per-cell mapped ids ----
  // With threads=1 and chunkSize=1, each column dictionary is built in first-seen order.
  // We rebuild those dictionaries from `expected`, then:
  //  - Validate getFeatureCount(col) matches (# distinct non-empty values)
  //  - Validate getColumnValue(col, featureId) for every featureId
  //  - Validate lookupMap(row,col) for every cell (expected featureId)

  for (std::uint64_t c = 0; c <= 6; ++c) {
    std::vector<std::string> nonEmptyFirstSeen;
    nonEmptyFirstSeen.reserve(64);

    auto indexOf = [&](const std::string& v) -> std::uint32_t {
      if (v.empty()) return 0u; // empty sentinel
      for (std::size_t i = 0; i < nonEmptyFirstSeen.size(); ++i) {
        if (nonEmptyFirstSeen[i] == v) return static_cast<std::uint32_t>(i + 1);
      }
      nonEmptyFirstSeen.push_back(v);
      return static_cast<std::uint32_t>(nonEmptyFirstSeen.size());
    };

    // Build the expected per-column dictionary and per-cell ids.
    std::vector<std::uint32_t> expectedIds;
    expectedIds.resize(17);
    for (std::size_t ri = 0; ri < 17; ++ri) {
      expectedIds[ri] = indexOf(expected[ri][static_cast<std::size_t>(c)]);
    }

    // Dictionary invariants
    ASSERT_EQ(t.getFeatureCount(c), static_cast<std::uint32_t>(nonEmptyFirstSeen.size()))
        << "Feature count mismatch for col " << c;

    // Validate id->string for every featureId, including 0.
    EXPECT_EQ(t.getColumnValue(c, 0u), "") << "Empty sentinel mismatch for col " << c;
    for (std::size_t i = 0; i < nonEmptyFirstSeen.size(); ++i) {
      const std::uint32_t featureId = static_cast<std::uint32_t>(i + 1);
      EXPECT_EQ(t.getColumnValue(c, featureId), nonEmptyFirstSeen[i])
          << "ColumnValue mismatch for col " << c << " featureId " << featureId;
    }

    // Out-of-range (maxId+1) should throw.
    EXPECT_THROW(t.getColumnValue(c, static_cast<std::uint32_t>(nonEmptyFirstSeen.size() + 1)), std::out_of_range)
        << "Expected out_of_range for col " << c;

    // Validate per-cell lookupMap matches our expected id.
    for (std::uint64_t r = 1; r <= 17; ++r) {
      const std::uint32_t gotId = t.lookupMap(r, c);
      const std::uint32_t wantId = expectedIds[static_cast<std::size_t>(r - 1)];
      ASSERT_EQ(gotId, wantId) << "lookupMap mismatch at row " << r << ", col " << c;
    }
  }

  // ---- Keep a couple of explicit spot-checks for readability ----
  EXPECT_EQ(t.getFeatureCount(0), 17u);
  EXPECT_EQ(t.getColumnValue(6, 1), "USA");
  EXPECT_EQ(t.getColumnValue(6, 2), "USAA");
}

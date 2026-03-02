#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <DataTable/DataTable.h>

namespace fs = std::filesystem;

static fs::path repoRoot() {
#if defined(DATATABLE_SOURCE_DIR)
  return fs::path(DATATABLE_SOURCE_DIR);
#else
  return fs::current_path();
#endif
}

TEST(DataSet1, GetValue_MatchesEveryCell) {
  const fs::path csvPath = repoRoot() / "tests" / "test_data_sets" / "data_set_1.csv";
  const fs::path outDir  = repoRoot() / "tests" / "test_data_sets" / "data_output" / "data_set_1_out";

  ASSERT_TRUE(fs::exists(csvPath)) << "CSV not found at: " << csvPath;

  std::error_code ec;
  fs::remove_all(outDir, ec);

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
      {"13", "Jessica", "Brown", "30", "Fort Worth", "TX", "USA"},
      {"14", "David", "Garcia", "27", "Columbus", "OH", "USA"},
      {"15", "Sarah", "Miller", "", "Charlotte", "NC", "USA"},
      {"16", "Daniel", "Martinez", "35", "San Francisco", "CA", "USA"},
      {"17", "Laura", "Anderson", "28", "Indianapolis", "IN", "USA"},
  };

  for (std::uint64_t r = 1; r <= 17; ++r) {
    for (std::uint64_t c = 0; c <= 6; ++c) {
      const std::string got = t.getValue(r, c);
      const std::string& want =
          expected[static_cast<std::size_t>(r - 1)][static_cast<std::size_t>(c)];
      ASSERT_EQ(got, want) << "Mismatch at row " << r << ", col " << c;
    }
  }
}

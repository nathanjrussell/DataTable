#include <gtest/gtest.h>

#include <DataTable/DataTable.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

using DataTableLib::DataTable;

namespace {

static std::vector<std::uint64_t> loadIndexOffsets(const std::filesystem::path& indexPath) {
  std::ifstream in(indexPath, std::ios::binary);
  if (!in) {
    throw std::runtime_error("Failed to open index file: " + indexPath.string());
  }

  std::vector<std::uint64_t> offsets;
  std::string line;

  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') continue;

    // trim CR
    if (!line.empty() && line.back() == '\r') line.pop_back();

    // skip header-ish lines
    if (line.find("offset") != std::string::npos || line.find("row") != std::string::npos) {
      continue;
    }

    // parse either "offset" or "row,offset"
    std::string_view v(line);
    const auto comma = v.find_last_of(',');
    std::string_view num = (comma == std::string_view::npos) ? v : v.substr(comma + 1);

    // trim spaces
    while (!num.empty() && (num.front() == ' ' || num.front() == '\t')) num.remove_prefix(1);
    while (!num.empty() && (num.back() == ' ' || num.back() == '\t')) num.remove_suffix(1);

    if (num.empty()) continue;

    offsets.push_back(static_cast<std::uint64_t>(std::stoull(std::string(num))));
  }

  return offsets;
}

static int runPythonGenerator(const std::filesystem::path& scriptPath,
                              const std::filesystem::path& outDir,
                              const std::string& baseName) {
  // Deprecated: tests should not regenerate datasets.
  (void)scriptPath;
  (void)outDir;
  (void)baseName;
  return 0;
}

}  // namespace

TEST(DataTable, Parse_RowOffsets_FromPythonIndex_Threads1To10) {
  const auto repoRoot = std::filesystem::path(DATATABLE_SOURCE_DIR);

  // Use pre-generated fixtures. This test must not regenerate them.
  const auto csvPath = repoRoot / "tests" / "python_generators" / "test_dataset_10k_40cols.csv";
  const auto indexPath = repoRoot / "tests" / "python_generators" / "test_dataset_10k_40cols.index";

  ASSERT_TRUE(std::filesystem::exists(csvPath)) << csvPath;
  ASSERT_TRUE(std::filesystem::exists(indexPath)) << indexPath;

  auto expectedOffsets = loadIndexOffsets(indexPath);
  ASSERT_FALSE(expectedOffsets.empty());

  // Row 0 is the header row starting at byte offset 0.
  ASSERT_EQ(expectedOffsets.front(), 0u);

  // Use a temp output dir so meta_data/ is isolated.
  const auto outDir = std::filesystem::temp_directory_path() / "DataTableTests_py_index_out";
  std::error_code ec;
  std::filesystem::remove_all(outDir, ec);
  std::filesystem::create_directories(outDir, ec);

  for (int threads = 1; threads <= 10; ++threads) {
    DataTable dt(csvPath.string(), outDir.string());
    dt.parse(threads);

    ASSERT_TRUE(dt.parseCompleted());
    ASSERT_EQ(dt.getRowCount(), static_cast<std::uint64_t>(expectedOffsets.size()));

    const std::uint64_t last = dt.getRowCount() - 1;
    const std::uint64_t mid = dt.getRowCount() / 2;

    ASSERT_EQ(dt.getRowOffset(0), expectedOffsets[0]) << "threads=" << threads;
    ASSERT_EQ(dt.getRowOffset(static_cast<int>(mid)), expectedOffsets[static_cast<std::size_t>(mid)])
        << "threads=" << threads;
    ASSERT_EQ(dt.getRowOffset(static_cast<int>(last)), expectedOffsets[static_cast<std::size_t>(last)])
        << "threads=" << threads;
  }

  std::filesystem::remove_all(outDir, ec);
}

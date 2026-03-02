#include <DataTable/DataTable.h>

#include <cstdint>
#include <filesystem>
#include <iostream>

using DataTableLib::DataTable;

int main() {
  // Hard-coded paths for a simple visual spot-check.
  // Update this path to point to your generated CSV if needed.
  const std::filesystem::path csvPath =
      std::filesystem::path("tests") / "test_data_sets" / "data_set_1.csv";
  // Output directory is only used for meta_data/.
  const std::filesystem::path outDir = std::filesystem::path("tests") / "test_data_sets" / "data_output" / "data_set_1_out";

  const int threads = 1;

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(threads);
  int rowCount = static_cast<int>(dt.getRowCount());
  int colCount = static_cast<int>(dt.getColumnCount());
  std::cout << "Row count: " << rowCount << "\n";
  std::cout << "Column count: " << colCount << "\n";

  // Hard-coded rows to print (row 0 is the header).
  const std::uint64_t rowsToPrint[] = {0, 1, 2, 3, 4, 10, 100, 1000, 5834};

  for (int row = 0; row < rowCount; ++row) {
      const std::uint64_t offset = dt.getRowOffset(row);
      std::cout << "Row " << row << " starts at byte offset " << offset << "\n";
  }

    for (int col = 0; col < colCount; ++col) {
        std::cout << "Column " << col << " header: " << dt.getColumnHeader(col) << "\n";
    }

    std::cout << "Lookup map for row 11, col 3: " << dt.lookupMap(11, 3) << "\n";
    std::cout << "Value for row 11, col 3: " << dt.getValue(11, 3) << "\n";
    std::cout<< "Value for row 10, col 2: " << dt.getValue(10, 2) << "\n";
    for (int row = 1; row < rowCount; ++row) {
        for (int col = 0; col < colCount; ++col) {
            std::cout << "Value for row " << row << ", col " << col << ": " << dt.getValue(row, col) << "\n";
        }
    }



  return 0;
}

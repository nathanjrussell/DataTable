#include <DataTable/DataTable.h>

#include <cstdint>
#include <filesystem>
#include <iostream>

using DataTableLib::DataTable;

int main() {

  std::string folderName = "small_fake_datasets";
  std::string dataSetName = "data_set_1";
  std::string fileName = dataSetName + ".csv";

  const std::filesystem::path csvPath =
      std::filesystem::path("tests") / "test_data_sets" / folderName / fileName;
  // Output directory is only used for meta_data/.
  const std::filesystem::path outDir = std::filesystem::path("tests") / "test_data_sets" / "data_output" / dataSetName;

  const int threads = 10;

  DataTable dt(csvPath.string(), outDir.string());
  dt.parse(threads);
  DataTable dt2;
  dt2.load(outDir.string());
  std::cout<<dt2.getColumnHeaderJson()<<std::endl;
  int rowCount = static_cast<int>(dt2.getRowCount());
  int colCount = static_cast<int>(dt2.getColumnCount());
  std::cout << "Row count: " << rowCount << "\n";
  std::cout << "Column count: " << colCount << "\n";



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
            std::cout << "Value for row " << row << ", col " << col << ": " << dt.getValue(row, col) << ": " << dt.lookupMap(row,col)<<"\n";
        }
    }



  return 0;
}

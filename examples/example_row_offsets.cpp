#include <DataTable/DataTable.h>

#include <iostream>

int main() {
  DataTableLib::DataTable dt;

  // This example historically demonstrated row offsets, but row offsets are no longer part of the public API.
  // Keep a tiny smoke example that can be extended.
  dt.setInputFilePath("example.csv");
  dt.setOutputDirectory(".");

  try {
    dt.parse(/*threads=*/1);
    std::cout << "Rows (incl header): " << dt.getRowCount() << "\n";
    std::cout << "Columns: " << dt.getColumnCount() << "\n";
  } catch (const std::exception& e) {
    std::cerr << "Parse failed: " << e.what() << "\n";
    return 1;
  }

  return 0;
}

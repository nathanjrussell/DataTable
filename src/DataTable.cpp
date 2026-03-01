#include <DataTable/DataTable.h>

#include <utility>

namespace DataTableLib {

DataTable::DataTable(std::string inputFilePath, std::string outputDirectory)
    : inputFilePath_(std::move(inputFilePath)),
      outputDirectory_(std::move(outputDirectory)) {}

void DataTable::setInputFilePath(const std::string& inputFilePath) {
  inputFilePath_ = inputFilePath;
}

void DataTable::setOutputDirectory(const std::string& outputDirectory) {
  outputDirectory_ = outputDirectory;
}

const std::string& DataTable::inputFilePath() const noexcept {
  return inputFilePath_;
}

const std::string& DataTable::outputDirectory() const noexcept {
  return outputDirectory_;
}

}  // namespace DataTableLib


#pragma once

#include <string>

namespace DataTableLib {

class DataTable {
public:
  DataTable() = default;
  DataTable(std::string inputFilePath, std::string outputDirectory);

  void setInputFilePath(const std::string& inputFilePath);
  void setOutputDirectory(const std::string& outputDirectory);

  const std::string& inputFilePath() const noexcept;
  const std::string& outputDirectory() const noexcept;

private:
  std::string inputFilePath_;
  std::string outputDirectory_;
};

} // namespace DataTableLib


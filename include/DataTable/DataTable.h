#pragma once

#include <cstdint>
#include <memory>
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

  // Parse the input CSV and write metadata into {outputDirectory}/meta_data/
  // After this completes successfully, parseCompleted() becomes true and metadata getters are enabled.
  void parse(int threads = 1);

  // No method below is valid unless parse() has completed successfully.
  std::uint64_t getColumnCount() const;
  std::string getColumnHeader(int col) const;
  std::string getColumnHeaderJson() const;

  // Row offsets are byte offsets into the original CSV file.
  // Row 0 is the header row, row 1 is the first data row.
  // getRowCount() includes the header row.
  std::uint64_t getRowCount() const;
  std::uint64_t getRowOffset(int row) const;

  bool parseCompleted() const noexcept { return parseCompleted_; }

private:
  void ensureParsed() const;
  void locateRowOffsets(int threads);

  std::string inputFilePath_;
  std::string outputDirectory_;

  bool parseCompleted_ = false;
  std::uint64_t columnCount_ = 0;

  std::uint64_t rowCount_ = 0;
  std::unique_ptr<std::uint64_t[]> rowOffsets_;
};

} // namespace DataTableLib


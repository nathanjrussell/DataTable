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

  // Overload that sets/overrides the column chunk size (number of columns processed per iteration).
  void parse(int threads, std::uint32_t chunkSize);

  // Load an already-parsed output directory (metadata + mapped_data) into a working state.
  // This does not load the full dataset into memory; it only reads metadata needed for lookup/getValue.
  void load(const std::string& directory);

  // No method below is valid unless parse() has completed successfully.
  std::uint64_t getColumnCount() const;
  std::string getColumnHeader(int col) const;
  std::uint64_t getColumnIndex(const std::string& header) const;
  std::string getColumnHeaderJson() const;

  // Row numbering: row 0 is the header row, row 1 is the first data row.
  // getRowCount() includes the header row.
  std::uint64_t getRowCount() const;

  // Return the mapped integer id for a given cell by reading the persisted, bit-packed mapped_data.
  // Row 0 is the header and is not supported.
  std::uint32_t lookupMap(std::uint64_t row, std::uint64_t col) const;

  // Return the original (normalized) string value for a cell by decoding the mapped id
  // and resolving id->string in the per-chunk map file.
  // Row 0 is the header and is not supported.
  std::string getValue(std::uint64_t row, std::uint64_t col) const;

  // Return the string corresponding to a per-column feature id (id->string dictionary lookup).
  // featureId is the integer produced by lookupMap() for cells in this column.
  // Row indexing isn't needed for this lookup.
  std::string getColumnValue(std::uint64_t col, std::uint32_t featureId) const;

  // Return the number of distinct non-empty mapped feature ids in a column.
  // Since ids are sequential per-column, this equals the maximum id in the column dictionary.
  // (Id 0 is reserved for empty/whitespace-only values.)
  std::uint32_t getFeatureCount(std::uint64_t col) const;

  bool parseCompleted() const noexcept { return parseCompleted_; }

private:
  void ensureParsed() const;
  void locateRowOffsets(int threads);
  void parseChunks(int threads);

  // Internal helper: retrieve a row start offset (from memory during parse, or from disk if needed).
  std::uint64_t rowOffset(std::uint64_t row) const;

  std::string inputFilePath_;
  std::string outputDirectory_;

  bool parseCompleted_ = false;
  std::uint64_t columnCount_ = 0;

  std::uint64_t rowCount_ = 0;
  std::unique_ptr<std::uint64_t[]> rowOffsets_;

  // Per-row cursor used during chunked parsing.
  std::unique_ptr<std::uint64_t[]> currentRowOffsets_;

  // Default number of columns per chunk.
  std::uint32_t chunkSize_ = 1000;
};

} // namespace DataTableLib

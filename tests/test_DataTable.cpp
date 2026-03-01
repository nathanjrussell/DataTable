#include <gtest/gtest.h>

#include <DataTable/DataTable.h>

using DataTableLib::DataTable;

TEST(DataTable, DefaultConstructor_StoresEmptyPaths) {
  DataTable dt;
  EXPECT_TRUE(dt.inputFilePath().empty());
  EXPECT_TRUE(dt.outputDirectory().empty());
}

TEST(DataTable, Constructor_SetsPaths) {
  DataTable dt("input.csv", "out_dir");
  EXPECT_EQ(dt.inputFilePath(), "input.csv");
  EXPECT_EQ(dt.outputDirectory(), "out_dir");
}

TEST(DataTable, Setters_UpdatePaths) {
  DataTable dt;
  dt.setInputFilePath("a.csv");
  dt.setOutputDirectory("cache");
  EXPECT_EQ(dt.inputFilePath(), "a.csv");
  EXPECT_EQ(dt.outputDirectory(), "cache");
}


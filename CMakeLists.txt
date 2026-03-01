cmake_minimum_required(VERSION 3.20)

project(DataTable VERSION 0.1.0 LANGUAGES CXX)

option(DATATABLE_BUILD_TESTS "Build DataTable unit tests" ON)

add_library(DataTable)
add_library(DataTable::DataTable ALIAS DataTable)

target_sources(DataTable
  PRIVATE
    src/DataTable.cpp
)

target_include_directories(DataTable
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_compile_features(DataTable PUBLIC cxx_std_17)

if(DATATABLE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()


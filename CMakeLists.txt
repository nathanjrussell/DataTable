cmake_minimum_required(VERSION 3.20)

project(DataTable VERSION 0.1.0 LANGUAGES CXX)

include(FetchContent)

option(DATATABLE_BUILD_TESTS "Build DataTable unit tests" ON)
option(DATATABLE_BUILD_EXAMPLES "Build DataTable example executables" ON)

# JSON (header-only)
FetchContent_Declare(
  nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(nlohmann_json)

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

target_link_libraries(DataTable
  PUBLIC
    nlohmann_json::nlohmann_json
)

target_compile_features(DataTable PUBLIC cxx_std_17)

if(DATATABLE_BUILD_EXAMPLES)
  add_executable(example_row_offsets examples/example_row_offsets.cpp)
  target_link_libraries(example_row_offsets PRIVATE DataTable::DataTable)
  target_compile_features(example_row_offsets PRIVATE cxx_std_17)
endif()

if(DATATABLE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

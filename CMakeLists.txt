cmake_minimum_required(VERSION 3.20)
project(DataTable VERSION 1.1.0 LANGUAGES CXX)

# Ensure Release uses -O3 (GCC/Clang). Keep MSVC defaults.
if(NOT MSVC)
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "" FORCE)
  set(CMAKE_C_FLAGS_RELEASE   "-O3 -DNDEBUG" CACHE STRING "" FORCE)
endif()

include(FetchContent)

option(DATATABLE_BUILD_TESTS "Build DataTable unit tests" ON)
option(DATATABLE_BUILD_EXAMPLES "Build DataTable example executables" ON)

# ProgressBar-CPP integration (default OFF in this project)
option(DATATABLE_ENABLE_PROGRESSBAR "Enable ProgressBar-CPP integration" ON)

FetchContent_Declare(
        nlohmann_json
        URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(nlohmann_json)

FetchContent_Declare(
        ProgressBar
        GIT_REPOSITORY https://github.com/nathanjrussell/ProgressBar-CPP.git
        GIT_TAG v1.3.0
)
# Optional: avoid building ProgressBar examples when fetched
set(PROGRESSBAR_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(ProgressBar)

# HashLibrary integration
FetchContent_Declare(
        HashLibrary
        GIT_REPOSITORY https://github.com/nathanjrussell/HashLibrary.git
        GIT_TAG v1.2.0
)
FetchContent_MakeAvailable(HashLibrary)

add_library(DataTable)
add_library(DataTable::DataTable ALIAS DataTable)

target_sources(DataTable PRIVATE src/DataTable.cpp)

target_include_directories(DataTable PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_link_libraries(DataTable PUBLIC
        nlohmann_json::nlohmann_json
        ProgressBar::progressbar
        HashLibrary::sha256
)

target_compile_features(DataTable PUBLIC cxx_std_17)

# Define for any TU that includes `<progressbar/...>` via DataTable headers
target_compile_definitions(DataTable PUBLIC
        PROGRESSBAR_ENABLED=$<IF:$<BOOL:${DATATABLE_ENABLE_PROGRESSBAR}>,1,0>
)



if(DATATABLE_BUILD_EXAMPLES)
  add_executable(example_row_offsets examples/example_row_offsets.cpp)
  target_link_libraries(example_row_offsets PRIVATE DataTable::DataTable)
  target_compile_features(example_row_offsets PRIVATE cxx_std_17)
endif()

if(DATATABLE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

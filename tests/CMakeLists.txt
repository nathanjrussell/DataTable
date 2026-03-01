include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

# Avoid overriding parent project's compiler/linker settings on Windows.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

add_executable(DataTableTests
  test_DataTable.cpp
)

target_link_libraries(DataTableTests
  PRIVATE
    DataTable::DataTable
    GTest::gtest_main
)

target_compile_features(DataTableTests PRIVATE cxx_std_17)

include(GoogleTest)
gtest_discover_tests(DataTableTests)

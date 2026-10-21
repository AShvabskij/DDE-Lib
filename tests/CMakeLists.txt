add_executable(dde_tests
    test_serialization.cpp
)

target_link_libraries(dde_tests
    PRIVATE
    dde
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(dde_tests)
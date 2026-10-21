cmake_minimum_required(VERSION 3.16)
project(DDE-Lib LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Werror)

add_library(dde
    src/serialization.cpp
    # src/shm_transport.cpp
)

target_include_directories(dde PUBLIC include)

# enable_testing()
# add_subdirectory(tests)
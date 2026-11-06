cmake_minimum_required(VERSION 3.16)
project(DDE-Lib LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Werror)

set(DDE_SOURCES 
    src/serialization.cpp
    src/in_process_transport.cpp
)

if (UNIX) 
  list(APPEND DDE_SOURCES src/shm_transport.cpp)
endif()

add_library(dde ${DDE_SOURCES})

target_include_directories(dde PUBLIC include)
find_package(Threads REQUIRED)
target_link_libraries(dde PUBLIC Threads::Threads)

if (UNIX)
  add_executable(dde_lower demo/dde_lower.cpp)
  add_executable(dde_upper demo/dde_upper.cpp)

  # привязываем библиотеку dde
  target_link_libraries(dde_lower PRIVATE dde)
  target_link_libraries(dde_upper PRIVATE dde)
endif()

# Подключаем GoogleTest
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)


enable_testing()
add_subdirectory(tests)
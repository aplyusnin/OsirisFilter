cmake_minimum_required(VERSION 3.26)
project(OsirisFilterBenchmark)

set(CMAKE_CXX_STANDARD 23)

add_executable(${PROJECT_NAME} bench_utils.cpp bench.cpp)
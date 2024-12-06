cmake_minimum_required(VERSION 3.30)
project(wal)

set(CMAKE_CXX_STANDARD 20)

add_executable(wal main.cpp
        wal.cpp
        wal.h
        os.cpp
        os.h)

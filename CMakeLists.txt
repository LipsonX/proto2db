# ========================================
# Created by Lipson on 24-4-1.
# Email to LipsonChan@yahoo.com
# Copyright (c) 2024 Lipson. All rights reserved.
# version 1.0
# ========================================

cmake_minimum_required(VERSION 3.0)
project(proto2db)

set(CMAKE_CXX_STANDARD 20)

include(protobuild.cmake)
add_protolib(protos ${CMAKE_CURRENT_SOURCE_DIR}/proto ${CMAKE_CURRENT_SOURCE_DIR}/proto.out "LD_LIBRARY_PATH=." protoc)

add_executable(protodb main.cpp)
target_link_libraries(protodb sqlite3 protos)

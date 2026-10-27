cmake_minimum_required(VERSION 3.28)
project(exchange-platform LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---- Matching engine ----
include(FetchContent)
FetchContent_Declare(
    matching_engine
    GIT_REPOSITORY https://github.com/smhussa22/matching-engine.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(matching_engine)

# ---- External libraries (installed via apt) ----
# sudo apt install libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc
#                  librdkafka-dev libhiredis-dev libpqxx-dev
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RdKafkaCpp REQUIRED IMPORTED_TARGET rdkafka++)
pkg_check_modules(Hiredis REQUIRED IMPORTED_TARGET hiredis)

# ---- Proto codegen ----
# calls protoc directly so import path resolution is consistent across all files.
# all generated headers land flat in CMAKE_CURRENT_BINARY_DIR so services include
# them as "orders.pb.h", "events.pb.h", etc.
get_target_property(grpc_cpp_plugin gRPC::grpc_cpp_plugin LOCATION)

file(GLOB PROTO_FILES "${CMAKE_SOURCE_DIR}/proto/*.proto")

set(PROTO_SRCS)
set(PROTO_HDRS)

foreach(proto_file ${PROTO_FILES})
    get_filename_component(proto_name ${proto_file} NAME_WE)

    set(pb_cc  "${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.cc")
    set(pb_h   "${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.pb.h")
    set(grpc_cc "${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.grpc.pb.cc")
    set(grpc_h  "${CMAKE_CURRENT_BINARY_DIR}/${proto_name}.grpc.pb.h")

    add_custom_command(
        OUTPUT ${pb_cc} ${pb_h} ${grpc_cc} ${grpc_h}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
            --plugin=protoc-gen-grpc=${grpc_cpp_plugin}
            -I${CMAKE_SOURCE_DIR}/proto
            ${proto_file}
        DEPENDS ${proto_file}
        COMMENT "Compiling proto: ${proto_name}.proto"
    )

    list(APPEND PROTO_SRCS ${pb_cc} ${grpc_cc})
    list(APPEND PROTO_HDRS ${pb_h}  ${grpc_h})
endforeach()

add_library(exchange_proto STATIC ${PROTO_SRCS})
target_include_directories(exchange_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(exchange_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

# ---- Services ----
add_subdirectory(services/gateway)
add_subdirectory(services/engine-host)

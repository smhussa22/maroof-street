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
find_package(RdKafka CONFIG REQUIRED)
find_package(hiredis CONFIG REQUIRED)

# ---- Proto codegen ----
# compiles all .proto files into C++ and gRPC stubs, collected into one library
# every service that needs generated types links exchange_proto
file(GLOB PROTO_FILES "${CMAKE_SOURCE_DIR}/proto/*.proto")

add_library(exchange_proto STATIC)
target_include_directories(exchange_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(exchange_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

get_target_property(grpc_cpp_plugin gRPC::grpc_cpp_plugin LOCATION)

protobuf_generate(
    TARGET exchange_proto
    PROTOS ${PROTO_FILES}
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}/proto
    PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
)

protobuf_generate(
    TARGET exchange_proto
    PROTOS ${PROTO_FILES}
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin}"
    IMPORT_DIRS ${CMAKE_SOURCE_DIR}/proto
    PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
)

# ---- Services ----
add_subdirectory(services/gateway)
add_subdirectory(services/engine-host)

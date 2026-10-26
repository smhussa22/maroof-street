cmake_minimum_required(VERSION 3.28)
project(exchange-platform LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(
    matching_engine
    GIT_REPOSITORY https://github.com/smhussa22/matching-engine.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(matching_engine)


# ---- External libraries (installed via apt) ----
find_package(gRPC CONFIG REQUIRED) # target: gRPC::grpc++
find_package(protobuf CONFIG REQUIRED) # target: protobuf::libprotobuf
find_package(RdKafka CONFIG REQUIRED) # target: RdKafka::rdkafka++
find_package(hiredis CONFIG REQUIRED) # target: hiredis::hiredis

# ---- Services ----
add_subdirectory(services/gateway)
add_subdirectory(services/engine-host)

cmake_minimum_required(VERSION 3.29)
project(relational_engine)

include_directories($ENV{LIBRESSL_INCLUDE_PATH})

add_library(relational_engine SHARED library.cpp)

target_link_libraries(relational_engine
        $ENV{LIBRESSL_LIB_PATH}/libssl.$ENV{OS_LIB_EXTENSION}
        $ENV{LIBRESSL_LIB_PATH}/libcrypto.$ENV{OS_LIB_EXTENSION}
)
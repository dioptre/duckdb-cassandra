cmake_minimum_required(VERSION 3.5...3.29)

if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)
endif()

# Set extension name here
set(TARGET_NAME cassandra)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Build DataStax C++ driver statically for bulletproof extension
set(DATASTAX_BUILD_DIR ${CMAKE_BINARY_DIR}/datastax-build)
set(DATASTAX_INSTALL_DIR ${CMAKE_BINARY_DIR}/datastax-install)

message("Building DataStax C++ driver statically...")

# Configure DataStax driver
execute_process(
    COMMAND ${CMAKE_COMMAND}
        -DCASS_BUILD_STATIC=ON
        -DCASS_BUILD_SHARED=OFF
        -DCASS_BUILD_EXAMPLES=OFF
        -DCASS_BUILD_TESTS=OFF
        -DCASS_BUILD_INTEGRATION_TESTS=OFF
        -DCASS_BUILD_UNIT_TESTS=OFF
        -DCASS_USE_OPENSSL=ON
        -DCASS_USE_ZLIB=ON
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${DATASTAX_INSTALL_DIR}
        -S ${CMAKE_CURRENT_SOURCE_DIR}/external/datastax-cpp-driver
        -B ${DATASTAX_BUILD_DIR}
    RESULT_VARIABLE DATASTAX_CONFIG_RESULT
)

if(NOT DATASTAX_CONFIG_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to configure DataStax C++ driver")
endif()

# Build and install DataStax driver
execute_process(
    COMMAND ${CMAKE_COMMAND} --build ${DATASTAX_BUILD_DIR} --config ${CMAKE_BUILD_TYPE} --target install
    RESULT_VARIABLE DATASTAX_BUILD_RESULT
)

if(NOT DATASTAX_BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to build DataStax C++ driver")
endif()

message("DataStax C++ driver built successfully")

include_directories(src/include)
include_directories(${DATASTAX_INSTALL_DIR}/include)

set(EXTENSION_SOURCES
    src/cassandra_extension.cpp
    src/cassandra_client.cpp
    src/cassandra_scan.cpp
    src/cassandra_settings.cpp
    src/cassandra_types.cpp
)

# Define both static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link libraries to the extensions
function(link_libraries_to_extension target_name)
    target_link_libraries(
        ${target_name}
        # Static DataStax C++ driver
        ${DATASTAX_INSTALL_DIR}/lib/libcassandra_static.a
        # Required system dependencies for DataStax driver
        /opt/homebrew/lib/libuv.dylib
        /opt/homebrew/lib/libssl.dylib
        /opt/homebrew/lib/libcrypto.dylib
        -lz
        # System libraries
        -lpthread
        -ldl
    )
endfunction()

link_libraries_to_extension(${EXTENSION_NAME})
link_libraries_to_extension(${LOADABLE_EXTENSION_NAME})

# Install targets
install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

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

# Configure DataStax driver with cross-compilation support
set(DATASTAX_CMAKE_ARGS
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
    -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
    -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
    -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
)

# Handle macOS cross-compilation
if(CMAKE_OSX_ARCHITECTURES)
    list(APPEND DATASTAX_CMAKE_ARGS -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES})
    message(STATUS "Building DataStax driver for macOS architecture: ${CMAKE_OSX_ARCHITECTURES}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND}
        ${DATASTAX_CMAKE_ARGS}
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

# Find vcpkg dependencies
find_package(OpenSSL REQUIRED)
find_package(libuv CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(src/include)
include_directories(${DATASTAX_INSTALL_DIR}/include)

# Determine DataStax library name based on platform
if(WIN32)
    set(DATASTAX_STATIC_LIB ${DATASTAX_INSTALL_DIR}/lib/cassandra_static.lib)
else()
    set(DATASTAX_STATIC_LIB ${DATASTAX_INSTALL_DIR}/lib/libcassandra_static.a)
endif()

set(EXTENSION_SOURCES
    src/cassandra_extension.cpp
    src/cassandra_client.cpp
    src/cassandra_scan.cpp
    src/cassandra_settings.cpp
    src/cassandra_types.cpp
    src/cassandra_utils.cpp
    src/storage/cassandra_catalog.cpp
    src/storage/cassandra_schema_entry.cpp
    src/storage/cassandra_table_entry.cpp
    src/storage/cassandra_transaction.cpp
)

# Define both static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link libraries to the extensions
function(link_libraries_to_extension target_name)
    target_link_libraries(
        ${target_name}
        # Static DataStax C++ driver
        ${DATASTAX_STATIC_LIB}
        # Required dependencies from vcpkg
        $<IF:$<TARGET_EXISTS:libuv::uv_a>,libuv::uv_a,libuv::uv>
        OpenSSL::SSL
        OpenSSL::Crypto
        ZLIB::ZLIB
        # Platform-specific system libraries
        $<$<PLATFORM_ID:Linux>:-lpthread>
        $<$<PLATFORM_ID:Linux>:-ldl>
        $<$<PLATFORM_ID:Windows>:ws2_32>
        $<$<PLATFORM_ID:Windows>:iphlpapi>
        $<$<PLATFORM_ID:Windows>:psapi>
        $<$<PLATFORM_ID:Windows>:crypt32>
        $<$<PLATFORM_ID:Windows>:userenv>
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

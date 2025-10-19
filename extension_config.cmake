# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
# Note: Cassandra extension supports Linux, macOS, and Windows
# Excluded platforms:
# - WASM: libuv doesn't support WASI, and no TCP sockets in browsers
if(NOT "${DUCKDB_EXPLICIT_PLATFORM}" MATCHES "wasm")
    duckdb_extension_load(cassandra
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
    )
else()
    message(STATUS "Skipping cassandra extension - WASM not supported (no libuv WASI support)")
endif()

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
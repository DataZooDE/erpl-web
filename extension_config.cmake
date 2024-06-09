if(CMAKE_BUILD_TYPE STREQUAL "Release")
    duckdb_extension_load(erpl_web
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        DONT_LINK)
    set(BUILD_UNITTESTS OFF)
else()
    duckdb_extension_load(erpl_web
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS)
    set(BUILD_UNITTESTS ON)
endif()



duckdb_extension_load(json)
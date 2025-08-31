Directory structure:
└── duckdb-duckdb-teradata/
    ├── README.md
    ├── CMakeLists.txt
    ├── extension_config.cmake
    ├── LICENSE
    ├── Makefile
    ├── vcpkg.json
    ├── .clang-format
    ├── .clang-tidy
    ├── src/
    │   ├── teradata_catalog.cpp
    │   ├── teradata_catalog.hpp
    │   ├── teradata_catalog_set.cpp
    │   ├── teradata_catalog_set.hpp
    │   ├── teradata_clear_cache.cpp
    │   ├── teradata_clear_cache.hpp
    │   ├── teradata_column_reader.cpp
    │   ├── teradata_column_reader.hpp
    │   ├── teradata_column_writer.cpp
    │   ├── teradata_column_writer.hpp
    │   ├── teradata_common.cpp
    │   ├── teradata_common.hpp
    │   ├── teradata_connection.cpp
    │   ├── teradata_connection.hpp
    │   ├── teradata_delete_update.cpp
    │   ├── teradata_delete_update.hpp
    │   ├── teradata_execute.cpp
    │   ├── teradata_execute.hpp
    │   ├── teradata_extension.cpp
    │   ├── teradata_extension.hpp
    │   ├── teradata_filter.cpp
    │   ├── teradata_filter.hpp
    │   ├── teradata_index.cpp
    │   ├── teradata_index.hpp
    │   ├── teradata_index_entry.cpp
    │   ├── teradata_index_entry.hpp
    │   ├── teradata_index_set.cpp
    │   ├── teradata_index_set.hpp
    │   ├── teradata_insert.cpp
    │   ├── teradata_insert.hpp
    │   ├── teradata_query.cpp
    │   ├── teradata_query.hpp
    │   ├── teradata_request.cpp
    │   ├── teradata_request.hpp
    │   ├── teradata_result.hpp
    │   ├── teradata_schema_entry.cpp
    │   ├── teradata_schema_entry.hpp
    │   ├── teradata_schema_set.cpp
    │   ├── teradata_schema_set.hpp
    │   ├── teradata_secret.cpp
    │   ├── teradata_secret.hpp
    │   ├── teradata_storage.cpp
    │   ├── teradata_storage.hpp
    │   ├── teradata_table_entry.cpp
    │   ├── teradata_table_entry.hpp
    │   ├── teradata_table_set.cpp
    │   ├── teradata_table_set.hpp
    │   ├── teradata_transaction.cpp
    │   ├── teradata_transaction.hpp
    │   ├── teradata_transaction_manager.cpp
    │   ├── teradata_transcation_manager.hpp
    │   ├── teradata_type.cpp
    │   ├── teradata_type.hpp
    │   ├── teradata/
    │   │   ├── coperr.h
    │   │   ├── coptypes.h
    │   │   ├── dbcarea.h
    │   │   └── parcel.h
    │   └── util/
    │       └── binary_reader.hpp
    ├── test/
    │   └── sql/
    │       └── teradata/
    │           ├── basic.test
    │           ├── ctas.test
    │           ├── filter_pushdown.test
    │           ├── index.test
    │           ├── primary_key.test
    │           ├── td_connect.test
    │           ├── td_query.test
    │           ├── teradata_delete.test
    │           ├── teradata_secret.test
    │           ├── teradata_update.test
    │           └── types/
    │               ├── binary.test
    │               ├── character.test
    │               ├── date.test
    │               ├── decimal.test
    │               ├── float.test
    │               ├── integer.test
    │               ├── time.test
    │               ├── timestamp.test
    │               ├── timestamptz.test
    │               └── timetz.test
    └── .github/
        └── workflows/
            ├── Linux.yml
            └── MainDistributionPipeline.yml

================================================
FILE: README.md
================================================
# duckdb-teradata

This is a DuckDB extension for connecting to and "attach":ing Teradata databases as if they were part of the DuckDB catalog. It allows you to manipulate tables, query data, and execute raw SQL commands directly on the Teradata database using DuckDB, by either pushing part of queries down into Teradata or by pulling data up into DuckDB for further processing.

**DuckDB Labs gratefully acknowledges support from the Teradata Corporation for the creation of this DuckDB extension.**

## Table of Contents
<!-- TOC -->
* [Usage](#usage)
  * [Attaching to Teradata](#attaching-to-teradata)
    * [Using Secrets](#using-secrets)
    * [Parameter Precedence](#parameter-precedence)
  * [Querying data](#querying-data)
    * [Sending raw SQL queries to Teradata](#sending-raw-sql-queries-to-teradata)
    * [Executing raw SQL commands](#executing-raw-sql-commands)
  * [Type mapping](#type-mapping)
  * [Configuration Options](#configuration-options)
* [Building the Extension](#building-the-extension)
<!-- TOC -->

# Usage

This extension currently requires the [Teradata Tools and Utilities](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Database-Introduction/Vantage-and-Tools/Teradata-Tools-and-Utilities) dynamic libraries to be installed on your machine. You can download them at the following links for [windows](https://downloads.teradata.com/download/database/teradata-tools-and-utilities-13-10), [macos](https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-mac-osx-installation-package), and [linux](https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-linux-installation-package-0). In the future we hope to distribute this extension with the required libraries statically linked to remove this requirement.


## Attaching to Teradata

Once the extension is loaded, you can attach DuckDB to a teradata system by executing the following SQL:

```sql
ATTACH
'{logon string}' as {database name} (TYPE TERADATA);
```

This will mount the database corresponding to the username specified in the terdata "logon string" (on the form "
host/username,password"). To attach to a specific teradata database, provide the optional `DATABASE '{name}'` parameter
to the `ATTACH` command:

```sql
ATTACH
'{logon string}' as {database name} (TYPE TERADATA, DATABASE '{teradata database name}');
```

Here's a complete example connecting to a teradata instance running on localhost as the `dbc` user (with password `dbc`)
and attaching the database `my_db` as `td` from within duckdb:

```sql
ATTACH
'127.0.0.1/dbc,dbc' as td (TYPE TERADATA, DATABASE 'my_db');
```

Additionally, parts of the logon string can be omitted from the string itself and instead given as parameters to the
`ATTACH` command, e.g. `HOST`, `USER`, `PASSWORD`, with the parts present in the logon string taking priority.

The following table illustrates all available `ATTACH` options when attaching to a `TYPE TERADATA` database:

| Option        | Description                                                                                                                             |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `HOST`        | The host to connect to, e.g. `127.0.0.1` or `localhost`.                                                                                |
| `USER`        | The username to connect with, e.g. `dbc`.                                                                                               |
| `PASSWORD`    | The password to use, e.g. `dbc`.                                                                                                        |
| `DATABASE`    | The teradata database to attach, e.g. `my_db`. This is optional and defaults to the user database.                                      |
| `BUFFER_SIZE` | The size of the response buffer used to fetch data from Teradata. This can be used to tune performance. Defaults to 1MiB (1024 * 1024). | 

### Using Secrets

This extension also supports handling Teradata credentials through
DuckDBs [Secret Management](https://duckdb.org/docs/stable/configuration/secrets_manager.html) system. You can create a
temporary in-memory Teradata secret using the `CREATE SECRET` command, e.g.:

```sql
CREATE
SECRET my_secret (
	TYPE TERADATA,
	HOST '127.0.0.1',
    USER '{teradata username}',
	DATABASE '{teradata database name}',
	PASSWORD '{teradata password}'
);
```

> [!WARNING]
> You can also create a persistent secret using the `CREATE PERSISTENT SECRET` command instead, but
> note that persistent secrets in DuckDB are stored in an unencrypted binary format on disk.

You can then use the secret to provide the connection credentials when attaching to a Teradata database by using the
`ATTACH` command together with the `SECRET` keyword:

```sql
ATTACH
'' as td (TYPE TERADATA, SECRET 'my_secret');
```

### Parameter Precedence

When a secret is used, you do not need to provide a logon string, or any of the `HOST`, `USER`, `PASSWORD`, or
`DATABASE` parameters in the `ATTACH` command, as all information is gathered from the secret instead.

However, if you want to override any of the connection parameters, you can still do so by providing them in logon string
or the `ATTACH` command. Similarly, you can also exclude any of the parameters when creating the secret, in which case
they will have to be provided in the logon string or the `ATTACH` command.

Credential parts passed through the logon string takes precedence over those defined in the `ATTACH` command, and those
defined in the `ATTACH` command takes precedence over those defined in the secret.

## Querying data

Once a teradata database is attached, you can query data from its tables using standard SQL queries, e.g.:

```sql
-- Attach a teradata database using a secret
ATTACH
'' as td (TYPE TERADATA, SECRET my_secret);
   
-- Query data from a table in the teradata database
SELECT * FROM td.my_table WHERE id = 42;
```

Most standard SQL queries are supported, including `SELECT`, `INSERT`, `UPDATE`, and `DELETE`. These also support filter-pushdown for simple predicates, allowing you to filter data directly on the teradata side before it is returned to DuckDB.

However, some Teradata-specific features may not be fully supported, and not all catalog operations are currently implemented either.
Therefore, you may sometime want to send and execute a raw SQL string directly to teradata, using the `teradata_query`
function.

### Sending raw SQL queries to Teradata

The `teradata_query` function can be used to send raw SQL queries directly to the teradata database. This is useful for
executing queries that may not be fully supported by DuckDB's SQL parser or for using Teradata-specific features.

The `teradata_query` function takes two parameters:

- The name of the attached teradata database (e.g. `td`).
- The raw SQL query string to execute on the teradata database.

Example usage, executing a raw SQL query to select data from a table in the teradata database, instead of going through
DuckDB's SQL parser:

```sql
SELECT teradata_query('td', 'SEL * FROM my_table WHERE id = 42');
```

For convenience, the teradata extension will also automatically register a database-scoped macro in each attached
teradata database called `query()`, which can be used to execute raw SQL queries in a more concise way. The following
example is equivalent to the previous one, but uses the `query()` macro instead:

```sql
SELECT td.query('SEL * FROM my_table WHERE id = 42');
```

### Executing raw SQL commands

In addition to querying data, you can also execute raw SQL commands using the `teradata_execute` function. This is
useful for executing commands that do not return any results, such as `CREATE TABLE`, `DROP TABLE`, or other DDL
commands not currently implemented in the DuckDB Teradata extension.

The `teradata_execute` function works similarly to `teradata_query`, but it does not return any results. Instead, it
simply executes the command on the teradata database. It similarly takes two parameters:

- The name of the attached teradata database (e.g. `td`).
- The raw SQL command string to execute on the teradata database.

The following example shows how to use `teradata_execute` to create a new table in the teradata database, instead of
using the DuckDB `CREATE TABLE` statement:

```sql
SELECT teradata_execute('td', 'CREATE TABLE my_table (id INT, value INT)');
```

## Type mapping

The DuckDB Teradata extension attempts to automatically map Teradata data types to DuckDB data types as closely as possible when
querying data from Teradata tables or inserting from DuckDB. The following tables shows the default type mapping from Teradata types to DuckDB
types, and vice-versa. 

__Teradata -> DuckDB Type Mapping__

| Teradata Type  | DuckDB Type    | Notes                                                                   |
|----------------|----------------|-------------------------------------------------------------------------|
| `BYTEINT`      | `TINYINT`      |                                                                         |
| `SMALLINT`     | `SMALLINT`     |                                                                         |
| `INTEGER`      | `INTEGER`      |                                                                         |
| `BIGINT`       | `BIGINT`       |                                                                         |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Always defaults to returning decimal precision 38.                      |
| `FLOAT`        | `DOUBLE`       | Teradata `FLOAT`s are 8 bytes (like DuckDB `DOUBLE`)                    |
| `VARCHAR(n)`   | `VARCHAR`      |                                                                         |
| `CHAR(n)`      | `VARCHAR`      |                                                                         |
| `CLOB(n)`      | `VARCHAR`      |                                                                         |
| `BYTE(n)`      | `BLOB`         |                                                                         |
| `BLOB(n)`      | `BLOB`         |                                                                         |
| `VARBYTE(n)`   | `BLOB`         |                                                                         |
| `DATE`         | `DATE`         |                                                                         |
| `TIMESTAMP`    | `TIMESTAMP_*`  | Mapped to the appropriate DuckDB timestamp type depending on precision. |
| `TIME(n)`      | `TIME`         | DuckDB rounds to microsecond precision                                  |
| `TIMESTAMP_TZ` | `TIMESTAMP_TZ` |                                                                         |
| `TIME_TZ`      | `TIME_TZ`      |                                                                         |
| `INTERVAL_*`   | `INTERVAL`     | Attempts to map all variants to DuckDBs single `INTERVAL` type.         |

__DuckDB -> Teradata Type Mapping__

| DuckDB Type      | Teradata Type     | Notes                                                    |
|------------------|-------------------|----------------------------------------------------------|
| `BOOLEAN`        | `BYTEINT`         |                                                          |
| `TINYINT`        | `BYTEINT`         |                                                          |
| `SMALLINT`       | `SMALLINT`        |                                                          |
| `INTEGER`        | `INTEGER`         |                                                          |
| `BIGINT`         | `BIGINT`          |                                                          |
| `DOUBLE`         | `FLOAT`           | Teradata `FLOAT`s are 8 bytes (like DuckDB `DOUBLE`)     |
| `DECIMAL(p,s)`   | `DECIMAL(p,s)`    |                                                          |
| `VARCHAR`        | `VARCHAR(64000)`  | DuckDB `VARCHAR` is mapped to Teradata `VARCHAR(64000)`. |
| `BLOB`           | `BLOB(64000)`     | DuckDB `BLOB` is mapped to Teradata `BLOB(64000)`        |
| `DATE`           | `DATE`            |                                                          |
| `TIME`           | `TIME(6)`         | DuckDB `TIME` is in microsecond precision                |
| `TIME_TZ`        | `TIME_TZ(6)`      | DuckDB `TIME_TZ` is in microsecond precision             |
| `TIMESTAMP`      | `TIMESTAMP(6)`    | DuckDB `TIMESTAMP` is in microsecond precision           |
| `TIMESTAMP_TZ`   | `TIMESTAMP_TZ(6)` | DuckDB `TIMESTAMP_TZ` is in microsecond precision        |
| `TIMESTAMP_NS`   | `TIMESTAMP(9)`    |                                                          |
| `TIMESTAMP_MS`   | `TIMESTAMP(3)`    |                                                          |
| `TIMESTAMP_SEC ` | `TIMESTAMP(0)`    |                                                          |

Because DuckDB does not support fixed-size `VARCHAR` or `BLOB` types, and Teradata does not support truly variable size types either, `VARCHAR` and `BLOB` are always mapped to the maximum size of `64000` bytes when creating or inserting into Teradata tables from within DuckDB.

Type conversion not listed in the above tables are not supported, and will result in an error if attempted.

## Configuration Options

The DuckDB Teradata extension supports the following configuration options that can be set using the `SET` command:

- `SET teradata_use_primary_index = <bool> (= true)`

This option controls whether DuckDB should automatically add a [primary index](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Database-Design/Primary-Index-Primary-AMP-Index-and-NoPI-Objects0) on the first column of tables created in Teradata.
This defaults to `true`, to follow the standard behavior in Teradata, but can be set to false if you want to create
Teradata tables from within DuckDB where the first column can contain duplicates.

# Building the Extension

The DuckDB Teradata extension is based on the [DuckDB Extension Template](https://github.com/duckdb/extension-template), but does not use `vcpkg` for dependency management.

To build the extension, you will need to have the following dependencies installed:
- A C++ compiler that supports C++11 or later.
- CMake 3.10 or later.
- Make or Ninja build system.

Additionally, you will need the Teradata CLIv2 Libraries installed on your system, which can be obtained from Teradata's official website. These need to be available in a location where the CMake build system can find them, e.g. by modifying the `TD_INCLUDE_DIR` and `TD_LIBRARY_DIR` variables in the `CMakeLists.txt` file to point to the correct include and library directories of the Teradata CLIv2 Libraries.

Once you have the dependencies installed, simply running `make debug` or `make release` in the root directory of the extension will build the extension in debug or release mode, and provide the following artifacts: 

- The resulting DuckDB CLI binary, `duckdb` in `build/debug/duckdb` or `build/release/duckdb`, which will have the Teradata extension statically linked built in. Can be used to quickly test the extension interactively. 

- The shared library of the extension itself, `teradata.duckdb_extension`, is placed in the `./build/debug/extension/teradata/` or `./build/release/extension/teradata` directory, depending on the build mode.

# Running tests

DuckDB provides a built-in test framework that can be used to run tests for the extension. However, the tests require a running Teradata instance to be available. Credentials to the Teradata instance needs to be provided through environment variables, that is: 
- `TD_LOGON`: the logon string to the Teradata instance, e.g. `127.0.0.1/dbc,dbc`
- `TD_DB`: the name of the Teradata database to use for testing, e.g. `my_db`.

You can set these environment variables in your shell before running the tests:

```bash
export TD_LOGON="127.0.0.1/dbc,dbc
export TD_DB="my_db"
```

After these enviornment variables are set, you can run the tests using the following command:

```bash
make test_debug
```

Or by invoking the DuckDB test runner directly:

```bash
./build/debug/test/unittest --test-dir . test/sql/teradata/
```

If you want to run the tests in release mode, use `make test` instead of `make test_debug`.



================================================
FILE: CMakeLists.txt
================================================
cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME teradata)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

# Set c++ standard to c++11
set(CMAKE_CXX_STANDARD 11)

project(${TARGET_NAME})

include_directories(src)
set(EXTENSION_SOURCES
    src/teradata_common.cpp
    src/teradata_extension.cpp
    src/teradata_secret.cpp
    src/teradata_storage.cpp
    src/teradata_catalog.cpp
    src/teradata_connection.cpp
    src/teradata_transaction_manager.cpp
    src/teradata_transaction.cpp
    src/teradata_query.cpp
    src/teradata_insert.cpp
    src/teradata_request.cpp
    src/teradata_schema_entry.cpp
    src/teradata_schema_set.cpp
    src/teradata_catalog_set.cpp
    src/teradata_table_set.cpp
    src/teradata_table_entry.cpp
    src/teradata_index.cpp
    src/teradata_index_set.cpp
    src/teradata_index_entry.cpp
    src/teradata_type.cpp
    src/teradata_execute.cpp
    src/teradata_clear_cache.cpp
    src/teradata_filter.cpp
    src/teradata_column_reader.cpp
    src/teradata_column_writer.cpp
    src/teradata_secret.cpp
    src/teradata_delete_update.cpp)

# Teradata on macos if(APPLE) set(TD_INCLUDE_DIR "/Library/Application\
# Support/teradata/client/20.00/include") endif()

# on linux (ubuntu has the lib64 suffix) if(UNIX AND NOT APPLE)
# set(TD_INCLUDE_DIR "/opt/teradata/client/20.00/include") endif()

# on windows if (WIN32) set(TD_INCLUDE_DIR "C:/Program
# Files/teradata/client/20.00/include") endif ()

set(TD_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/teradata")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Suppress warnings from Teradata headers
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                             "AppleClang")
  target_compile_options(${LOADABLE_EXTENSION_NAME}
                         PRIVATE -Wno-deprecated-register)
  target_compile_options(${EXTENSION_NAME} PRIVATE -Wno-deprecated-register)
endif()

target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE include
                                                              ${TD_INCLUDE_DIR})
target_include_directories(${EXTENSION_NAME} PRIVATE include ${TD_INCLUDE_DIR})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")



================================================
FILE: extension_config.cmake
================================================
# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(teradata
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)



================================================
FILE: LICENSE
================================================
Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONIN



================================================
FILE: Makefile
================================================
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=teradata
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

.PHONY: configure_ci

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile



================================================
FILE: vcpkg.json
================================================
{
  "dependencies": []
}



================================================
FILE: .clang-format
================================================
---
BasedOnStyle: LLVM
SortIncludes: false
TabWidth: 4
IndentWidth: 4
ColumnLimit: 120
AllowShortFunctionsOnASingleLine: false
---
UseTab: ForIndentation
DerivePointerAlignment: false
PointerAlignment: Right
AlignConsecutiveMacros: true
AlignTrailingComments: true
AllowAllArgumentsOnNextLine: true
AllowAllConstructorInitializersOnNextLine: true
AllowAllParametersOfDeclarationOnNextLine: true
AlignAfterOpenBracket: Align
SpaceBeforeCpp11BracedList: true
SpaceBeforeCtorInitializerColon: true
SpaceBeforeInheritanceColon: true
SpacesInAngles: false
SpacesInCStyleCastParentheses: false
SpacesInConditionalStatement: false
AllowShortLambdasOnASingleLine: Inline
AllowShortLoopsOnASingleLine: false
AlwaysBreakTemplateDeclarations: Yes
IncludeBlocks: Regroup
Language: Cpp
AccessModifierOffset: -4
---
Language: Java
SpaceAfterCStyleCast: true
---



================================================
FILE: .clang-tidy
================================================
Checks:          '-*,clang-diagnostic-*,bugprone-*,performance-*,google-explicit-constructor,google-build-using-namespace,google-runtime-int,misc-definitions-in-headers,modernize-use-nullptr,modernize-use-override,-bugprone-macro-parentheses,readability-braces-around-statements,-bugprone-branch-clone,readability-identifier-naming,hicpp-exception-baseclass,misc-throw-by-value-catch-by-reference,-bugprone-signed-char-misuse,-bugprone-misplaced-widening-cast,-bugprone-sizeof-expression,-bugprone-easily-swappable-parameters,google-global-names-in-headers,llvm-header-guard,misc-definitions-in-headers,modernize-use-emplace,modernize-use-bool-literals,-performance-inefficient-string-concatenation,-performance-no-int-to-ptr,readability-container-size-empty,cppcoreguidelines-pro-type-cstyle-cast,-llvm-header-guard,-performance-enum-size,cppcoreguidelines-pro-type-const-cast,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-interfaces-global-init,cppcoreguidelines-slicing,cppcoreguidelines-rvalue-reference-param-not-moved,cppcoreguidelines-virtual-class-destructor,-readability-identifier-naming,-bugprone-exception-escape,-bugprone-unused-local-non-trivial-variable,-bugprone-empty-catch'
WarningsAsErrors: '*'
HeaderFilterRegex: 'src/include/duckdb/.*'
FormatStyle:     none
CheckOptions:
  - key:             readability-identifier-naming.ClassCase
    value:           CamelCase
  - key:             readability-identifier-naming.EnumCase
    value:           CamelCase
  - key:             readability-identifier-naming.TypedefCase
    value:           lower_case
  - key:             readability-identifier-naming.TypedefSuffix
    value:           _t
  - key:             readability-identifier-naming.FunctionCase
    value:           CamelCase
  - key:             readability-identifier-naming.MemberCase
    value:           lower_case
  - key:             readability-identifier-naming.ParameterCase
    value:           lower_case
  - key:             readability-identifier-naming.ConstantCase
    value:           aNy_CasE
  - key:             readability-identifier-naming.ConstantParameterCase
    value:           lower_case
  - key:             readability-identifier-naming.NamespaceCase
    value:           lower_case
  - key:             readability-identifier-naming.MacroDefinitionCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.StaticConstantCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.ConstantMemberCase
    value:           aNy_CasE
  - key:             readability-identifier-naming.StaticVariableCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.ClassConstantCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.EnumConstantCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.ConstexprVariableCase
    value:           aNy_CasE
  - key:             readability-identifier-naming.StaticConstantCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.TemplateTemplateParameterCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.TypeTemplateParameterCase
    value:           UPPER_CASE
  - key:             readability-identifier-naming.VariableCase
    value:           lower_case
  - key:             modernize-use-emplace.SmartPointers
    value:           '::duckdb::shared_ptr;::duckdb::unique_ptr;::std::auto_ptr;::duckdb::weak_ptr'
  - key:             cppcoreguidelines-rvalue-reference-param-not-moved.IgnoreUnnamedParams
    value:           true




================================================
FILE: src/teradata_catalog.cpp
================================================
#include "teradata_catalog.hpp"
#include "teradata_connection.hpp"
#include "teradata_common.hpp"

#include "teradata_schema_set.hpp"
#include "teradata_schema_entry.hpp"
#include "teradata_table_entry.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Initialization
//----------------------------------------------------------------------------------------------------------------------

TeradataCatalog::TeradataCatalog(AttachedDatabase &db, const string &logon_string, const string &database_to_load, idx_t buffer_size)
    : Catalog(db), schemas(*this, database_to_load), default_schema(database_to_load), buffer_size(buffer_size) {
	conn = make_uniq<TeradataConnection>(logon_string, buffer_size);
	path = logon_string;

	// No empty default schema
	if (default_schema.empty()) {
		throw InvalidInputException("No default schema provided for TeradataCatalog!");
	}
}

TeradataCatalog::~TeradataCatalog() {
}

void TeradataCatalog::Initialize(bool load_builtin) {
}

TeradataConnection &TeradataCatalog::GetConnection() const {
	return *conn;
}

//----------------------------------------------------------------------------------------------------------------------
// Schema Management
//----------------------------------------------------------------------------------------------------------------------

optional_ptr<CatalogEntry> TeradataCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("TeradataCatalog::CreateSchema");
}

void TeradataCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	// Ok return a schema, with entries
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<TeradataSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> TeradataCatalog::LookupSchema(CatalogTransaction transaction,
                                                               const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA) {
		schema_name = default_schema;
	}

	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

void TeradataCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("TeradataCatalog::DropSchema");
}

void TeradataCatalog::ClearCache() {
	schemas.ClearEntries();
}

//----------------------------------------------------------------------------------------------------------------------
// Metadata
//----------------------------------------------------------------------------------------------------------------------

DatabaseSize TeradataCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("TeradataCatalog::GetDatabaseSize");
}

bool TeradataCatalog::InMemory() {
	return false;
}

string TeradataCatalog::GetCatalogType() {
	return "teradata";
}

string TeradataCatalog::GetDBPath() {
	return path;
}

} // namespace duckdb


================================================
FILE: src/teradata_catalog.hpp
================================================
#pragma once
#include "teradata_storage.hpp"
#include "teradata_connection.hpp"
#include "teradata_schema_set.hpp"

namespace duckdb {

class TeradataCatalog final : public Catalog {
public:
	explicit TeradataCatalog(AttachedDatabase &db, const string &logon_string, const string &databse_to_load, idx_t buffer_size);
	~TeradataCatalog() override;

public:
	// TODO: this should not be exposed like this. We should expose a pool instead?
	TeradataConnection &GetConnection() const;

public:
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetCatalogType() override;
	string GetDBPath() override;

	static bool IsTeradataScan(const string &name) {
		return name == "teradata_query" || name == "teradata_scan";
	}

	void ClearCache();

private:
	unique_ptr<TeradataConnection> conn;
	string path;

	// The set of schemas in this database
	TeradataSchemaSet schemas;
	string default_schema;

	// Teradata response buffer size
	idx_t buffer_size;
};

} // namespace duckdb


================================================
FILE: src/teradata_catalog_set.cpp
================================================
#include "teradata_catalog_set.hpp"
#include "teradata_schema_entry.hpp"
#include "teradata_transaction.hpp"
#include "teradata_connection.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {
TeradataCatalogSet::TeradataCatalogSet(Catalog &catalog) : catalog(catalog) {
}

void TeradataCatalogSet::TryLoadEntries(ClientContext &context) {
	lock_guard<mutex> lock(load_lock);
	if (is_loaded) {
		return;
	}
	LoadEntries(context);
	is_loaded = true;
}

void TeradataCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	TryLoadEntries(context);
	// TODO: lock this too
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> TeradataCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("TeradataCatalogSet::CreateEntry called with empty name");
	}

	entry_map.emplace(result->name, result->name);
	entries.emplace(result->name, std::move(entry));

	return result;
}

void TeradataCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {

		// Teradata doesnt support IF EXISTS, so check if the entry exists first
		const auto entry = GetEntry(context, info.name);
		if (!entry) {
			return;
		}
	}

	if (!info.schema.empty()) {
		drop_query += KeywordHelper::WriteQuoted(info.schema, '"') + ".";
	}
	drop_query += KeywordHelper::WriteQuoted(info.name, '"');
	if (info.cascade) {
		drop_query += "CASCADE";
	}

	// Execute the drop query
	const auto &transaction = TeradataTransaction::Get(context, catalog);
	auto &conn = transaction.GetConnection();

	conn.Execute(drop_query);

	// erase the entry from the catalog set
	lock_guard<mutex> l(entry_lock);
	entries.erase(info.name);
}

optional_ptr<CatalogEntry> TeradataCatalogSet::GetEntry(ClientContext &context, const string &name) {
	TryLoadEntries(context);
	{
		lock_guard<mutex> l(entry_lock);
		auto entry = entries.find(name);
		if (entry != entries.end()) {
			return entry->second.get();
		}
	}

	// entry not found
	// TODO: Try to reload the catalog!
	auto name_entry = entry_map.find(name);
	if (name_entry == entry_map.end()) {
		// no entry found
		return nullptr;
	}
	// try again with the entry we found in the case insensitive map
	auto entry = entries.find(name_entry->second);
	if (entry == entries.end()) {
		// still not found
		return nullptr;
	}

	return entry->second.get();
}

TeradataInSchemaSet::TeradataInSchemaSet(TeradataSchemaEntry &schema)
    : TeradataCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> TeradataInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	entry->internal = schema.internal;
	return TeradataCatalogSet::CreateEntry(std::move(entry));
}

void TeradataCatalogSet::ClearEntries() {
	entry_map.clear();
	entries.clear();
	is_loaded = false;
}

} // namespace duckdb


================================================
FILE: src/teradata_catalog_set.hpp
================================================
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class Catalog;
class ClientContext;
class CatalogEntry;
class TeradataSchemaEntry;
struct DropInfo;

//----------------------------------------------------------------------------------------------------------------------
// TeradataCatalogSet
//----------------------------------------------------------------------------------------------------------------------
// Base class for sets of catalog entries
// Takes care of caching/reloading the entries

class TeradataCatalogSet {
public:
	explicit TeradataCatalogSet(Catalog &catalog);
	virtual ~TeradataCatalogSet() = default;

	// Scan all entries in this set
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

	// Create an entry in the set
	virtual optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);

	// Drop an entry from the set
	void DropEntry(ClientContext &context, DropInfo &info);

	// Get entry by name
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);

	// Reset this set, clearing all cached entries
	void ClearEntries();

protected:
	// Needs to be implemented by subclasses: method to load the entries of this set
	virtual void LoadEntries(ClientContext &context) = 0;

	Catalog &catalog;
	unordered_map<string, unique_ptr<CatalogEntry>> entries = {};

	// Case insensitive entry map
	case_insensitive_map_t<string> entry_map = {};

private:
	void TryLoadEntries(ClientContext &context);
	mutex load_lock;
	mutex entry_lock;
	bool is_loaded = false;
};

//----------------------------------------------------------------------------------------------------------------------
// TeradataInSchemaSet
//----------------------------------------------------------------------------------------------------------------------
// Base class for sets of entries within a schema
class TeradataInSchemaSet : public TeradataCatalogSet {
public:
	explicit TeradataInSchemaSet(TeradataSchemaEntry &schema);

	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry) override;

protected:
	TeradataSchemaEntry &schema;
};

} // namespace duckdb


================================================
FILE: src/teradata_clear_cache.cpp
================================================
#include "teradata_clear_cache.hpp"
#include "teradata_catalog.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

class ClearCacheFunctionData final : public TableFunctionData {
public:
	bool finished = false;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ClearCacheFunctionData>();
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

void TeradataClearCacheFunction::Clear(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = db_ref.get();
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "teradata") {
			continue;
		}
		catalog.Cast<TeradataCatalog>().ClearCache();
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	TeradataClearCacheFunction::Clear(context);
	data.finished = true;
}

void TeradataClearCacheFunction::Register(DatabaseInstance &db) {
	TableFunction func("teradata_clear_cache", {}, ClearCacheFunction, ClearCacheBind);
	ExtensionUtil::RegisterFunction(db, func);
}

} // namespace duckdb



================================================
FILE: src/teradata_clear_cache.hpp
================================================
#pragma once

namespace duckdb {

class DatabaseInstance;
class ClientContext;

struct TeradataClearCacheFunction {
	static void Clear(ClientContext &context);
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb


================================================
FILE: src/teradata_column_reader.cpp
================================================
#include "teradata_column_reader.hpp"
#include "teradata_type.hpp"

#include "util/binary_reader.hpp"

#include <cmath>

namespace duckdb {

class TeradataVarcharReader final : public TeradataColumnReader {
public:
	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		const auto length = reader.Read<uint16_t>();
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
		} else {
			const auto src_ptr = reader.ReadBytes(length);
			const auto dst_ptr = FlatVector::GetData<string_t>(vec);
			dst_ptr[row_idx] = StringVector::AddString(vec, src_ptr, length);
		}
	}

private:
	// TODO: Deal with character encoding/conversion here
	vector<char> encoding_buffer;
};

class TeradataCharReader final : public TeradataColumnReader {
public:
	explicit TeradataCharReader(int32_t max_size_p) : max_size(max_size_p) {
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(max_size);
		} else {
			const auto src_ptr = reader.ReadBytes(max_size);
			const auto dst_ptr = FlatVector::GetData<string_t>(vec);
			dst_ptr[row_idx] = StringVector::AddString(vec, src_ptr, max_size);
		}
	}

private:
	idx_t max_size;

	// TODO: Deal with character encoding/conversion here
	vector<char> encoding_buffer;
};

class TeradataDateReader final : public TeradataColumnReader {
public:
	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
		} else {
			// Teradata stores dates in the following format
			// (YEAR - 1900) * 10000 + (MONTH * 100) + DAY
			const auto td_date = reader.Read<int32_t>();

			const auto years = td_date / 10000 + 1900;
			const auto months = (td_date % 10000) / 100;
			const auto days = td_date % 100;

			FlatVector::GetData<date_t>(vec)[row_idx] = Date::FromDate(years, months, days);
		}
	}
};

class TeradataByteReader final : public TeradataColumnReader {
public:
	explicit TeradataByteReader(int32_t max_size_p) : max_size(max_size_p) {
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		D_ASSERT(vec.GetType().id() == LogicalTypeId::BLOB);

		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(max_size);
		} else {
			const auto src_ptr = reader.ReadBytes(max_size);
			const auto dst_ptr = FlatVector::GetData<string_t>(vec);
			dst_ptr[row_idx] = StringVector::AddStringOrBlob(vec, src_ptr, max_size);
		}
	}

private:
	idx_t max_size;
};

class TeradataVarbyteReader final : public TeradataColumnReader {
public:
	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		D_ASSERT(vec.GetType().id() == LogicalTypeId::BLOB);

		const auto length = reader.Read<uint16_t>();
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
		} else {
			const auto src_ptr = reader.ReadBytes(length);
			const auto dst_ptr = FlatVector::GetData<string_t>(vec);
			dst_ptr[row_idx] = StringVector::AddStringOrBlob(vec, src_ptr, length);
		}
	}
};

template <class SRC, class DST = SRC>
class TeradataFixedSizeReader final : public TeradataColumnReader {
public:
	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(sizeof(SRC));
		} else {
			const auto src = reader.Read<SRC>();
			const auto dst = FlatVector::GetData<DST>(vec);
			dst[row_idx] = src;
		}
	}
};

class TeradataIntervalYearReader final : public TeradataColumnReader {
public:
	static constexpr auto BUFFER_SIZE = 6;

	idx_t char_size = 0;

	explicit TeradataIntervalYearReader(idx_t precision_p) : char_size(precision_p + 1) {
		D_ASSERT(precision_p < 5);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {

			char buffer[BUFFER_SIZE] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t years = 0;

			// Scan into years
			sscanf(buffer, "%d", &years);

			interval_t interval = {};
			interval.months = years * Interval::MONTHS_PER_YEAR;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalYearToMonthReader final : public TeradataColumnReader {
public:
	static constexpr auto BUFFER_SIZE = 6;

	idx_t char_size = 0;

	explicit TeradataIntervalYearToMonthReader(idx_t precision_p) : char_size(precision_p + 1) {
		D_ASSERT(precision_p < BUFFER_SIZE - 1);
	}
	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {

			char buffer[BUFFER_SIZE] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t years = 0;
			int32_t months = 0;

			// Scan into years and months
			sscanf(buffer, "%d-%d", &years, &months);

			interval_t interval = {};
			interval.months = years * Interval::MONTHS_PER_YEAR + months;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalMonthReader final : public TeradataColumnReader {
public:
	static constexpr auto BUFFER_SIZE = 6;
	idx_t char_size = 0;

	explicit TeradataIntervalMonthReader(idx_t precision_p) : char_size(precision_p + 1) {
		D_ASSERT(precision_p < BUFFER_SIZE - 1);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {

			char buffer[BUFFER_SIZE] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t months = 0;

			// Scan into months
			sscanf(buffer, "%d", &months);

			interval_t interval = {};
			interval.months = months;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalDayReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;

	explicit TeradataIntervalDayReader(idx_t precision_p) : char_size(precision_p + 1) {
		D_ASSERT(precision_p < 5);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {

			char buffer[6] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t days = 0;

			// Scan into days
			sscanf(buffer, "%d", &days);

			interval_t interval = {};
			interval.days = days;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalDayToHourReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;

	explicit TeradataIntervalDayToHourReader(idx_t precision_p) : char_size(precision_p + 1) {
		D_ASSERT(precision_p < 5);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {

			char buffer[6] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t days = 0;
			int32_t hours = 0;

			// Scan into days and hours
			sscanf(buffer, "%d %d", &days, &hours);

			// Convert all to micros
			const auto micros = hours * Interval::MICROS_PER_HOUR + days * Interval::MICROS_PER_DAY;
			const auto interval = Interval::FromMicro(micros);

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalDayToMinuteReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;

	explicit TeradataIntervalDayToMinuteReader(idx_t precision_p) : char_size(precision_p + 1) {
		D_ASSERT(precision_p < 5);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {

			char buffer[6] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t days = 0;
			int32_t hours = 0;
			int32_t minutes = 0;

			// Scan into days, hours and minutes
			sscanf(buffer, "%d %d:%d", &days, &hours, &minutes);

			const auto micros = hours * Interval::MICROS_PER_HOUR + days * Interval::MICROS_PER_DAY +
			                    minutes * Interval::MICROS_PER_MINUTE;
			const auto interval = Interval::FromMicro(micros);

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalDayToSecondReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;
	idx_t precision = 0;
	idx_t second_precision = 0;

	explicit TeradataIntervalDayToSecondReader(idx_t precision, idx_t second_precision)
	    : precision(precision), second_precision(second_precision) {
		D_ASSERT(precision < 5);
		D_ASSERT(second_precision < 7);
		if (second_precision != 0) {
			char_size = precision + second_precision + 11;
		} else {
			char_size = precision + 10;
		}
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[32] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t days = 0;
			int32_t hours = 0;
			int32_t minutes = 0;
			int32_t seconds = 0;
			int32_t fractional_seconds = 0;

			if (second_precision != 0) {
				// Scan into days, hours, minutes, seconds and fractional seconds
				sscanf(buffer, "%d %d:%d:%d.%d", &days, &hours, &minutes, &seconds, &fractional_seconds);
			} else {
				// Scan into days, hours, minutes and seconds
				sscanf(buffer, "%d %d:%d:%d", &days, &hours, &minutes, &seconds);
			}

			auto micros = hours * Interval::MICROS_PER_HOUR + days * Interval::MICROS_PER_DAY +
			              minutes * Interval::MICROS_PER_MINUTE + seconds * Interval::MICROS_PER_SEC;

			// Also add fractional seconds if present
			if (second_precision != 0) {
				micros += UnsafeNumericCast<int64_t>(fractional_seconds) * Interval::MICROS_PER_SEC /
				          static_cast<int64_t>(std::pow(10, second_precision));
			}

			const auto interval = Interval::FromMicro(micros);
			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalHourReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;

	explicit TeradataIntervalHourReader(idx_t precision) : char_size(precision + 1) {
		D_ASSERT(precision < 5);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[6] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t hours = 0;

			// Scan into hours
			sscanf(buffer, "%d", &hours);

			interval_t interval = {};
			interval.micros = hours * Interval::MICROS_PER_HOUR;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalHourToMinuteReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;

	explicit TeradataIntervalHourToMinuteReader(idx_t precision) : char_size(precision + 1) {
		D_ASSERT(precision < 5);
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[6] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t hours = 0;
			int32_t minutes = 0;

			// Scan into hours and minutes
			sscanf(buffer, "%d:%d", &hours, &minutes);

			interval_t interval = {};
			interval.micros = hours * Interval::MICROS_PER_HOUR + minutes * Interval::MICROS_PER_MINUTE;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalHourToSecondReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;
	idx_t precision = 0;
	idx_t second_precision = 0;

	explicit TeradataIntervalHourToSecondReader(idx_t precision, idx_t second_precision)
	    : precision(precision), second_precision(second_precision) {
		D_ASSERT(precision < 5);
		D_ASSERT(second_precision < 7);
		if (second_precision != 0) {
			char_size = precision + second_precision + 8;
		} else {
			char_size = precision + 7;
		}
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[32] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t hours = 0;
			int32_t minutes = 0;
			int32_t seconds = 0;
			int32_t fractional_seconds = 0;

			if (second_precision != 0) {
				// Scan into hours, minutes, seconds and fractional seconds
				sscanf(buffer, "%d:%d:%d.%d", &hours, &minutes, &seconds, &fractional_seconds);
			} else {
				// Scan into hours, minutes and seconds
				sscanf(buffer, "%d:%d:%d", &hours, &minutes, &seconds);
			}

			auto micros = hours * Interval::MICROS_PER_HOUR + minutes * Interval::MICROS_PER_MINUTE +
			              seconds * Interval::MICROS_PER_SEC;

			// Also add fractional seconds if present
			if (second_precision != 0) {
				micros += UnsafeNumericCast<int64_t>(fractional_seconds) * Interval::MICROS_PER_SEC /
				          static_cast<int64_t>(std::pow(10, second_precision));
			}

			const auto interval = Interval::FromMicro(micros);
			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalMinuteReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;
	explicit TeradataIntervalMinuteReader(idx_t precision) : char_size(precision + 1) {
		D_ASSERT(precision < 5);
	}
	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[6] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t minutes = 0;

			// Scan into minutes
			sscanf(buffer, "%d", &minutes);

			interval_t interval = {};
			interval.micros = minutes * Interval::MICROS_PER_MINUTE;

			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalMinuteToSecondReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;
	idx_t precision = 0;
	idx_t second_precision = 0;

	explicit TeradataIntervalMinuteToSecondReader(idx_t precision, idx_t second_precision)
	    : precision(precision), second_precision(second_precision) {
		D_ASSERT(precision < 5);
		D_ASSERT(second_precision < 7);
		if (second_precision != 0) {
			char_size = precision + second_precision + 4;
		} else {
			char_size = precision + 3;
		}
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[32] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t minutes = 0;
			int32_t seconds = 0;
			int32_t fractional_seconds = 0;

			if (second_precision != 0) {
				// Scan into minutes, seconds and fractional seconds
				sscanf(buffer, "%d:%d.%d", &minutes, &seconds, &fractional_seconds);
			} else {
				// Scan into minutes and seconds
				sscanf(buffer, "%d:%d", &minutes, &seconds);
			}

			auto micros = minutes * Interval::MICROS_PER_MINUTE + seconds * Interval::MICROS_PER_SEC;

			// Also add fractional seconds if present
			if (second_precision != 0) {
				micros += UnsafeNumericCast<int64_t>(fractional_seconds) * Interval::MICROS_PER_SEC /
				          static_cast<int64_t>(std::pow(10, second_precision));
			}

			const auto interval = Interval::FromMicro(micros);
			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

class TeradataIntervalSecondReader final : public TeradataColumnReader {
public:
	idx_t char_size = 0;
	idx_t precision = 0;
	idx_t second_precision = 0;

	explicit TeradataIntervalSecondReader(idx_t precision, idx_t second_precision)
	    : precision(precision), second_precision(second_precision) {
		D_ASSERT(precision < 5);
		if (second_precision != 0) {
			char_size = precision + second_precision + 2;
		} else {
			char_size = precision + 1;
		}
	}

	void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) override {
		if (is_null) {
			FlatVector::SetNull(vec, row_idx, true);
			reader.Skip(char_size);
		} else {
			char buffer[32] = {0};
			reader.ReadInto(buffer, char_size);

			int32_t seconds = 0;
			int32_t fractional_seconds = 0;

			if (second_precision != 0) {
				// Scan into seconds and fractional seconds
				sscanf(buffer, "%d.%d", &seconds, &fractional_seconds);
			} else {
				// Scan into seconds
				sscanf(buffer, "%d", &seconds);
			}

			auto micros = seconds * Interval::MICROS_PER_SEC;

			// Also add fractional seconds if present
			if (second_precision != 0) {
				micros += UnsafeNumericCast<int64_t>(fractional_seconds) * Interval::MICROS_PER_SEC /
				          static_cast<int64_t>(std::pow(10, second_precision));
			}

			const auto interval = Interval::FromMicro(micros);
			const auto dst = FlatVector::GetData<interval_t>(vec);
			dst[row_idx] = interval;
		}
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------------------------------------------------

unique_ptr<TeradataColumnReader> TeradataColumnReader::Make(const TeradataType &type) {
	switch (type.GetId()) {
	case TeradataTypeId::VARCHAR:
		return make_uniq<TeradataVarcharReader>();
	case TeradataTypeId::CHAR:
		return make_uniq<TeradataCharReader>(type.GetLength());
	case TeradataTypeId::BYTE:
		return make_uniq<TeradataByteReader>(type.GetLength());
	case TeradataTypeId::VARBYTE:
		return make_uniq<TeradataVarbyteReader>();
	case TeradataTypeId::DATE:
		return make_uniq<TeradataDateReader>();
	case TeradataTypeId::BYTEINT:
		return make_uniq<TeradataFixedSizeReader<int8_t>>();
	case TeradataTypeId::SMALLINT:
		return make_uniq<TeradataFixedSizeReader<int16_t>>();
	case TeradataTypeId::INTEGER:
		return make_uniq<TeradataFixedSizeReader<int32_t>>();
	case TeradataTypeId::BIGINT:
		return make_uniq<TeradataFixedSizeReader<int64_t>>();
	case TeradataTypeId::FLOAT:
		return make_uniq<TeradataFixedSizeReader<double>>();
	case TeradataTypeId::DECIMAL: {
		const auto width = type.GetWidth();
		if (width <= 2) {
			// In teradata, the small decimals are stored as 1 byte, compared to 2 bytes in duckdb
			return make_uniq<TeradataFixedSizeReader<int8_t, int16_t>>();
		}
		if (width <= 4) {
			return make_uniq<TeradataFixedSizeReader<int16_t>>();
		}
		if (width <= 9) {
			return make_uniq<TeradataFixedSizeReader<int32_t>>();
		}
		if (width <= 18) {
			return make_uniq<TeradataFixedSizeReader<int64_t>>();
		}
		if (width <= 38) {
			return make_uniq<TeradataFixedSizeReader<hugeint_t>>();
		}
		throw InvalidInputException("Invalid Teradata decimal width: %d", width);
	}
	case TeradataTypeId::INTERVAL_YEAR:
		return make_uniq<TeradataIntervalYearReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_YEAR_TO_MONTH:
		return make_uniq<TeradataIntervalYearToMonthReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_MONTH:
		return make_uniq<TeradataIntervalMonthReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_DAY:
		return make_uniq<TeradataIntervalDayReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_DAY_TO_HOUR:
		return make_uniq<TeradataIntervalDayToHourReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_DAY_TO_MINUTE:
		return make_uniq<TeradataIntervalDayToMinuteReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_DAY_TO_SECOND:
		return make_uniq<TeradataIntervalDayToSecondReader>(type.GetWidth(), type.GetScale());
	case TeradataTypeId::INTERVAL_HOUR:
		return make_uniq<TeradataIntervalHourReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_HOUR_TO_MINUTE:
		return make_uniq<TeradataIntervalHourToMinuteReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_HOUR_TO_SECOND:
		return make_uniq<TeradataIntervalHourToSecondReader>(type.GetWidth(), type.GetScale());
	case TeradataTypeId::INTERVAL_MINUTE:
		return make_uniq<TeradataIntervalMinuteReader>(type.GetWidth());
	case TeradataTypeId::INTERVAL_MINUTE_TO_SECOND:
		return make_uniq<TeradataIntervalMinuteToSecondReader>(type.GetWidth(), type.GetScale());
	case TeradataTypeId::INTERVAL_SECOND:
		return make_uniq<TeradataIntervalSecondReader>(type.GetWidth(), type.GetScale());
	default:
		throw NotImplementedException("Teradata type reader for '%s' not implemented", type.ToString());
	}
}

struct IntervalYearOp {
	static void Parse(BinaryReader &reader, interval_t &result) {
		int32_t years = 0;
		sscanf(reader.ReadBytes(4), "%d", &years);
	}
};

} // namespace duckdb


================================================
FILE: src/teradata_column_reader.hpp
================================================
#pragma once

#include "duckdb.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Teradata Converter
//-------------------------------------------------------------------------------------------------∂---------------------

class BinaryReader;
class TeradataType;

class TeradataColumnReader {
public:
	virtual ~TeradataColumnReader() = default;
	virtual void Decode(BinaryReader &reader, Vector &vec, idx_t row_idx, bool is_null) = 0;

	// Construct a reader from a Teradata type
	static unique_ptr<TeradataColumnReader> Make(const TeradataType &type);
};

} // namespace duckdb


================================================
FILE: src/teradata_column_writer.cpp
================================================
#include "teradata_column_writer.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"

#include <duckdb/common/types/time.hpp>

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Teradata Column Writer
//----------------------------------------------------------------------------------------------------------------------

void TeradataColumnWriter::SetPresenceBits(idx_t count, idx_t col_idx, char *records[]) const {
	const auto byte_idx = col_idx / 8;
	const auto bit_idx = (7 - (col_idx % 8));

	for (idx_t out_idx = 0; out_idx < count; out_idx++) {
		const auto row_idx = format.sel->get_index(out_idx);

		// Set the presence bit
		if (!format.validity.RowIsValid(row_idx)) {
			auto &validity_byte = records[out_idx][byte_idx];
			validity_byte |= static_cast<char>(1) << bit_idx;
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Helper alias
//----------------------------------------------------------------------------------------------------------------------

template <class T>
using const_optional_ptr = optional_ptr<const T>;

//----------------------------------------------------------------------------------------------------------------------
// Typed Column Writer Helper Class
//----------------------------------------------------------------------------------------------------------------------

template <class T>
class TypedColumnWriter : public TeradataColumnWriter {
public:
	void ComputeSizes(idx_t count, int32_t lengths[]) override {
		auto data_ptr = UnifiedVectorFormat::GetData<T>(format);

		for (idx_t out_idx = 0; out_idx < count; out_idx++) {
			const auto row_idx = format.sel->get_index(out_idx);
			const auto is_valid = format.validity.RowIsValid(row_idx);

			if (is_valid) {
				lengths[out_idx] += Reserve(&data_ptr[row_idx]);
			} else {
				lengths[out_idx] += Reserve(nullptr);
			}
		}
	}

	void EncodeVector(idx_t count, char *records[]) override {
		auto data_ptr = UnifiedVectorFormat::GetData<T>(format);

		for (idx_t out_idx = 0; out_idx < count; out_idx++) {
			const auto row_idx = format.sel->get_index(out_idx);
			const auto is_valid = format.validity.RowIsValid(row_idx);

			if (is_valid) {
				Encode(&data_ptr[row_idx], records[out_idx]);
			} else {
				Encode(nullptr, records[out_idx]);
			}
		}
	}

	virtual int32_t Reserve(const_optional_ptr<T> value) = 0;
	virtual void Encode(const_optional_ptr<T> value, char *&result) = 0;
};

//----------------------------------------------------------------------------------------------------------------------
// Column Writers
//----------------------------------------------------------------------------------------------------------------------

class TeradataVarcharWriter final : public TypedColumnWriter<string_t> {
public:
	int32_t Reserve(const_optional_ptr<string_t> value) override {
		return sizeof(uint16_t) + (value ? value->GetSize() : 0);
	}

	// TODO: Deal with character encoding/conversion here
	void Encode(const_optional_ptr<string_t> value, char *&result) override {
		if (value) {
			const auto len = value->GetSize();
			memcpy(result, &len, sizeof(uint16_t));
			result += sizeof(uint16_t);

			memcpy(result, value->GetDataUnsafe(), len);
			result += len;
		} else {
			constexpr auto zero_length = 0;
			memcpy(result, &zero_length, sizeof(uint16_t));
			result += sizeof(uint16_t);
		}
	}
};

class TeradataBlobWriter final : public TypedColumnWriter<string_t> {
public:
	int32_t Reserve(const_optional_ptr<string_t> value) override {
		return sizeof(uint16_t) + (value ? value->GetSize() : 0);
	}

	void Encode(const_optional_ptr<string_t> value, char *&result) override {
		if (value) {
			const auto len = value->GetSize();
			memcpy(result, &len, sizeof(uint16_t));
			result += sizeof(uint16_t);

			memcpy(result, value->GetDataUnsafe(), len);
			result += len;
		} else {
			constexpr auto zero_length = 0;
			memcpy(result, &zero_length, sizeof(uint16_t));
			result += sizeof(uint16_t);
		}
	}
};

template <class SOURCE, class TARGET = SOURCE>
class TeradataFixedSizeWriter final : public TypedColumnWriter<SOURCE> {
public:
	int32_t Reserve(const_optional_ptr<SOURCE> value) override {
		return sizeof(TARGET);
	}

	void Encode(const_optional_ptr<SOURCE> value, char *&result) override {
		if (value) {
			const auto src = *value;
			const auto dst = static_cast<TARGET>(src);

			// We have to memcpy here to avoid alignment issues
			memcpy(result, &dst, sizeof(TARGET));
		}

		result += sizeof(TARGET);
	}
};

class TeradataDateWriter final : public TypedColumnWriter<date_t> {
public:
	int32_t Reserve(const_optional_ptr<date_t> value) override {
		return sizeof(int32_t);
	}

	void Encode(const_optional_ptr<date_t> value, char *&result) override {
		if (value) {

			int32_t year = 0;
			int32_t month = 0;
			int32_t day = 0;

			Date::Convert(*value, year, month, day);

			const auto td_date = (year - 1900) * 10000 + month * 100 + day;

			memcpy(result, &td_date, sizeof(int32_t));
		}

		result += sizeof(int32_t);
	}
};

class TeradataTimestampTZWriter final : public TypedColumnWriter<timestamp_t> {
public:
	static constexpr auto CHAR_SIZE = 32;

	TeradataTimestampTZWriter() {
		StrfTimeFormat::ParseFormatSpecifier("%Y-%m-%d %H:%M:%S.%f+00:00", time_format);
	}

	int32_t Reserve(const_optional_ptr<timestamp_t> value) override {
		return CHAR_SIZE;
	}

	void Encode(const_optional_ptr<timestamp_t> value, char *&result) override {
		if (value) {
			const auto date = Timestamp::GetDate(*value);
			const auto time = Timestamp::GetTime(*value);

			const auto len = time_format.GetLength(date, time, 0, nullptr);

			if (len != CHAR_SIZE) {
				throw InvalidInputException("Teradata timestamp: '%s' is not in the expected format",
				                            Timestamp::ToString(*value));
			}

			time_format.FormatString(date, time, result);
		}

		result += CHAR_SIZE;
	}

private:
	StrfTimeFormat time_format;
};

class TeradataTimestampUSWriter final : public TypedColumnWriter<timestamp_t> {
public:
	static constexpr auto CHAR_SIZE = 26;

	TeradataTimestampUSWriter() {
		StrfTimeFormat::ParseFormatSpecifier("%Y-%m-%d %H:%M:%S.%f", time_format);
	}

	int32_t Reserve(const_optional_ptr<timestamp_t> value) override {
		return CHAR_SIZE;
	}

	void Encode(const_optional_ptr<timestamp_t> value, char *&result) override {
		if (value) {
			const auto date = Timestamp::GetDate(*value);
			const auto time = Timestamp::GetTime(*value);

			const auto len = time_format.GetLength(date, time, 0, nullptr);

			if (len != CHAR_SIZE) {
				throw InvalidInputException("Teradata timestamp: '%s' is not in the expected format",
				                            Timestamp::ToString(*value));
			}

			time_format.FormatString(date, time, result);
		}

		result += CHAR_SIZE;
	}

private:
	StrfTimeFormat time_format;
};

class TeradataTimestampMSWriter final : public TypedColumnWriter<timestamp_t> {
public:
	static constexpr auto CHAR_SIZE = 23;

	TeradataTimestampMSWriter() {
		StrfTimeFormat::ParseFormatSpecifier("%Y-%m-%d %H:%M:%S.%g", time_format);
	}

	int32_t Reserve(const_optional_ptr<timestamp_t> value) override {
		return CHAR_SIZE;
	}

	void Encode(const_optional_ptr<timestamp_t> value, char *&result) override {
		if (value) {
			auto ts = *value;

			if (Timestamp::IsFinite(ts)) {
				ts = Timestamp::FromEpochMs(ts.value);
			}

			const auto date = Timestamp::GetDate(ts);
			const auto time = Timestamp::GetTime(ts);
			const auto len = time_format.GetLength(date, time, 0, nullptr);

			if (len != CHAR_SIZE) {
				throw InvalidInputException("Teradata timestamp: '%s' is not in the expected format",
				                            Timestamp::ToString(ts));
			}

			time_format.FormatString(date, time, result);
		}

		result += CHAR_SIZE;
	}

private:
	StrfTimeFormat time_format;
};

class TeradataTimestampSecWriter final : public TypedColumnWriter<timestamp_t> {
public:
	static constexpr auto CHAR_SIZE = 19;

	TeradataTimestampSecWriter() {
		StrfTimeFormat::ParseFormatSpecifier("%Y-%m-%d %H:%M:%S", time_format);
	}

	int32_t Reserve(const_optional_ptr<timestamp_t> value) override {
		return CHAR_SIZE;
	}

	void Encode(const_optional_ptr<timestamp_t> value, char *&result) override {
		if (value) {
			auto ts = *value;

			if (Timestamp::IsFinite(ts)) {
				ts = Timestamp::FromEpochSeconds(ts.value);
			}

			const auto date = Timestamp::GetDate(ts);
			const auto time = Timestamp::GetTime(ts);

			const auto len = time_format.GetLength(date, time, 0, nullptr);

			if (len != CHAR_SIZE) {
				throw InvalidInputException("Teradata timestamp: '%s' is not in the expected format",
				                            Timestamp::ToString(ts));
			}

			time_format.FormatString(date, time, result);
		}

		result += CHAR_SIZE;
	}

private:
	StrfTimeFormat time_format;
};

class TeradataTimeWriter final : public TypedColumnWriter<dtime_t> {
public:
	static constexpr auto CHAR_SIZE = 15;

	int32_t Reserve(const_optional_ptr<dtime_t> value) override {
		return CHAR_SIZE;
	}

	void Encode(const_optional_ptr<dtime_t> value, char *&result) override {
		if (value) {
			const auto &time = *value;

			int32_t time_units[4];
			Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

			char micro_buffer[6];
			TimeToStringCast::FormatMicros(time_units[3], micro_buffer);
			TimeToStringCast::Format(result, CHAR_SIZE, time_units, micro_buffer);
		}
		result += CHAR_SIZE;
	}
};

class TeradataTimeTZWriter final : public TypedColumnWriter<dtime_tz_t> {
public:
	static constexpr auto CHAR_SIZE = 21;

	int32_t Reserve(const_optional_ptr<dtime_tz_t> value) override {
		return CHAR_SIZE;
	}

	void Encode(const_optional_ptr<dtime_tz_t> value, char *&result) override {
		if (value) {
			const auto time = value->time();

			int32_t time_units[4];
			Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

			char micro_buffer[6];
			TimeToStringCast::FormatMicros(time_units[3], micro_buffer);
			TimeToStringCast::Format(result, 15, time_units, micro_buffer);

			memcpy(result + 15, "+00:00", 6);
		}

		result += CHAR_SIZE;
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------------------------------------------------

unique_ptr<TeradataColumnWriter> TeradataColumnWriter::Make(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int8_t>>();
	case LogicalTypeId::SMALLINT:
		return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int16_t>>();
	case LogicalTypeId::INTEGER:
		return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int32_t>>();
	case LogicalTypeId::BIGINT:
		return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int64_t>>();
	case LogicalTypeId::DOUBLE:
		return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<double>>();
	case LogicalTypeId::VARCHAR:
		return make_uniq_base<TeradataColumnWriter, TeradataVarcharWriter>();
	case LogicalTypeId::BLOB:
		return make_uniq_base<TeradataColumnWriter, TeradataBlobWriter>();
	case LogicalTypeId::DECIMAL: {
		const auto width = DecimalType::GetWidth(type);
		if (width <= 2) {
			// In teradata, the small decimals are stored as 1 byte, compared to 2 bytes in duckdb
			return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int16_t, int8_t>>();
		}
		if (width <= 4) {
			return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int16_t>>();
		}
		if (width <= 9) {
			return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int32_t>>();
		}
		if (width <= 18) {
			return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<int64_t>>();
		}
		if (width <= 38) {
			return make_uniq_base<TeradataColumnWriter, TeradataFixedSizeWriter<hugeint_t>>();
		}
		throw InvalidInputException("Invalid Teradata decimal width: %d", width);
	}
	case LogicalTypeId::DATE:
		return make_uniq_base<TeradataColumnWriter, TeradataDateWriter>();
	case LogicalTypeId::TIME:
		return make_uniq_base<TeradataColumnWriter, TeradataTimeWriter>();
	case LogicalTypeId::TIME_TZ:
		return make_uniq_base<TeradataColumnWriter, TeradataTimeTZWriter>();
	case LogicalTypeId::TIMESTAMP_SEC:
		return make_uniq_base<TeradataColumnWriter, TeradataTimestampSecWriter>();
	case LogicalTypeId::TIMESTAMP_MS:
		return make_uniq_base<TeradataColumnWriter, TeradataTimestampMSWriter>();
	case LogicalTypeId::TIMESTAMP:
		return make_uniq_base<TeradataColumnWriter, TeradataTimestampUSWriter>();
	case LogicalTypeId::TIMESTAMP_TZ:
		return make_uniq_base<TeradataColumnWriter, TeradataTimestampTZWriter>();
	default:
		throw NotImplementedException("TeradataColumnWriter::Make: type %s not supported", type.ToString());
	}
}

} // namespace duckdb


================================================
FILE: src/teradata_column_writer.hpp
================================================
#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TeradataColumnWriter {
public:
	virtual ~TeradataColumnWriter() = default;
	void Init(Vector &vec, idx_t count);
	void SetPresenceBits(idx_t count, idx_t col_idx, char *records[]) const;

	virtual void ComputeSizes(idx_t count, int32_t lengths[]) = 0;
	virtual void EncodeVector(idx_t count, char *records[]) = 0;

	static unique_ptr<TeradataColumnWriter> Make(const LogicalType &type);

protected:
	UnifiedVectorFormat format;
};

inline void TeradataColumnWriter::Init(Vector &vec, idx_t count) {
	vec.ToUnifiedFormat(count, format);
}

} // namespace duckdb


================================================
FILE: src/teradata_common.cpp
================================================
#include "teradata_common.hpp"

#include "duckdb/common/dl.hpp"

namespace duckdb {

typedef Int32 (*td_callback)(Int32 *, char *, struct DBCAREA *);
static td_callback DBCHINI_IMPL = nullptr;
static td_callback DBCHCL_IMPL = nullptr;

static mutex load_mutex;
static bool IsLoadedInternal() {
	return DBCHINI_IMPL != nullptr && DBCHCL_IMPL != nullptr;
}

/*
	https://docs.teradata.com/r/Enterprise_IntelliFlex_Lake_VMware/Teradata-Call-Level-Interface-Version-2-Reference-for-Workstation-Attached-Systems-20.00/CLI-Files-and-Setup/Finding-CLI-Files-on-the-System

	On all 64-bit UNIX platforms, the default installation directory for:
		/opt/teradata/client/<release_number>/lib64

	On Apple macOS platforms, the default installation directory is:
		/Library/Application Support/teradata/client/<release_number>/lib

	On 64-bit Windows, the default installation directory for 64-bit files is:
		%ProgramFiles%\Teradata\ Client\<release_number>\bin
 */
static const char* const CLIV2_SEARCH_PATHS[] = {
#if defined(__APPLE__)
	"libcliv2.dylib",
	"/Library/Application Support/teradata/client/20.00/lib/libcliv2.dylib",
	"/Library/Application Support/teradata/client/17.20/lib/libcliv2.dylib",
	"/Library/Application Support/teradata/client/17.10/lib/libcliv2.dylib",
	"/Library/Application Support/teradata/client/17.00/lib/libcliv2.dylib",
	"/Library/Application Support/teradata/client/16.20/lib/libcliv2.dylib",
#endif
#if defined(_WIN32)
	"libcliv2.dll",
	"C:\\Program Files\\Teradata\\Client\\20.00\\bin\\libcliv2.dll",
	"C:\\Program Files\\Teradata\\Client\\17.20\\bin\\libcliv2.dll",
	"C:\\Program Files\\Teradata\\Client\\17.10\\bin\\libcliv2.dll",
	"C:\\Program Files\\Teradata\\Client\\17.00\\bin\\libcliv2.dll",
	"C:\\Program Files\\Teradata\\Client\\16.20\\bin\\libcliv2.dll",
#endif
#if defined(__linux__)
	"libcliv2.so",
	"opt/teradata/client/20.00/lib64/libcliv2.so",
	"opt/teradata/client/17.20/lib64/libcliv2.so",
	"opt/teradata/client/17.10/lib64/libcliv2.so",
	"opt/teradata/client/17.00/lib64/libcliv2.so",
	"opt/teradata/client/16.20/lib64/libcliv2.so",
#endif
};

static bool TryPath(const char* str, void* &handle, vector<string> &errors) {
	const auto new_handle = dlopen(str, RTLD_NOW | RTLD_LOCAL);
	if (new_handle) {
		handle = new_handle;
		return true;
	}
	errors.push_back( GetDLError());
	return false;
}


static void* TryLoadCLIV2() {
	vector<string> errors;
	void* handle = nullptr;

	// Otherwise, check some default paths
	for (const auto path : CLIV2_SEARCH_PATHS) {
		if (TryPath(path, handle, errors)) {
			return handle;
		}
	}

	throw IOException("Failed to load Teradata CLIV2: %s", StringUtil::Join(errors, "\n"));
}

bool TeradataCLIV2::IsLoaded() {
	lock_guard<mutex> lock(load_mutex);
	return IsLoadedInternal();
}

void TeradataCLIV2::Load() {
	lock_guard<mutex> lock(load_mutex);
	if (IsLoadedInternal()) {
		return;
	}

	const auto cliv2_handle = TryLoadCLIV2();

	// Load the DBCHINI and DBCHCL functions from the Teradata library
	DBCHINI_IMPL = reinterpret_cast<td_callback>(dlsym(cliv2_handle, "DBCHINI"));
	if (!DBCHINI_IMPL) {
		throw IOException("Failed to load DBCHINI from Teradata library: %s", GetDLError());
	}

	DBCHCL_IMPL = reinterpret_cast<td_callback>(dlsym(cliv2_handle, "DBCHCL"));
	if (!DBCHCL_IMPL) {
		throw IOException("Failed to load DBCHCL from Teradata library: %s", GetDLError());
	}
}

} // namespace duckdb


Int32 DBCHINI(Int32 *a, char *b, struct DBCAREA *c) {
	if (duckdb::DBCHINI_IMPL) {
		return duckdb::DBCHINI_IMPL(a, b, c);
	}
	throw duckdb::IOException("Teradata CLIV2 not loaded, cannot call DBCHINI!");
}

Int32 DBCHCL(Int32 *a, char *b, struct DBCAREA *c) {
	if (duckdb::DBCHCL_IMPL) {
		return duckdb::DBCHCL_IMPL(a, b, c);
	}
	throw duckdb::IOException("Teradata CLIV2 not loaded, cannot call DBCHCL!");
}



================================================
FILE: src/teradata_common.hpp
================================================
#pragma once

#include "duckdb.hpp"

// Otherwise doesnt work on linux
#define NO_CLIV2_ERROR_T

// Teradata Includes
#include <coptypes.h>
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>

#define CONNECTED     0
#define NOT_CONNECTED 1
#define OK            0
#define STOP          1
#define FAILED        -1

#ifdef WIN32
__declspec(dllimport) char COPCLIVersion[];
__declspec(dllimport) char COPMTDPVersion[];
__declspec(dllimport) char COPMOSIosVersion[];
__declspec(dllimport) char COPMOSIDEPVersion[];
__declspec(dllimport) char OSERRVersion[];
#else
extern char COPCLIVersion[];
extern char COPMTDPVersion[];
extern char COPMOSIosVersion[];
extern char COPMOSIDEPVersion[];
extern char OSERRVersion[];
#endif

namespace duckdb {

struct TeradataCLIV2 {
	static void Load();
	static bool IsLoaded();
};

} // namespace duckdb



================================================
FILE: src/teradata_connection.cpp
================================================
#include "teradata_connection.hpp"
#include "teradata_common.hpp"
#include "teradata_request.hpp"

namespace duckdb {

void TeradataConnection::Reconnect() {

	Int32 result = EM_OK;

	const auto dbc_ptr = make_uniq<DBCAREA>();
	auto dbc = *dbc_ptr;         // Copy the DBCAREA to the member variable
	char cnta[4] = {0, 0, 0, 0}; // Initialize the control area to zero

	// Set the total length
	dbc.total_len = sizeof(DBCAREA);

	DBCHINI(&result, cnta, &dbc); // Try to initialize the DBCAREA
	if (result != EM_OK) {
		// Failed to initialize the DBCAREA we cannot connect
		throw InternalException("Failed to initialize DBCAREA: %s", string(dbc.msg_text, dbc.msg_len));
	}

	dbc.change_opts = 'Y';

	vector<char> logon_buf(logon_string.begin(), logon_string.end());
	dbc.func = DBFCON;
	dbc.logon_ptr = logon_buf.data();
	dbc.logon_len = logon_buf.size();
	dbc.connect_type = 'R';

	// Try to connect
	DBCHCL(&result, cnta, &dbc);
	if (result != EM_OK) {
		// Failed to connect
		throw IOException("Failed to connect to Teradata: %s", string(dbc.msg_text, dbc.msg_len));
	}

	dbc.func = DBFFET;
	char buf[512] = {};
	dbc.fet_data_ptr = buf;
	dbc.fet_max_data_len = sizeof(buf);

	dbc.i_req_id = dbc.o_req_id;
	dbc.i_sess_id = dbc.o_sess_id;

	// Now call the fetch command
	DBCHCL(&result, cnta, &dbc);
	if (result != EM_OK) {
		// Failed to fetch
		throw IOException("Failed to fetch from Teradata: %s", string(dbc.msg_text, dbc.msg_len));
	}

	// End the connection request properly
	dbc.func = DBFERQ;
	DBCHCL(&result, cnta, &dbc);
	// Don't check error here - some versions may not require DBFERQ after connection

	session_id = dbc.o_sess_id;
	is_connected = true;
}

void TeradataConnection::Disconnect() {
	if (!is_connected) {
		return;
	}

	auto dbc_ptr = make_uniq<DBCAREA>();
	auto dbc = *dbc_ptr;         // Copy the DBCAREA to the member variable
	char cnta[4] = {0, 0, 0, 0}; // Initialize the control area to zero

	Int32 result = EM_OK;

	dbc.func = DBFDSC;
	dbc.i_sess_id = session_id;
	DBCHCL(&result, cnta, &dbc);
	if (result != EM_OK) {
		// Can this even happen?
		throw IOException("Failed to disconnect from Teradata database: %s", string(dbc.msg_text, dbc.msg_len));
	}

	is_connected = false;
}

void TeradataConnection::Execute(const string &sql) {
	// TODO: Pool request contexts
	TeradataRequestContext ctx(*this);
	ctx.Execute(sql);
}

void TeradataConnection::Execute(const string &sql, DataChunk &chunk, ArenaAllocator &arena,
                                 vector<unique_ptr<TeradataColumnWriter>> &writers) {
	TeradataRequestContext ctx(*this);
	ctx.Execute(sql, chunk, arena, writers);
}

unique_ptr<TeradataQueryResult> TeradataConnection::Query(const string &sql, bool materialize) {

	// TODO: Pool request contexts
	auto ctx = make_uniq<TeradataRequestContext>(*this);

	vector<TeradataType> types;
	ctx->Query(sql, types);

	if (materialize) {
		// Fetch all into a CDC and return a materialized result
		auto cdc = ctx->FetchAll(types);
		return make_uniq<MaterializedTeradataQueryResult>(std::move(types), std::move(cdc));
	} else {
		// Streaming result, pass on the context to the result so we can keep fetching it lazily
		return make_uniq<StreamingTeradataQueryResult>(std::move(types), std::move(ctx));
	}
}

void TeradataConnection::Prepare(const string &sql, vector<TeradataType> &types, vector<string> &names) {
	// TODO: Pool request contexts
	TeradataRequestContext td_ctx(*this);
	td_ctx.Prepare(sql, types, names);
}

} // namespace duckdb


================================================
FILE: src/teradata_connection.hpp
================================================
#pragma once

#include "teradata_common.hpp"
#include "teradata_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class TeradataColumnWriter;

class TeradataConnection {
public:
	explicit TeradataConnection(const string &logon_string_p, idx_t buffer_size_p) : buffer_size(buffer_size_p){
		SetLogonString(logon_string_p);
		Reconnect();
	}

	~TeradataConnection() {
		Disconnect();
	}

	Int32 GetSessionId() const {
		return session_id;
	}

	void SetLogonString(const string &logon_string_p) {
		logon_string = logon_string_p;
	}

	const string &GetLogonString() const {
		return logon_string;
	}

	idx_t GetBufferSize() const {
		return buffer_size;
	}

	void Reconnect();
	void Disconnect();

	void Execute(const string &sql);

	// Execute a parameterized statement, once for each row in the chunk
	// TODO: Move arena and writers into a ExecuteState class
	void Execute(const string &sql, DataChunk &chunk, ArenaAllocator &arena,
	             vector<unique_ptr<TeradataColumnWriter>> &writers);

	// Execute a query with a result set, optionally materializing everything
	unique_ptr<TeradataQueryResult> Query(const string &sql, bool materialize);

	// Prepare a query
	void Prepare(const string &sql, vector<TeradataType> &types, vector<string> &names);

private:
	string logon_string;
	Int32 session_id = 0;
	bool is_connected = false;

	// Teradata response buffer size
	idx_t buffer_size;
};

} // namespace duckdb


================================================
FILE: src/teradata_delete_update.cpp
================================================
#include "teradata_delete_update.hpp"
#include "teradata_catalog.hpp"
#include "teradata_transaction.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Delete Operator
//----------------------------------------------------------------------------------------------------------------------

TeradataDeleteUpdate::TeradataDeleteUpdate(LogicalOperator &op, TableCatalogEntry &table, string name, string query)
	 : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
	name(std::move(name)), query(std::move(query)) {
}

class TeradataDeleteUpdateGlobalState final : public GlobalSinkState {
public:
	idx_t affected_rows = 0;
};

unique_ptr<GlobalSinkState> TeradataDeleteUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TeradataDeleteUpdateGlobalState>();
}

SinkResultType TeradataDeleteUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// This operator does not actually sink any data, it just generates a DELETE statement
	return SinkResultType::FINISHED;
}

SinkFinalizeType TeradataDeleteUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
											 OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<TeradataDeleteUpdateGlobalState>();
	auto &transaction = TeradataTransaction::Get(context, table.catalog);
	auto &connection = transaction.GetConnection();

	// TODO:
	// We cant get affected rows from the DELETE statement, we need to inspect the query log.
	// So for now, always return 0 affected rows.
	connection.Execute(query);

	gstate.affected_rows = 0; //result->AffectedRows();
	return SinkFinalizeType::READY;
}

SourceResultType TeradataDeleteUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
											OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<TeradataDeleteUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.affected_rows));

	return SourceResultType::FINISHED;
}

string TeradataDeleteUpdate::GetName() const {
	return name;
}

InsertionOrderPreservingMap<string> TeradataDeleteUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// Plan
//----------------------------------------------------------------------------------------------------------------------

// Attempt to reconstruct the WHERE clause from the physical operator tree
static string ExtractFilters(PhysicalOperator &child, const string &statement) {
	switch (child.type) {
		case PhysicalOperatorType::FILTER: {
			const auto &filter = child.Cast<PhysicalFilter>();
			const auto result = ExtractFilters(child.children[0], statement);
			auto filter_str = filter.expression->ToString();
			if (result.empty()) {
				return filter_str;
			}
			return result + " AND " + filter_str;
		}
		case PhysicalOperatorType::PROJECTION: {
			const auto &proj = child.Cast<PhysicalProjection>();
			for (auto &expr : proj.select_list) {
				switch (expr->type) {
				case ExpressionType::BOUND_REF:
				case ExpressionType::BOUND_COLUMN_REF:
				case ExpressionType::VALUE_CONSTANT:
					break;
				default:
					throw NotImplementedException("Unsupported expression type in projection - only simple deletes/updates "
												  "are supported in the Teradata connector");
				}
			}
			return ExtractFilters(child.children[0], statement);
		}
		case PhysicalOperatorType::TABLE_SCAN: {
			const auto &table_scan = child.Cast<PhysicalTableScan>();
			if (!table_scan.table_filters) {
				return string();
			}
			string result;
			for (auto &entry : table_scan.table_filters->filters) {
				const auto column_index = entry.first;
				const auto &filter = entry.second;
				string column_name;
				if (column_index < table_scan.names.size()) {
					const auto col_id = table_scan.column_ids[column_index].GetPrimaryIndex();
					if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
						throw NotImplementedException("RowID column not supported in Teradata DELETE statements - "
													  "use a primary key or unique index instead");

					}
					column_name = table_scan.names[col_id];
				}
				BoundReferenceExpression bound_ref(std::move(column_name), LogicalTypeId::INVALID, 0);
				const auto filter_expr = filter->ToExpression(bound_ref);
				auto filter_str = filter_expr->ToString();
				if (result.empty()) {
					result = std::move(filter_str);
				} else {
					result += " AND " + filter_str;
				}
			}
			return result;
		}
		default:
			throw NotImplementedException("Unsupported operator type %s in %s statement - only simple deletes "
			                              "(e.g. %s "
			                              "FROM tbl WHERE x=y) are supported in the Teradata connector",
			                              PhysicalOperatorToString(child.type), statement, statement);
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Plan DELETE
//----------------------------------------------------------------------------------------------------------------------

// Attempt to reconstruct the DELETE statement from the logical delete operator and its child
static string ConstructDeleteStatement(LogicalDelete &op, PhysicalOperator &child) {
	string result = "DELETE FROM ";
	result += KeywordHelper::WriteQuoted(op.table.schema.name, '"');
	result += ".";
	result += KeywordHelper::WriteQuoted(op.table.name, '"');
	const auto filters = ExtractFilters(child, "DELETE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

PhysicalOperator &TeradataCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                              PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a Teradata table");
	}

	auto &execute = planner.Make<TeradataDeleteUpdate>(op, op.table, "TERADATA_DELETE", ConstructDeleteStatement(op, plan));
	execute.children.push_back(plan);
	return execute;
}

//----------------------------------------------------------------------------------------------------------------------
// Plan UPDATE
//----------------------------------------------------------------------------------------------------------------------

static string ConstructUpdateStatement(LogicalUpdate &op, PhysicalOperator &child) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	string result = "UPDATE";
	result += KeywordHelper::WriteQuoted(op.table.schema.name, '"');
	result += ".";
	result += KeywordHelper::WriteQuoted(op.table.name, '"');
	result += " SET ";
	if (child.type != PhysicalOperatorType::PROJECTION) {
		throw NotImplementedException("Teradata Update not supported - Expected the "
									  "child of an update to be a projection");
	}
	auto &proj = child.Cast<PhysicalProjection>();
	for (idx_t c = 0; c < op.columns.size(); c++) {
		if (c > 0) {
			result += ", ";
		}
		auto &col = op.table.GetColumn(op.table.GetColumns().PhysicalToLogical(op.columns[c]));
		result += KeywordHelper::WriteQuoted(col.GetName(), '"');
		result += " = ";
		if (op.expressions[c]->type == ExpressionType::VALUE_DEFAULT) {
			result += "DEFAULT";
			continue;
		}
		if (op.expressions[c]->type != ExpressionType::BOUND_REF) {
			throw NotImplementedException("Teradata Update not supported - Expected a bound reference expression");
		}
		auto &ref = op.expressions[c]->Cast<BoundReferenceExpression>();
		result += proj.select_list[ref.index]->ToString();
	}
	result += " ";
	auto filters = ExtractFilters(child.children[0], "UPDATE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

PhysicalOperator &TeradataCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
										   PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a Teradata table");
	}

	auto &execute = planner.Make<TeradataDeleteUpdate>(op, op.table, "TERADATA_UPDATE", ConstructUpdateStatement(op, plan));
	execute.children.push_back(plan);
	return execute;
}




} // namespace duckdb


================================================
FILE: src/teradata_delete_update.hpp
================================================
#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class TeradataDeleteUpdate final : public PhysicalOperator {
public:
	TeradataDeleteUpdate(LogicalOperator &op, TableCatalogEntry &table, string name, string query);

	//! The table to delete/update from
	TableCatalogEntry &table;
	//! The name of the operator
	string name;
	//! The generated statement
	string query;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
							  OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb



================================================
FILE: src/teradata_execute.cpp
================================================
#include "teradata_execute.hpp"
#include "teradata_catalog.hpp"
#include "teradata_transaction.hpp"

#include "duckdb/function/function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
struct TeradataExecuteBindData final : public TableFunctionData {
	TeradataExecuteBindData(TeradataCatalog &td_catalog, string query_p)
	    : td_catalog(td_catalog), query(std::move(query_p)) {
	}

	bool finished = false;
	TeradataCatalog &td_catalog;
	string query;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in teradata_execute", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "teradata") {
		throw BinderException("Attached database \"%s\" does not refer to a Teradata database", db_name);
	}
	auto &td_catalog = catalog.Cast<TeradataCatalog>();
	return make_uniq<TeradataExecuteBindData>(td_catalog, input.inputs[1].GetValue<string>());
}

//----------------------------------------------------------------------------------------------------------------------
// Execute
//----------------------------------------------------------------------------------------------------------------------
static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<TeradataExecuteBindData>();
	if (data.finished) {
		return;
	}

	const auto &transaction = TeradataTransaction::Get(context, data.td_catalog);
	transaction.GetConnection().Execute(data.query);
	data.finished = true;
}

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------

void TeradataExecuteFunction::Register(DatabaseInstance &db) {
	TableFunction func("teradata_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute, Bind);
	ExtensionUtil::RegisterFunction(db, func);
}

} // namespace duckdb


================================================
FILE: src/teradata_execute.hpp
================================================
#pragma once

namespace duckdb {

class DatabaseInstance;

struct TeradataExecuteFunction {
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb


================================================
FILE: src/teradata_extension.cpp
================================================
#define DUCKDB_EXTENSION_MAIN

#include "teradata_extension.hpp"
#include "teradata_query.hpp"
#include "teradata_execute.hpp"
#include "teradata_storage.hpp"
#include "teradata_secret.hpp"
#include "teradata_clear_cache.hpp"
#include "teradata_common.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// State
//----------------------------------------------------------------------------------------------------------------------

class TeradataExtensionState final : public ClientContextState {
public:
	bool CanRequestRebind() override {
		return true;
	}

	RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) override {
		if (error.Type() != ExceptionType::BINDER) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}

		const auto &extra_info = error.ExtraInfo();
		const auto entry = extra_info.find("error_subtype");
		if (entry == extra_info.end()) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		if (entry->second != "COLUMN_NOT_FOUND") {
			return RebindQueryInfo::DO_NOT_REBIND;
		}

		// Try to clear caches and rebind
		TeradataClearCacheFunction::Clear(context);
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
};

class TeradataExtensionCallback final : public ExtensionCallback {
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("teradata_extension", make_shared_ptr<TeradataExtensionState>());
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Extension Load
//----------------------------------------------------------------------------------------------------------------------

void TeradataExtension::Load(DuckDB &db) {
	auto &instance = *db.instance;

	// Register Teradata functions
	TeradataQueryFunction::Register(instance);
	TeradataExecuteFunction::Register(instance);
	TeradataClearCacheFunction::Register(instance);
	TeradataSecret::Register(instance);

	// Register storage
	instance.config.storage_extensions["teradata"] = make_uniq<TeradataStorageExtension>();

	// Register callbacks
	instance.config.extension_callbacks.push_back(make_uniq<TeradataExtensionCallback>());

	// Register states on existing connections
	for (const auto &con : ConnectionManager::Get(instance).GetConnectionList()) {
		con->registered_state->Insert("teradata_extension", make_shared_ptr<TeradataExtensionState>());
	}

	instance.config.AddExtensionOption(
		"teradata_use_primary_index",
		"Whether or not to use a primary index when creating Teradata tables",
		LogicalType::BOOLEAN, Value::BOOLEAN(true));

}

std::string TeradataExtension::Name() {
	return "teradata";
}

std::string TeradataExtension::Version() const {
#ifdef EXT_VERSION_TERADATA
	return EXT_VERSION_TERADATA;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void teradata_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::TeradataExtension>();
}

DUCKDB_EXTENSION_API const char *teradata_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif



================================================
FILE: src/teradata_extension.hpp
================================================
#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TeradataExtension final : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb



================================================
FILE: src/teradata_filter.cpp
================================================
#include "teradata_filter.hpp"

#include <teradata_type.hpp>
#include <duckdb/planner/filter/constant_filter.hpp>
#include <duckdb/planner/filter/in_filter.hpp>
#include <duckdb/planner/filter/optional_filter.hpp>
#include <duckdb/planner/filter/struct_filter.hpp>

namespace duckdb {

static string TransformFilter(const string &column_name, const TableFilter &filter);
static string CreateExpression(const string &column_name, const vector<unique_ptr<TableFilter>> &filters,
                               const string &op) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto filter_str = TransformFilter(column_name, *filter);
		if (!filter_str.empty()) {
			filter_entries.push_back(std::move(filter_str));
		}
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string TransformBlob(const string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	string result = "'";
	for (idx_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}

	// Hexadecimal byte literal
	// https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Literals/Hexadecimal-Byte-Literals
	result += "'XBV";
	return result;
}

static string TransformLiteral(const Value &val) {
	// TODO: More literals
	switch (val.type().id()) {
	case LogicalTypeId::BLOB:
		return TransformBlob(StringValue::Get(val));
	default:
		return KeywordHelper::WriteQuoted(val.ToString());
	}
}

static string TransformComparision(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "<>";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Unsupported expression type");
	}
}

static string TransformFilter(const string &column_name, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return column_name + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column_name + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "AND");
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "OR");
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto constant_string = TransformLiteral(constant_filter.constant);
		auto operator_string = TransformComparision(constant_filter.comparison_type);
		return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		throw NotImplementedException("Struct extract filter is not supported in Teradata");
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return TransformFilter(column_name, *optional_filter.child_filter);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string in_list;
		for (auto &val : in_filter.values) {
			if (!in_list.empty()) {
				in_list += ", ";
			}
			in_list += TransformLiteral(val);
		}
		return column_name + " IN (" + in_list + ")";
	}
	case TableFilterType::DYNAMIC_FILTER:
		return string();
	default:
		throw InternalException("Unsupported table filter type");
	}
}

string TeradataFilter::Transform(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                 const vector<string> &names) {

	string result;
	for (auto &entry : filters->filters) {
		auto col_name = KeywordHelper::WriteQuoted(names[column_ids[entry.first]], '"');
		auto &filter = entry.second;
		auto filter_text = TransformFilter(col_name, *filter);
		if (filter_text.empty()) {
			continue;
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += filter_text;
	}

	return result;
}

} // namespace duckdb


================================================
FILE: src/teradata_filter.hpp
================================================
#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TableFilterSet;

class TeradataFilter {
public:
	static string Transform(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                        const vector<string> &names);
};

} // namespace duckdb



================================================
FILE: src/teradata_index.cpp
================================================
#include "teradata_index.hpp"
#include "teradata_catalog.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Physical Operator
//----------------------------------------------------------------------------------------------------------------------
PhysicalTeradataCreateIndex::PhysicalTeradataCreateIndex(unique_ptr<CreateIndexInfo> info, TableCatalogEntry &table)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), info(std::move(info)), table(table) {
}

SourceResultType PhysicalTeradataCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &catalog = table.catalog;
	auto &schema = table.schema;
	auto transaction = catalog.GetCatalogTransaction(context.client);
	auto existing = schema.GetEntry(transaction, CatalogType::INDEX_ENTRY, info->index_name);
	if (existing) {
		switch (info->on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return SourceResultType::FINISHED;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw BinderException("Index with name \"%s\" already exists in schema \"%s\"", info->index_name,
			                      table.schema.name);
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo drop_info;
			drop_info.type = CatalogType::INDEX_ENTRY;
			drop_info.schema = info->schema;
			drop_info.name = info->index_name;
			schema.DropEntry(context.client, drop_info);
			break;
		}
		default:
			throw InternalException("Unsupported on create conflict");
		}
	}
	schema.CreateIndex(transaction, *info, table);

	return SourceResultType::FINISHED;
}

//----------------------------------------------------------------------------------------------------------------------
// Logical Operator
//----------------------------------------------------------------------------------------------------------------------
class LogicalTeradataCreateIndex final : public LogicalExtensionOperator {
public:
	LogicalTeradataCreateIndex(unique_ptr<CreateIndexInfo> info_p, TableCatalogEntry &table)
	    : info(std::move(info_p)), table(table) {
	}

	unique_ptr<CreateIndexInfo> info;
	TableCatalogEntry &table;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<PhysicalTeradataCreateIndex>(std::move(info), table);
	}

	void Serialize(Serializer &serializer) const override {
		throw NotImplementedException("Cannot serialize TeradataCreateIndex operator");
	}

	void ResolveTypes() override {
		types = {LogicalType::BIGINT};
	}
};

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
unique_ptr<LogicalOperator> TeradataCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	return make_uniq<LogicalTeradataCreateIndex>(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info)),
	                                             table);
}

} // namespace duckdb


================================================
FILE: src/teradata_index.hpp
================================================
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

class PhysicalTeradataCreateIndex final : public PhysicalOperator {
public:
	explicit PhysicalTeradataCreateIndex(unique_ptr<CreateIndexInfo> info, TableCatalogEntry &table);

	unique_ptr<CreateIndexInfo> info;
	TableCatalogEntry &table;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb


================================================
FILE: src/teradata_index_entry.cpp
================================================
#include "teradata_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

TeradataIndexEntry::TeradataIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info,
                                       string table_name)
    : IndexCatalogEntry(catalog, schema, info), table_name(std::move(table_name)) {
}

string TeradataIndexEntry::GetSchemaName() const {
	return schema.name;
}

string TeradataIndexEntry::GetTableName() const {
	return table_name;
}

} // namespace duckdb


================================================
FILE: src/teradata_index_entry.hpp
================================================
#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

class TeradataIndexEntry final : public IndexCatalogEntry {
public:
	TeradataIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info, string table_name);

public:
	string GetSchemaName() const override;
	string GetTableName() const override;

private:
	string table_name;
};

} // namespace duckdb


================================================
FILE: src/teradata_index_set.cpp
================================================
#include "teradata_index_set.hpp"

#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include <teradata_catalog.hpp>
#include <teradata_connection.hpp>
#include <teradata_schema_entry.hpp>
#include <teradata_transaction.hpp>

namespace duckdb {

TeradataIndexSet::TeradataIndexSet(TeradataSchemaEntry &schema) : TeradataInSchemaSet(schema) {
}

static void UnqualifyColumnReferences(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, UnqualifyColumnReferences);
}

static string GetCreateIndexSQL(CreateIndexInfo &info, TableCatalogEntry &tbl) {
	string sql;
	sql = "CREATE";
	if (info.constraint_type == IndexConstraintType::UNIQUE) {
		sql += " UNIQUE";
	}
	sql += " INDEX ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.index_name, '"');
	sql += "(";
	for (idx_t i = 0; i < info.parsed_expressions.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		UnqualifyColumnReferences(*info.parsed_expressions[i]);
		sql += info.parsed_expressions[i]->ToString();
	}
	sql += ")";
	sql += " ON ";
	sql += KeywordHelper::WriteOptionallyQuoted(tbl.schema.name, '"') + ".";
	sql += KeywordHelper::WriteOptionallyQuoted(tbl.name, '"');
	return sql;
}

optional_ptr<CatalogEntry> TeradataIndexSet::CreateIndex(ClientContext &context, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {

	auto &td_transcation = TeradataTransaction::Get(context, table.catalog);
	auto &td_connection = td_transcation.GetConnection();

	const auto sql = GetCreateIndexSQL(info, table);
	td_connection.Execute(sql);

	auto index_entry = make_uniq<TeradataIndexEntry>(schema.ParentCatalog(), schema, info, table.name);
	return CreateEntry(std::move(index_entry));
}

void TeradataIndexSet::LoadEntries(ClientContext &context) {
	const auto &td_catalog = catalog.Cast<TeradataCatalog>();
	auto &conn = td_catalog.GetConnection();

	const auto query = StringUtil::Format("SELECT T.TableName, I.IndexName, I.IndexType, I.ColumnName, I.UniqueFlag "
	                                      "FROM DBC.IndicesV AS I JOIN DBC.TablesV AS T "
	                                      "ON T.TableName = I.TableName AND T.DatabaseName = I.DatabaseName "
	                                      "WHERE T.DatabaseName = '%s' AND (T.TableKind = 'T' OR T.TableKind = 'O') "
	                                      "ORDER BY T.TableName, I.IndexName, I.IndexType, I.ColumnName, I.UniqueFlag;",
	                                      schema.name);

	const auto result = conn.Query(query, false);

	for (auto &chunk : result->Chunks()) {
		chunk.Flatten();

		const auto count = chunk.size();

		const auto &tbl_name_vec = chunk.data[0];
		const auto &idx_name_vec = chunk.data[1];
		const auto &idx_type_vec = chunk.data[2];
		const auto &col_name_vec = chunk.data[3];
		const auto &idx_uniq_vec = chunk.data[4];

		const auto tbl_names = FlatVector::GetData<string_t>(tbl_name_vec);
		const auto idx_names = FlatVector::GetData<string_t>(idx_name_vec);
		const auto idx_types = FlatVector::GetData<string_t>(idx_type_vec);
		const auto col_names = FlatVector::GetData<string_t>(col_name_vec);
		const auto idx_flags = FlatVector::GetData<string_t>(idx_uniq_vec);

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {

			// TODO: Come up with a temp name if the index name is empty?
			if (FlatVector::IsNull(idx_name_vec, row_idx)) {
				continue;
				;
			}

			auto idx_name = idx_names[row_idx].GetString();
			const auto tbl_name = tbl_names[row_idx].GetString();
			const auto idx_type = idx_types[row_idx].GetString(); // TODO: Parse this
			const auto col_name = col_names[row_idx].GetString();

			auto idx_flag = IndexConstraintType::NONE;
			if (idx_flags[row_idx].GetData()[0] == 'Y') {
				idx_flag = IndexConstraintType::UNIQUE;
			}

			CreateIndexInfo info;
			info.schema = schema.name;
			info.table = tbl_name;
			info.index_name = idx_name;
			info.index_type = idx_type;
			info.constraint_type = idx_flag;

			// TODO: We need to check if there are multiple columns in the index
			info.expressions.push_back(make_uniq<ColumnRefExpression>(col_name));

			auto index_entry = make_uniq<TeradataIndexEntry>(catalog, schema, info, info.table);
			CreateEntry(std::move(index_entry));
		}
	}
}

} // namespace duckdb


================================================
FILE: src/teradata_index_set.hpp
================================================
#pragma once

#include "teradata_catalog_set.hpp"
#include "teradata_index_entry.hpp"

namespace duckdb {

class TeradataSchemaEntry;
class TableCatalogEntry;

class TeradataIndexSet final : public TeradataInSchemaSet {
public:
	explicit TeradataIndexSet(TeradataSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateIndex(ClientContext &context, CreateIndexInfo &info, TableCatalogEntry &table);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb


================================================
FILE: src/teradata_insert.cpp
================================================
#include "teradata_insert.hpp"
#include "teradata_catalog.hpp"

#include "teradata_table_entry.hpp"
#include "teradata_transaction.hpp"
#include "teradata_request.hpp"
#include "teradata_query.hpp"

#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <teradata_column_writer.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>

namespace duckdb {

//======================================================================================================================
// Teradata Insert
//======================================================================================================================

// Normal INSERT INTO table
TeradataInsert::TeradataInsert(LogicalOperator &op, TableCatalogEntry &table,
                               physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

// CREATE TABLE AS
TeradataInsert::TeradataInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//----------------------------------------------------------------------------------------------------------------------
// State
//----------------------------------------------------------------------------------------------------------------------
class TeradataInsertGlobalState final : public GlobalSinkState {
public:
	TeradataTableEntry *table = nullptr;
	idx_t insert_count = 0;
	string insert_sql;

	// TODO: move these into a state object
	ArenaAllocator arena;
	vector<unique_ptr<TeradataColumnWriter>> writers;

	explicit TeradataInsertGlobalState(ClientContext &context) : arena(BufferAllocator::Get(context)) {
	}
};

static string GetInsertSQL(const TeradataInsert &insert, const TeradataTableEntry &entry) {

	// First, figure out what columns we are inserting
	auto &columns = entry.GetColumns();
	idx_t column_count = columns.LogicalColumnCount();
	vector<PhysicalIndex> column_indices;

	if (insert.column_index_map.empty()) {
		for (idx_t i = 0; i < column_count; i++) {
			column_indices.emplace_back(i);
		}
	} else {
		column_indices.resize(column_count, PhysicalIndex(DConstants::INVALID_INDEX));
		column_count = 0;
		for (idx_t entry_idx = 0; entry_idx < insert.column_index_map.size(); entry_idx++) {
			const auto col_idx = PhysicalIndex(entry_idx);
			const auto mapped_idx = insert.column_index_map[col_idx];
			if (mapped_idx == DConstants::INVALID_INDEX) {
				// Column not specified
				continue;
			}
			column_indices[mapped_idx] = col_idx;
			column_count++;
		}
	}

	// Now construct the SQL string.
	// We first construct the USING descriptor with the column names and types so that we can pass parameters.
	// Then we construct the INSERT INTO statement and the VALUES clause with the parameter names.

	string result = "USING ";
	result += "(";
	for (idx_t i = 0; i < column_count; i++) {
		if (i > 0) {
			result += ", ";
		}

		auto &col = columns.GetColumn(column_indices[i]);
		result += KeywordHelper::WriteQuoted(col.GetName(), '"');
		result += " ";

		// Convert to teradata type first
		const auto td_type = TeradataType::FromDuckDB(col.GetType());
		result += td_type.ToString();
	}
	result += ") ";

	result += "INSERT INTO ";

	if (!entry.schema.name.empty()) {
		result += KeywordHelper::WriteQuoted(entry.schema.name, '"') + ".";
	}
	result += KeywordHelper::WriteQuoted(entry.name, '"');

	result += " VALUES (";

	for (idx_t i = 0; i < column_count; i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &col = columns.GetColumn(column_indices[i]);
		result += ":" + KeywordHelper::WriteQuoted(col.GetName(), '"');
	}
	result += ");";
	return result;
}

unique_ptr<GlobalSinkState> TeradataInsert::GetGlobalSinkState(ClientContext &context) const {

	// If no table supplied, this is a CTAS
	if (!table) {
		// CTAS is not supported. Teradata doesnt support mixing DML and DDL in transactions
		throw BinderException("CREATE TABLE AS is not supported for Teradata tables. Please create the table first and "
		                      "then insert into it.");
	}

	// Prepare just to see that we type check
	// TODO: We should type check the statement somehow...
	// auto &transaction = TeradataTransaction::Get(context, insert_table->catalog);
	// auto &conn = transaction.GetConnection();
	const auto insert_table = &table.get_mutable()->Cast<TeradataTableEntry>();

	auto result = make_uniq<TeradataInsertGlobalState>(context);
	result->table = insert_table;
	result->insert_count = 0;
	result->insert_sql = GetInsertSQL(*this, *insert_table);

	const auto table_types = insert_table->GetTypes();
	result->writers.reserve(table_types.size());
	for (auto &type : table_types) {
		// Initialize writers
		result->writers.push_back(TeradataColumnWriter::Make(type));
	}

	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Sink
//----------------------------------------------------------------------------------------------------------------------
SinkResultType TeradataInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {

	// Sink into the Teradata table
	auto &state = sink_state->Cast<TeradataInsertGlobalState>();
	auto &transaction = TeradataTransaction::Get(context.client, state.table->catalog);
	auto &conn = transaction.GetConnection();

	// Reset the arena before executing the query
	state.arena.Reset();

	// Execute, passing the data chunk as parameters.
	conn.Execute(state.insert_sql, chunk, state.arena, state.writers);
	state.insert_count += chunk.size();

	return SinkResultType::NEED_MORE_INPUT;
}

//----------------------------------------------------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------------------------------------------------
SinkFinalizeType TeradataInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

//----------------------------------------------------------------------------------------------------------------------
// GetData
//----------------------------------------------------------------------------------------------------------------------

SourceResultType TeradataInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {

	const auto &state = sink_state->Cast<TeradataInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(UnsafeNumericCast<int64_t>(state.insert_count)));

	return SourceResultType::FINISHED;
}

//----------------------------------------------------------------------------------------------------------------------
// Misc
//----------------------------------------------------------------------------------------------------------------------

string TeradataInsert::GetName() const {
	return "TeradataInsert";
}

InsertionOrderPreservingMap<string> TeradataInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// Plan
//----------------------------------------------------------------------------------------------------------------------

static void MaterializeTeradataScans(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = op.Cast<PhysicalTableScan>();
		if (TeradataCatalog::IsTeradataScan(table_scan.function.name)) {
			auto &bind_data = table_scan.bind_data->Cast<TeradataBindData>();

			// If we are inserting into Teradata, materialize the all td-scans in this part of the plan
			bind_data.is_materialized = true;
		}
	}

	for (auto &child : op.children) {
		MaterializeTeradataScans(child);
	}
}

PhysicalOperator &TeradataCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {

	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Teradata tables");
	}
	if (op.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Teradata tables");
	}

	D_ASSERT(plan);

	MaterializeTeradataScans(*plan);

	// TODO: This is where we would cast to TD types if needed
	// plan = AddCastToTeradataTypes(context, std::move(plan));

	auto &insert = planner.Make<TeradataInsert>(op, op.table, op.column_index_map);
	insert.children.push_back(*plan);

	return insert;
}

PhysicalOperator &TeradataCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {

	// TODO: This is where we would cast to TD types if needed
	// plan = AddCastToTeradataTypes(context, std::move(plan));

	MaterializeTeradataScans(plan);

	auto &insert = planner.Make<TeradataInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}

} // namespace duckdb


================================================
FILE: src/teradata_insert.hpp
================================================
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class TeradataInsert final : public PhysicalOperator {
public:
	//! INSERT INTO
	TeradataInsert(LogicalOperator &op, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map);
	//! CREATE TABLE AS
	TeradataInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb


================================================
FILE: src/teradata_query.cpp
================================================
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/attached_database.hpp"

#include "teradata_query.hpp"
#include "teradata_catalog.hpp"
#include "teradata_transaction.hpp"
#include "teradata_table_entry.hpp"

#include "duckdb/main/extension_util.hpp"

#include <teradata_filter.hpp>

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
static unique_ptr<FunctionData> TeradataQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to teradata_query cannot be NULL");
	}

	const auto db_name = input.inputs[0].GetValue<string>();

	// Look up the database to query
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in teradata_query", db_name);
	}

	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "teradata") {
		throw BinderException("Attached database \"%s\" does not refer to a Teradata database", db_name);
	}

	auto &td_catalog = catalog.Cast<TeradataCatalog>();
	auto &transaction = TeradataTransaction::Get(context, catalog);

	// Strip any trailing semicolons
	auto sql = input.inputs[1].GetValue<string>();
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}

	auto &con = transaction.GetConnection();

	vector<TeradataType> td_types;
	con.Prepare(sql, td_types, names);

	// Convert to DuckDB types
	for (auto &td_type : td_types) {
		return_types.push_back(td_type.ToDuckDB());
	}

	if (td_types.empty()) {
		throw BinderException("No fields returned by query \"%s\" - the query must be a SELECT statement that returns "
		                      "at least one column",
		                      sql);
	}

	auto result = make_uniq<TeradataBindData>();
	result->types = return_types;
	result->td_types = std::move(td_types);
	result->names = names;
	result->sql = sql;
	result->SetCatalog(td_catalog);

	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Bind Info
//----------------------------------------------------------------------------------------------------------------------
static BindInfo TeradataQueryBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TeradataBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.GetTable().get();
	return info;
}

//----------------------------------------------------------------------------------------------------------------------
// Init
//----------------------------------------------------------------------------------------------------------------------
struct TeradataQueryState final : GlobalTableFunctionState {
	unique_ptr<TeradataQueryResult> td_query;
	DataChunk scan_chunk;
	vector<column_t> column_ids;
};

static unique_ptr<GlobalTableFunctionState> TeradataQueryInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<TeradataBindData>();
	string sql = data.sql;

	// If we dont have a SQL string, just copy from the table
	if (sql.empty()) {
		if (data.table_name.empty()) {
			throw InvalidInputException("Teradata query requires a valid SQL string");
		}

		// If we get here, we have a table name, so we need to construct a SQL string
		sql = StringUtil::Format("SELECT * FROM %s.%s", data.schema_name, data.table_name);
	}

	// Also add simple filters if we got them
	if (input.filters) {
		const auto where_clause = TeradataFilter::Transform(input.column_ids, *input.filters, data.names);
		if (!where_clause.empty()) {
			sql += " WHERE " + where_clause;
		}
	}

	auto result = make_uniq<TeradataQueryState>();
	result->column_ids = input.column_ids;

	auto &con = data.GetCatalog()->GetConnection();
	result->td_query = con.Query(sql, data.is_materialized);

	// Check that the types are still the same, in case we need to rebind
	auto &td_types = result->td_query->GetTypes();

	for (idx_t i = 0; i < td_types.size(); i++) {
		// Compare the types of the query with the types we got during binding
		auto &expected = data.td_types[i];
		auto &actual = td_types[i];

		if (actual != expected) {
			if (expected.GetId() == TeradataTypeId::TIMESTAMP && actual.GetId() == TeradataTypeId::CHAR) {

				const auto is_ts_s = expected.GetWidth() == 6 && actual.GetLength() == 26;
				const auto is_ts_ms = expected.GetWidth() == 3 && actual.GetLength() == 23;
				const auto is_ts_us = expected.GetWidth() == 0 && actual.GetLength() == 19;

				if (is_ts_s || is_ts_ms || is_ts_us) {
					// Special case, this gets cast later
					continue;
				}
			}

			if (expected.GetId() == TeradataTypeId::TIMESTAMP_TZ && actual.GetId() == TeradataTypeId::CHAR &&
			    actual.GetLength() == 32) {
				// Special case, this gets cast later
				continue;
			}

			if (expected.GetId() == TeradataTypeId::TIME && actual.GetId() == TeradataTypeId::CHAR &&
			    actual.GetLength() == 15) {
				// Special case, this gets cast later
				continue;
			}

			if (expected.GetId() == TeradataTypeId::TIME_TZ && actual.GetId() == TeradataTypeId::CHAR &&
			    actual.GetLength() == 21) {
				// Special case, this gets cast later
				continue;
			}

			throw InvalidInputException("Teradata query schema has changed since it was last bound!\n"
			                            "Column: '%s' expected to be of type '%s' but received '%s'\n"
			                            "Please re-execute or re-prepare the query",
			                            data.names[i], expected.ToString(), actual.ToString());
		}
	}

	// Initialize the scan chunk
	result->td_query->InitScanChunk(result->scan_chunk);

	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Execute
//----------------------------------------------------------------------------------------------------------------------
static void TeradataQueryExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<TeradataQueryState>();

	// Scan the query result.
	state.td_query->Scan(state.scan_chunk);

	// Get the scan count
	const auto count = state.scan_chunk.size();

	// Cast all vectors
	// For most types, this is a no-op, the target just references the source.
	// But there are some special cases, like TIMESTAMP that always gets transmitted as VARCHAR.
	for (idx_t output_idx = 0; output_idx < state.scan_chunk.ColumnCount(); output_idx++) {
		const auto col_idx = state.column_ids[output_idx];

		if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			throw InvalidInputException(
			    "Teradata query does not support the row id column (rowid)");
		}

		auto &source = state.scan_chunk.data[col_idx]; // col ids
		auto &target = output.data[output_idx];

		VectorOperations::DefaultCast(source, target, count);
	}

	output.SetCardinality(count);
}

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------
TableFunction TeradataQueryFunction::GetFunction() {
	TableFunction function;
	function.name = "teradata_query";
	function.arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR};

	function.bind = TeradataQueryBind;
	function.init_global = TeradataQueryInit;
	function.function = TeradataQueryExec;
	function.get_bind_info = TeradataQueryBindInfo;
	function.projection_pushdown = true;
	function.filter_pushdown = true;

	return function;
}

void TeradataQueryFunction::Register(DatabaseInstance &db) {
	const auto function = TeradataQueryFunction::GetFunction();
	ExtensionUtil::RegisterFunction(db, function);
}

} // namespace duckdb


================================================
FILE: src/teradata_query.hpp
================================================
#pragma once
#include "teradata_catalog.hpp"
#include "teradata_type.hpp"

namespace duckdb {

class DatabaseInstance;
class TeradataTableEntry;

class TeradataBindData final : public TableFunctionData {
public:
	string schema_name;
	string table_name;
	string sql;

	vector<LogicalType> types;
	vector<string> names;
	vector<TeradataType> td_types;

	bool is_read_only = false;
	bool is_materialized = false;

	void SetCatalog(TeradataCatalog &catalog) {
		this->catalog = &catalog;
	}
	optional_ptr<TeradataCatalog> GetCatalog() const {
		return catalog;
	}
	void SetTable(TeradataTableEntry &table) {
		this->table = &table;
	}
	optional_ptr<TeradataTableEntry> GetTable() const {
		return table;
	}

private:
	optional_ptr<TeradataCatalog> catalog;
	optional_ptr<TeradataTableEntry> table;
};

struct TeradataQueryFunction {
	static TableFunction GetFunction();
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb


================================================
FILE: src/teradata_request.cpp
================================================
#include "teradata_request.hpp"
#include "teradata_type.hpp"
#include "teradata_connection.hpp"
#include "teradata_column_writer.hpp"

#include "util/binary_reader.hpp"

namespace duckdb {

TeradataRequestContext::TeradataRequestContext(const TeradataConnection &con) {
	Init(con);
}

void TeradataRequestContext::Init(const TeradataConnection &con) {
	memset(&dbc, 0, sizeof(DBCAREA)); // Clear the DBCAREA structure
	memset(&cnta, 0, sizeof(cnta)); // Clear the control area

	// Initialize
	int32_t result = EM_OK;

	// Set the total length
	dbc.total_len = sizeof(DBCAREA);

	DBCHINI(&result, cnta, &dbc);
	if (result != EM_OK) {
		throw IOException("Failed to initialize DBCAREA: %s", string(dbc.msg_text, dbc.msg_len));
	}

	// Resize the parcel buffer to the default size
	buffer.resize(8 * 1024); // 8KB default parcel size

	// Set the session ID, and change options
	dbc.change_opts = 'Y';

	dbc.i_sess_id = con.GetSessionId();
	dbc.resp_buf_len = con.GetBufferSize();
	dbc.resp_mode = 'I';     // 'Record' mode
	dbc.keep_resp = 'N';     // Only allow one sequential pass through the response buffer, then discard it
	dbc.save_resp_buf = 'N'; // Do not save the response buffer
	dbc.two_resp_bufs = 'Y'; // Use two response buffers

	dbc.use_presence_bits = 'Y';

	dbc.wait_for_resp = 'Y';
	dbc.wait_across_crash = 'N';
	dbc.tell_about_crash = 'Y';

	dbc.ret_time = 'N'; // Do not return the time
	dbc.date_form = 'T';

	dbc.var_len_req = 'N';   // Required to pass parameter descriptor length, p.120
	dbc.var_len_fetch = 'N'; // Do not use variable length fetch

	dbc.max_decimal_returned = 38; // DuckDB default decimals are 38 digits
}

void TeradataRequestContext::Execute(const string &sql) {
	BeginRequest(sql, 'E');

	MatchParcel(PclENDSTATEMENT);

	EndRequest();
}

void TeradataRequestContext::Execute(const string &sql, DataChunk &chunk, ArenaAllocator &arena,
                                     vector<unique_ptr<TeradataColumnWriter>> &writers) {

	const auto row_count = chunk.size();
	const auto col_count = chunk.ColumnCount();

	D_ASSERT(row_count > 0);
	D_ASSERT(writers.size() == chunk.ColumnCount());

	// Allocate space for the row pointers and lengths
	const auto record_array = reinterpret_cast<char **>(arena.AllocateAligned(row_count * sizeof(char *)));
	const auto cursor_array = reinterpret_cast<char **>(arena.AllocateAligned(row_count * sizeof(char *)));
	const auto length_array = reinterpret_cast<int32_t *>(arena.AllocateAligned(row_count * sizeof(int32_t)));

	// How many bytes are needed for the validity (presence) bits?
	const int32_t validity_bytes = (static_cast<int32_t>(col_count) + 7) / 8;

	// Initialize the writers
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		writers[col_idx]->Init(chunk.data[col_idx], row_count);
	}

	// Set the length of the rows to the length of the validity bits
	for (idx_t out_idx = 0; out_idx < row_count; out_idx++) {
		length_array[out_idx] = validity_bytes;
	}

	// First pass, column-wise compute the size of the rows
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		writers[col_idx]->ComputeSizes(row_count, length_array);
	}

	// Allocate space for the rows
	for (idx_t out_idx = 0; out_idx < row_count; out_idx++) {

		const auto row_length = length_array[out_idx];
		const auto row_record = reinterpret_cast<char *>(arena.AllocateAligned(row_length));

		record_array[out_idx] = row_record;
		cursor_array[out_idx] = row_record + validity_bytes;

		// initialize the presence bits to 0
		memset(row_record, 0, validity_bytes);
	}

	// Second pass, set the presence bits
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		writers[col_idx]->SetPresenceBits(row_count, col_idx, record_array);
	}

	// Now, third pass, actually write the rows
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		writers[col_idx]->EncodeVector(row_count, cursor_array);
	}

	dbc.using_data_ptr_array = reinterpret_cast<char *>(record_array);
	dbc.using_data_len_array = length_array;
	dbc.using_data_count = row_count;

	dbc.change_opts = 'Y';
	BeginRequest(sql, 'E');

	MatchParcel(PclENDSTATEMENT);

	for (idx_t i = 1; i < chunk.size(); i++) {
		MatchParcel(PclSUCCESS);
		MatchParcel(PclENDSTATEMENT);
	}

	EndRequest();
}

enum class TeradataColumnTypeVariant { STANDARD, NULLABLE, PARAM_IN, PARAM_INOUT, PARAM_OUT };

struct TeradataColumnType {
	TeradataType type;
	TeradataColumnTypeVariant variant;
};

static TeradataColumnType GetTeradataTypeFromParcel(const PclInt16 type) {
	// Nullable is 1 + the standard type
	// Param in is 500 + the standard type
	// Param inout is 501 + the standard type
	// Param out is 502 + the standard type
	switch (type) {
	case 400:
		return {TeradataTypeId::BLOB, TeradataColumnTypeVariant::STANDARD};
	case 448:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::STANDARD};
	case 449:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::NULLABLE};
	case 948:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::PARAM_IN};
	case 949:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::PARAM_INOUT};
	case 950:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::PARAM_OUT};

	case 452:
		return {TeradataTypeId::CHAR, TeradataColumnTypeVariant::STANDARD};
	case 453:
		return {TeradataTypeId::CHAR, TeradataColumnTypeVariant::NULLABLE};
	case 952:
		return {TeradataTypeId::CHAR, TeradataColumnTypeVariant::PARAM_IN};
	case 953:
		return {TeradataTypeId::CHAR, TeradataColumnTypeVariant::PARAM_INOUT};
	case 954:
		return {TeradataTypeId::CHAR, TeradataColumnTypeVariant::PARAM_OUT};

	// LONGVARCHAR
	case 456:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::STANDARD};
	case 457:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::NULLABLE};
	case 956:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::PARAM_IN};
	case 957:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::PARAM_INOUT};
	case 958:
		return {TeradataTypeId::VARCHAR, TeradataColumnTypeVariant::PARAM_OUT};

	// DECIMAL
	case 484:
		return {TeradataTypeId::DECIMAL, TeradataColumnTypeVariant::STANDARD};
	case 485:
		return {TeradataTypeId::DECIMAL, TeradataColumnTypeVariant::NULLABLE};
	case 984:
		return {TeradataTypeId::DECIMAL, TeradataColumnTypeVariant::PARAM_IN};
	case 985:
		return {TeradataTypeId::DECIMAL, TeradataColumnTypeVariant::PARAM_INOUT};
	case 986:
		return {TeradataTypeId::DECIMAL, TeradataColumnTypeVariant::PARAM_OUT};

	case 472:
		return {TeradataTypeId::LONGVARGRAPHIC, TeradataColumnTypeVariant::STANDARD};
	case 473:
		return {TeradataTypeId::LONGVARGRAPHIC, TeradataColumnTypeVariant::NULLABLE};
	case 972:
		return {TeradataTypeId::LONGVARGRAPHIC, TeradataColumnTypeVariant::PARAM_IN};
	case 973:
		return {TeradataTypeId::LONGVARGRAPHIC, TeradataColumnTypeVariant::PARAM_INOUT};
	case 974:
		return {TeradataTypeId::LONGVARGRAPHIC, TeradataColumnTypeVariant::PARAM_OUT};

	case 480:
		return {TeradataTypeId::FLOAT, TeradataColumnTypeVariant::STANDARD};
	case 481:
		return {TeradataTypeId::FLOAT, TeradataColumnTypeVariant::NULLABLE};
	case 980:
		return {TeradataTypeId::FLOAT, TeradataColumnTypeVariant::PARAM_IN};
	case 981:
		return {TeradataTypeId::FLOAT, TeradataColumnTypeVariant::PARAM_INOUT};
	case 982:
		return {TeradataTypeId::FLOAT, TeradataColumnTypeVariant::PARAM_OUT};

	case 496:
		return {TeradataTypeId::INTEGER, TeradataColumnTypeVariant::STANDARD};
	case 497:
		return {TeradataTypeId::INTEGER, TeradataColumnTypeVariant::NULLABLE};
	case 996:
		return {TeradataTypeId::INTEGER, TeradataColumnTypeVariant::PARAM_IN};
	case 997:
		return {TeradataTypeId::INTEGER, TeradataColumnTypeVariant::PARAM_INOUT};
	case 998:
		return {TeradataTypeId::INTEGER, TeradataColumnTypeVariant::PARAM_OUT};

	case 500:
		return {TeradataTypeId::SMALLINT, TeradataColumnTypeVariant::STANDARD};
	case 501:
		return {TeradataTypeId::SMALLINT, TeradataColumnTypeVariant::NULLABLE};
	case 1000:
		return {TeradataTypeId::SMALLINT, TeradataColumnTypeVariant::PARAM_IN};
	case 1001:
		return {TeradataTypeId::SMALLINT, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1002:
		return {TeradataTypeId::SMALLINT, TeradataColumnTypeVariant::PARAM_OUT};

	case 600:
		return {TeradataTypeId::BIGINT, TeradataColumnTypeVariant::STANDARD};
	case 601:
		return {TeradataTypeId::BIGINT, TeradataColumnTypeVariant::NULLABLE};
	case 1100:
		return {TeradataTypeId::BIGINT, TeradataColumnTypeVariant::PARAM_IN};
	case 1101:
		return {TeradataTypeId::BIGINT, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1102:
		return {TeradataTypeId::BIGINT, TeradataColumnTypeVariant::PARAM_OUT};

	case 688:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::STANDARD};
	case 689:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::NULLABLE};
	case 1188:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::PARAM_IN};
	case 1189:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1190:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::PARAM_OUT};

	case 692:
		return {TeradataTypeId::BYTE, TeradataColumnTypeVariant::STANDARD};
	case 693:
		return {TeradataTypeId::BYTE, TeradataColumnTypeVariant::NULLABLE};
	case 1192:
		return {TeradataTypeId::BYTE, TeradataColumnTypeVariant::PARAM_IN};
	case 1193:
		return {TeradataTypeId::BYTE, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1194:
		return {TeradataTypeId::BYTE, TeradataColumnTypeVariant::PARAM_OUT};

	// Just treat these as varchar with the max length
	case 697:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::STANDARD};
	case 698:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::NULLABLE};
	case 1197:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::PARAM_IN};
	case 1198:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1199:
		return {TeradataTypeId::VARBYTE, TeradataColumnTypeVariant::PARAM_OUT};

	// DATE_A (ansidate)
	case 748:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::STANDARD};
	case 749:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::NULLABLE};
	case 1248:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::PARAM_IN};
	case 1249:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1250:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::PARAM_OUT};

	// DATE_T (integerdate)
	case 752:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::STANDARD};
	case 753:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::NULLABLE};
	case 1252:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::PARAM_IN};
	case 1253:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1254:
		return {TeradataTypeId::DATE, TeradataColumnTypeVariant::PARAM_OUT};

	case 756:
		return {TeradataTypeId::BYTEINT, TeradataColumnTypeVariant::STANDARD};
	case 757:
		return {TeradataTypeId::BYTEINT, TeradataColumnTypeVariant::NULLABLE};
	case 1256:
		return {TeradataTypeId::BYTEINT, TeradataColumnTypeVariant::PARAM_IN};
	case 1257:
		return {TeradataTypeId::BYTEINT, TeradataColumnTypeVariant::PARAM_INOUT};
	case 1258:
		return {TeradataTypeId::BYTEINT, TeradataColumnTypeVariant::PARAM_OUT};
	default:
		// TODO: More types
		throw NotImplementedException("Unknown data type: %d", type);
	}
}

void TeradataRequestContext::Prepare(const string &sql, vector<TeradataType> &types, vector<string> &names) {
	BeginRequest(sql, 'P');

	// Fetch and match the second parcel
	MatchParcel(PclPREPINFO);

	// Parse the columns
	BinaryReader reader(buffer.data(), dbc.fet_ret_data_len);

	const auto prep_info = reader.Read<CliPrepInfoType>();
	for (int16_t col_idx = 0; col_idx < prep_info.ColumnCount; col_idx++) {
		const auto col_info = reader.Read<CliPrepColInfoType>();

		auto col_type = GetTeradataTypeFromParcel(col_info.DataType);
		auto &td_type = col_type.type;

		// Try to get the type mods from the format
		if (td_type.HasLengthModifier()) {
			td_type.SetLength(col_info.DataLen);
		} else if (td_type.IsDecimal()) {
			// The docs seem wrong here.
			// - The first byte is the fractional digits
			// - The second byte is the integral digits
			// This is the opposite of what the docs say
			const uint8_t scale = col_info.DataLen & 0xFF;
			const uint8_t width = col_info.DataLen >> 8;

			td_type.SetWidth(width);
			td_type.SetScale(scale);
		}

		/*
		    "ColumnName specifies a column's name, consisting of length in bytes of the name followed by that name in
		    characters from the current session character set."

		    TODO: We need to do the charset conversion here if needed.
		 */

		const auto name_len = reader.Read<int16_t>();
		const auto name_str = reader.ReadBytes(name_len);

		names.emplace_back(name_str, name_len);

		// TODO: Do something with the format?
		const auto format_len = reader.Read<int16_t>();
		// const auto format_str = reader.ReadBytes(format_len);
		reader.Skip(format_len);

		// TODO: Do something with the title?
		const auto title_len = reader.Read<int16_t>();
		// const auto title_str = reader.ReadBytes(title_len);
		reader.Skip(title_len);

		types.push_back(td_type);
	}

	// End statement
	MatchParcel(PclENDSTATEMENT);

	// End request
	MatchParcel(PclENDREQUEST);

	is_open = false;
}

void TeradataRequestContext::Query(const string &sql, vector<TeradataType> &types) {

	BeginRequest(sql, 'E');

	const auto parcel = FetchParcel();

	if (parcel == PclENDSTATEMENT) {
		// No data
		EndRequest();
		return;
	}

	// Otherwise, return records
	if (parcel != PclDATAINFO) {
		throw IOException("Expected PclDATAINFO, got %d", parcel);
	}

	// Now parse the data info and set types
	// TODO:
	BinaryReader reader(buffer.data(), dbc.fet_ret_data_len);

	// Read the number of columns
	const auto field_count = reader.Read<uint16_t>();
	for (uint16_t field_idx = 0; field_idx < field_count; field_idx++) {
		const auto id_type = reader.Read<uint16_t>();

		auto col_type = GetTeradataTypeFromParcel(id_type);
		auto &td_type = col_type.type;

		if (td_type.IsDecimal()) {
			// The docs seem wrong here.
			// - The first byte is the fractional digits
			// - The second byte is the integral digits
			// This is the opposite of what the docs say
			// const auto scale = reader.Read<uint8_t>();
			// const auto width = reader.Read<uint8_t>();

			const auto length = reader.Read<uint16_t>();
			const uint8_t scale = length & 0xFF;
			const uint8_t width = length >> 8;

			td_type.SetWidth(width);
			td_type.SetScale(scale);
		} else {
			const auto length = reader.Read<uint16_t>();
			if (td_type.HasLengthModifier()) {
				td_type.SetLength(length);
			}
		}

		types.push_back(td_type);
	}
}

void TeradataRequestContext::MatchParcel(uint16_t flavor) {
	const auto result = FetchParcel();

	if (result != flavor) {
		if (result == PclERROR) {
			// Parse the error message
			BinaryReader reader(buffer.data(), dbc.fet_ret_data_len);
			const auto stmt_no = reader.Read<uint16_t>();
			const auto info = reader.Read<uint16_t>();
			const auto code = reader.Read<uint16_t>();
			const auto msg_len = reader.Read<uint16_t>();
			const auto msg = reader.ReadBytes(msg_len);
			const auto msg_str = string(msg, msg_len);
			throw IOException("Expected parcel flavor %d, got error: stmt_no: %d, info: %d, code: %d, msg: '%s'",
			                  flavor, stmt_no, info, code, msg_str);
		}

		throw IOException("Expected parcel flavor %d, got %d", flavor, result);
	}
}

uint16_t TeradataRequestContext::FetchParcel() {
	dbc.func = DBFFET;
	dbc.fet_data_ptr = buffer.data();
	dbc.fet_max_data_len = static_cast<int32_t>(buffer.size());

	int32_t result = EM_OK;
	DBCHCL(&result, cnta, &dbc);

	while (result == BUFOVFLOW) {
		buffer.resize(dbc.fet_ret_data_len);
		dbc.fet_data_ptr = buffer.data();
		dbc.fet_max_data_len = buffer.size();
		DBCHCL(&result, cnta, &dbc);
	}

	while (result == EM_NODATA) {
		// No data, try again
		// TODO: Yield the DuckDB task here, dont just busy loop
		DBCHCL(&result, cnta, &dbc);
	}

	if (result != EM_OK) {
		throw IOException("Failed to fetch result: %s", string(dbc.msg_text, dbc.msg_len));
	}

	if (dbc.fet_parcel_flavor == PclFAILURE) {
		BinaryReader reader(buffer.data(), dbc.fet_ret_data_len);
		const auto stmt_no = reader.Read<uint16_t>();
		const auto info = reader.Read<uint16_t>();
		const auto code = reader.Read<uint16_t>();
		const auto msg_len = reader.Read<uint16_t>();
		const auto msg = reader.ReadBytes(msg_len);

		// Try to detect some common error codes
		switch (code) {
		case 2801:
		case 2802:
		case 2803:
			throw ConstraintException(string(msg, msg_len));
		default:
			throw IOException("Teradata request failed, stmt_no: %d, info: %d, code: %d, msg: '%s'", stmt_no, info,
			                  code, string(msg, msg_len));
		}
	}

	return dbc.fet_parcel_flavor;
}

bool TeradataRequestContext::Fetch(DataChunk &chunk, const vector<unique_ptr<TeradataColumnReader>> &readers) {
	if (!is_open) {
		chunk.SetCardinality(0);
		return false;
	}

	idx_t row_idx = 0;

	while (row_idx < chunk.GetCapacity()) {
		const auto parcel = FetchParcel();

		switch (parcel) {
		case PclRECORD: {
			// Offset null bytes
			const auto null_bytes = (chunk.ColumnCount() + 7) / 8;
			const auto data_ptr = buffer.data() + null_bytes;
			const auto data_len = dbc.fet_ret_data_len;

			// Parse the record
			BinaryReader reader(data_ptr, data_len);

			for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
				// The NullIndicators Field contains one bit for each item in the DataField,
				// stored in the minimum number of 8-bit bytes required to hold them,
				// with the unused bits in the rightmost byte set to zero.
				// Each bit is matched on a positional basis to an item in the Data Field.
				// That is, the ith bit in the NullIndicators Field corresponds to the ith item in the Data Field.
				const auto byte_idx = col_idx / 8;
				const auto bit_idx = (7 - (col_idx % 8));
				const bool is_null = (buffer[byte_idx] & (1 << bit_idx)) != 0;

				auto &col_vec = chunk.data[col_idx];

				// Call the reader to decode the column
				readers[col_idx]->Decode(reader, col_vec, row_idx, is_null);
			}

			// Increment row id
			row_idx++;

		} break;
		case PclENDSTATEMENT: {
			EndRequest();
			chunk.SetCardinality(row_idx);
			return true;
		}
		default: {
			throw IOException("Unexpected parcel flavor %d", parcel);
		}
		}
	}
	chunk.SetCardinality(row_idx);
	return true;
}

unique_ptr<ColumnDataCollection> TeradataRequestContext::FetchAll(const vector<TeradataType> &types) {
	if (!is_open) {
		throw IOException("Teradata request is not open");
	}
	if (types.empty()) {
		throw IOException("Cannot fetch all data without types");
	}

	// Convert teradatat types to DuckDB types
	vector<LogicalType> duck_types;
	for (const auto &td_type : types) {
		duck_types.push_back(td_type.ToDuckDB());
	}

	// Create a CDC to hold our result
	auto result = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), duck_types);

	// Initialize CDC append and payload chunk
	ColumnDataAppendState append_state;
	result->InitializeAppend(append_state);

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), duck_types);

	// Initialize readers
	vector<unique_ptr<TeradataColumnReader>> readers;
	for (const auto &td_type : types) {
		readers.push_back(TeradataColumnReader::Make(td_type));
	}

	// Fetch chunk by chunk and append to the CDC
	while (Fetch(chunk, readers)) {
		result->Append(append_state, chunk);
		chunk.Reset();
	}

	return result;
}

void TeradataRequestContext::BeginRequest(const string &sql, char mode) {
	// Setup the request
	dbc.func = DBFIRQ;     // initiate request
	dbc.change_opts = 'Y'; // change options to indicate that we want to change the options (to indicator mode)
	dbc.req_proc_opt = mode;

	// prepare mode (with params)	= 'S'
	// prepare mode (no params)		= 'P'
	// execute mode					= 'E'

	// Pass the SQL
	dbc.req_ptr = const_cast<char *>(sql.c_str());
	dbc.req_len = static_cast<int32_t>(sql.size());

	// Initialize request
	int32_t result = EM_OK;
	DBCHCL(&result, cnta, &dbc);
	if (result != EM_OK) {
		throw IOException("Failed to execute SQL: %s", string(dbc.msg_text, dbc.msg_len));
	}

	is_open = true;
	dbc.i_req_id = dbc.o_req_id;

	// Fetch and match the first parcel
	MatchParcel(PclSUCCESS);
}

void TeradataRequestContext::EndRequest() {
	// End request
	MatchParcel(PclENDREQUEST);
	is_open = false;
}

void TeradataRequestContext::Close() {
	if (!is_open) {
		return;
	}

	int32_t result = EM_OK;

	dbc.func = DBFERQ;
	DBCHCL(&result, cnta, &dbc);
	if (result != EM_OK) {
		throw IOException("Failed to close Teradata request");
	}
	is_open = false;
}

TeradataRequestContext::~TeradataRequestContext() {
	if (is_open) {
		Close();
	}
}

} // namespace duckdb


================================================
FILE: src/teradata_request.hpp
================================================
#pragma once

#include "teradata_common.hpp"
#include "teradata_type.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

class TeradataConnection;
class TeradataColumnReader;
class TeradataColumnWriter;

class TeradataRequestContext {
public:
	explicit TeradataRequestContext(const TeradataConnection &con);
	void Init(const TeradataConnection &con);

	// Execute a statement without returning any data
	void Execute(const string &sql);

	// Execute a paramterized statment, once for each row in the chunk
	void Execute(const string &sql, DataChunk &chunk, ArenaAllocator &arena,
	             vector<unique_ptr<TeradataColumnWriter>> &writers);

	// Prepare a statement, returning the types of the result set
	void Prepare(const string &sql, vector<TeradataType> &types, vector<string> &names);

	// Issue a SQL query and return the types of the result set
	void Query(const string &sql, vector<TeradataType> &types);

	// Fetch the next data chunk after calling Query. Returns true if there is more data to fetch
	bool Fetch(DataChunk &chunk, const vector<unique_ptr<TeradataColumnReader>> &readers);

	// Fetch all data after calling Query, into a ColumnDataCollection.
	unique_ptr<ColumnDataCollection> FetchAll(const vector<TeradataType> &types);
	~TeradataRequestContext();

private:
	void BeginRequest(const string &sql, char mode);
	void EndRequest();
	void Close();
	void MatchParcel(uint16_t flavor);
	uint16_t FetchParcel();

	DBCAREA dbc = {};
	char cnta[4] = {};
	vector<char> buffer;
	bool is_open = false;
};

} // namespace duckdb


================================================
FILE: src/teradata_result.hpp
================================================
#pragma once

#include "teradata_type.hpp"
#include "teradata_request.hpp"
#include "teradata_column_reader.hpp"

namespace duckdb {

class TeradataQueryResult;

//----------------------------------------------------------------------------------------------------------------------
// Query Result Iterator
//----------------------------------------------------------------------------------------------------------------------
class TeradataQueryResultIterator {
public:
	explicit TeradataQueryResultIterator(TeradataQueryResult *result_p);
	TeradataQueryResultIterator &operator++();
	bool operator!=(const TeradataQueryResultIterator &other) const;
	DataChunk &operator*() const;
	void Next();

private:
	TeradataQueryResult *result;
	shared_ptr<DataChunk> scan_chunk;
	idx_t row_index;
};

class TeradataQueryResultIteratorPair {
public:
	explicit TeradataQueryResultIteratorPair(TeradataQueryResult &result_p) : result(result_p) {
	}

	TeradataQueryResultIterator begin() {
		return TeradataQueryResultIterator(&result);
	}
	TeradataQueryResultIterator end() {
		return TeradataQueryResultIterator(nullptr);
	}

private:
	TeradataQueryResult &result;
};

//----------------------------------------------------------------------------------------------------------------------
// Query Result
//----------------------------------------------------------------------------------------------------------------------
class TeradataQueryResult {
public:
	virtual ~TeradataQueryResult() = default;
	// Not copyable or movable
	TeradataQueryResult(const TeradataQueryResult &) = delete;
	TeradataQueryResult &operator=(const TeradataQueryResult &) = delete;
	TeradataQueryResult(TeradataQueryResult &&) = delete;
	TeradataQueryResult &operator=(TeradataQueryResult &&) = delete;

	// Is this result materialized?
	virtual bool IsMaterialized() const = 0;

	// Get the types of the result set
	vector<TeradataType> &GetTypes() {
		return types;
	}
	// Consumes this result, returning the next chunk of data
	// Returns false if there is no more data to fetch
	virtual bool Scan(DataChunk &chunk) = 0;

	void InitScanChunk(DataChunk &chunk) const {
		vector<LogicalType> duck_types;
		for (const auto &td_type : types) {
			duck_types.push_back(td_type.ToDuckDB());
		}
		chunk.Initialize(Allocator::DefaultAllocator(), duck_types);
	}

	TeradataQueryResultIteratorPair Chunks() {
		return TeradataQueryResultIteratorPair(*this);
	}

protected:
	explicit TeradataQueryResult(vector<TeradataType> types_p) : types(std::move(types_p)) {
	}

	vector<TeradataType> types;
};

class StreamingTeradataQueryResult final : public TeradataQueryResult {
public:
	StreamingTeradataQueryResult(vector<TeradataType> types_p, unique_ptr<TeradataRequestContext> ctx_p)
	    : TeradataQueryResult(std::move(types_p)), ctx(std::move(ctx_p)) {

		for (auto &type : types) {
			readers.push_back(TeradataColumnReader::Make(type));
		}
	}

	bool IsMaterialized() const override {
		return false;
	}

	bool Scan(DataChunk &chunk) override {
		return ctx->Fetch(chunk, readers);
	}

private:
	unique_ptr<TeradataRequestContext> ctx;
	vector<unique_ptr<TeradataColumnReader>> readers;
};

class MaterializedTeradataQueryResult final : public TeradataQueryResult {
public:
	MaterializedTeradataQueryResult(vector<TeradataType> types_p, unique_ptr<ColumnDataCollection> cdc_p)
	    : TeradataQueryResult(std::move(types_p)), cdc(std::move(cdc_p)) {

		// Initialize scan state
		cdc->InitializeScan(scan_state);
	}

	bool IsMaterialized() const override {
		return true;
	}

	bool Scan(DataChunk &chunk) override {
		return cdc->Scan(scan_state, chunk);
	}

private:
	unique_ptr<ColumnDataCollection> cdc;
	ColumnDataScanState scan_state;
};

//----------------------------------------------------------------------------------------------------------------------
// Inlined methods
//----------------------------------------------------------------------------------------------------------------------
inline TeradataQueryResultIterator::TeradataQueryResultIterator(TeradataQueryResult *result_p)
    : result(result_p), row_index(0) {

	if (!result) {
		return;
	}

	scan_chunk = make_shared_ptr<DataChunk>();
	result->InitScanChunk(*scan_chunk);
	result->Scan(*scan_chunk);
	row_index += scan_chunk->size();
}

inline DataChunk &TeradataQueryResultIterator::operator*() const {
	return *scan_chunk;
}

inline void TeradataQueryResultIterator::Next() {
	if (!result) {
		return;
	}
	if (!result->Scan(*scan_chunk)) {
		result = nullptr;
		row_index = 0;
	} else {
		row_index += scan_chunk->size();
	}
}

inline bool TeradataQueryResultIterator::operator!=(const TeradataQueryResultIterator &other) const {
	return result != other.result || row_index != other.row_index;
}

inline TeradataQueryResultIterator &TeradataQueryResultIterator::operator++() {
	Next();
	return *this;
}

} // namespace duckdb


================================================
FILE: src/teradata_schema_entry.cpp
================================================
#include "teradata_schema_entry.hpp"
#include "teradata_table_entry.hpp"

#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

TeradataSchemaEntry::TeradataSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this), indexes(*this) {
}

void TeradataSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void TeradataSchemaEntry::Scan(ClientContext &context, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		tables.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<TeradataTableEntry>()); });
		break;
	case CatalogType::INDEX_ENTRY:
		indexes.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<TeradataIndexEntry>()); });
	default:
		break; // throw InternalException("Type not supported for TeradataSchemaEntry::Scan");
	}
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                            TableCatalogEntry &table) {

	return indexes.CreateIndex(transaction.GetContext(), info, table);
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                               CreateFunctionInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateFunction");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                            BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("TeradataSchemaEntry::CreateTable REPLACE ON CONFLICT");
	}
	return tables.CreateTable(transaction.GetContext(), info);
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateView");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                               CreateSequenceInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateSequence");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                    CreateTableFunctionInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateTableFunction");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                   CreateCopyFunctionInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateCopyFunction");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                     CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreatePragmaFunction");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                CreateCollationInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateCollation");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::CreateType");
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                            const EntryLookupInfo &lookup_info) {
	switch (lookup_info.GetCatalogType()) {
	case CatalogType::TABLE_ENTRY:
		return tables.GetEntry(transaction.GetContext(), lookup_info.GetEntryName());
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return TryLoadBuiltInFunction(lookup_info.GetEntryName());
	default:
		return nullptr;
	}
}

void TeradataSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	info.schema = name;
	switch (info.type) {
	case CatalogType::TABLE_ENTRY:
		tables.DropEntry(context, info);
		break;
	default:
		throw NotImplementedException("TeradataSchemaEntry::DropEntry");
	}
}

void TeradataSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("TeradataSchemaEntry::Alter");
}

//----------------------------------------------------------------------------------------------------------------------
// Built-in Functions
//----------------------------------------------------------------------------------------------------------------------

// clang-format off
static const DefaultTableMacro td_table_macros[] = {
	{DEFAULT_SCHEMA, "query", {"sql", nullptr}, {{nullptr, nullptr}}, "FROM teradata_query({CATALOG}, sql)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

optional_ptr<CatalogEntry> TeradataSchemaEntry::LoadBuiltInFunction(DefaultTableMacro macro) {
	string macro_def = macro.macro;
	macro_def = StringUtil::Replace(macro_def, "{CATALOG}", KeywordHelper::WriteQuoted(catalog.GetName(), '\''));
	macro_def = StringUtil::Replace(macro_def, "{SCHEMA}", KeywordHelper::WriteQuoted(name, '\''));
	macro.macro = macro_def.c_str();

	auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(macro);
	auto table_macro =
	    make_uniq_base<CatalogEntry, TableMacroCatalogEntry>(catalog, *this, info->Cast<CreateMacroInfo>());
	auto result = table_macro.get();
	default_function_map.emplace(macro.name, std::move(table_macro));
	return result;
}

optional_ptr<CatalogEntry> TeradataSchemaEntry::TryLoadBuiltInFunction(const string &entry_name) {
	lock_guard<mutex> guard(default_function_lock);
	auto entry = default_function_map.find(entry_name);
	if (entry != default_function_map.end()) {
		return entry->second.get();
	}
	for (idx_t index = 0; td_table_macros[index].name != nullptr; index++) {
		if (StringUtil::CIEquals(td_table_macros[index].name, entry_name)) {
			return LoadBuiltInFunction(td_table_macros[index]);
		}
	}
	return nullptr;
}

} // namespace duckdb


================================================
FILE: src/teradata_schema_entry.hpp
================================================
#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

#include "teradata_table_set.hpp"
#include "teradata_index_set.hpp"

namespace duckdb {

// Teradata doesnt have the equivalent of a "Schema" in the traditional sense, therefore we
// treat "Databases" as schemas.
class TeradataSchemaEntry final : public SchemaCatalogEntry {
public:
	TeradataSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	TeradataTableSet tables;
	TeradataIndexSet indexes;

	// Builtin default functions
	mutex default_function_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> default_function_map;

	optional_ptr<CatalogEntry> TryLoadBuiltInFunction(const string &entry_name);
	optional_ptr<CatalogEntry> LoadBuiltInFunction(DefaultTableMacro macro);
};

} // namespace duckdb


================================================
FILE: src/teradata_schema_set.cpp
================================================
#include "teradata_schema_set.hpp"
#include "teradata_catalog.hpp"
#include "teradata_schema_entry.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

TeradataSchemaSet::TeradataSchemaSet(Catalog &catalog_p, string schema_to_load_p)
    : TeradataCatalogSet(catalog_p), schema_to_load(std::move(schema_to_load_p)) {
}

void TeradataSchemaSet::LoadEntries(ClientContext &context) {
	const auto &td_catalog = catalog.Cast<TeradataCatalog>();

	// We have to issue a query to get the list of schemas
	auto &conn = td_catalog.GetConnection();

	// Select the schema name and the comment string
	// TODO: Do something like this to pull in the immediate children of the database as schemas
	// string query = "SELECT C.Child, D.CommentString FROM DBC.ChildrenV AS C JOIN DBC.DatabasesV AS D ON
	// D.DatabaseName = C.Child WHERE C.Parent = "; query += KeywordHelper::WriteQuoted(schema_to_load); query += UNION
	// (SELECT DatabaseName, CommentString FROM DBC.DatabasesV WHERE DatabaseName = ''dbc'')';

	string query = "SELECT DatabaseName, CommentString FROM DBC.DatabasesV";
	query += " WHERE DatabaseName = " + KeywordHelper::WriteQuoted(schema_to_load);

	const auto result = conn.Query(query, false);

	// Now iterate over the result and create the schema entries
	for (auto &chunk : result->Chunks()) {
		chunk.Flatten();
		const auto count = chunk.size();
		auto &name_vec = chunk.data[0];
		auto &comment_vec = chunk.data[1];

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {

			const auto name = FlatVector::GetData<string_t>(name_vec)[row_idx].GetString();

			CreateSchemaInfo info;
			info.catalog = catalog.GetName();
			info.schema = name;
			info.comment = comment_vec.GetValue(row_idx);

			// Pass the entry along
			auto entry = make_uniq<TeradataSchemaEntry>(catalog, info);
			CreateEntry(std::move(entry));
		}
	}
}

} // namespace duckdb


================================================
FILE: src/teradata_schema_set.hpp
================================================
#pragma once

#include "teradata_catalog_set.hpp"

namespace duckdb {

// Set of schemas in the catalog
class TeradataSchemaSet final : public TeradataCatalogSet {
public:
	explicit TeradataSchemaSet(Catalog &catalog, string schema_to_load);

protected:
	// Load all schemas in this catalog
	void LoadEntries(ClientContext &context) override;

	//! Schema to load - if empty loads all schemas (default behavior)
	string schema_to_load;
};

} // namespace duckdb


================================================
FILE: src/teradata_secret.cpp
================================================
#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension_util.hpp"

#include "teradata_secret.hpp"

namespace duckdb {

const char* TeradataSecret::TYPE = "teradata";

static unique_ptr<BaseSecret> CreateTeradataSecretFunction(ClientContext &context, CreateSecretInput &input) {
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, TeradataSecret::TYPE, "config", input.name);

	bool has_host = false;
	bool has_user = false;
	bool has_database = false;
	bool has_password = false;

	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
			has_host = true;
		} else if (lower_name == "user") {
			result->secret_map["user"] = named_param.second.ToString();
			has_user = true;
		} else if (lower_name == "database") {
			result->secret_map["database"] = named_param.second.ToString();
			has_database = true;
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
			has_password = true;
		} else {
			throw InternalException("Unknown named parameter passed to teradata secret: " + lower_name);
		}
	}

	if (!has_host || !has_user || !has_database || !has_password) {
		throw InvalidInputException(
		    "Teradata secret must contain 'HOST', 'USER', 'DATABASE', and 'PASSWORD' parameters");
	}

	// Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

bool TeradataSecret::TryGet(ClientContext &context, const string &name, TeradataSecret &out) {
	auto &secret_manager = SecretManager::Get(context);
	const auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	const auto secret_entry = secret_manager.GetSecretByName(transaction, name);
	if (!secret_entry) {
		return false;
	}
	if (secret_entry->secret->GetType() != TeradataSecret::TYPE) {
		return false;
	}
	const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	out.host = kv_secret.TryGetValue("host", true).ToString();
	out.user = kv_secret.TryGetValue("user", true).ToString();
	out.database = kv_secret.TryGetValue("database", true).ToString();
	out.password = kv_secret.TryGetValue("password", true).ToString();

	return true;
}

void TeradataSecret::Register(DatabaseInstance &db) {

	// Register the teradata secret type
	SecretType secret_type;
	secret_type.name = TeradataSecret::TYPE;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(db, secret_type);

	// Register the teradata secret creation function
	CreateSecretFunction secret_constructor;
	secret_constructor.secret_type = TeradataSecret::TYPE;
	secret_constructor.provider = "config";
	secret_constructor.function = CreateTeradataSecretFunction;
	secret_constructor.named_parameters["host"] = LogicalType::VARCHAR;
	secret_constructor.named_parameters["password"] = LogicalType::VARCHAR;
	secret_constructor.named_parameters["user"] = LogicalType::VARCHAR;
	secret_constructor.named_parameters["database"] = LogicalType::VARCHAR;

	ExtensionUtil::RegisterFunction(db, secret_constructor);
}

} // namespace duckdb


================================================
FILE: src/teradata_secret.hpp
================================================
#pragma once

namespace duckdb {

class DatabaseInstance;

struct TeradataSecret {
	static const char* TYPE;

	static void Register(DatabaseInstance &db);
	static bool TryGet(ClientContext &context, const string &name, TeradataSecret &out);

	string host;
	string user;
	string database;
	string password;
};

} // namespace duckdb


================================================
FILE: src/teradata_storage.cpp
================================================
#include "teradata_storage.hpp"
#include "teradata_catalog.hpp"
#include "teradata_secret.hpp"
#include "teradata_transcation_manager.hpp"

#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> TeradataAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AccessMode access_mode) {

	// Attempt to load the Teradata CLIV2 library
	TeradataCLIV2::Load();

	string host;
	string user;
	string database;
	string password;

	// First, check if we have a secret, if so use that for the default options
	case_insensitive_map_t<reference<Value>> options;
	for (auto &option : info.options) {
		options.emplace(option.first, option.second);
	}

	auto secret_name_opt = options.find("secret");
	if (secret_name_opt != options.end()) {
		auto secret_name = secret_name_opt->second.get().ToString();

		TeradataSecret secret;
		if (!TeradataSecret::TryGet(context, secret_name, secret)) {
			throw InvalidInputException("No Teradata secret found with name: " + secret_name);
		}

		host = secret.host;
		user = secret.user;
		database = secret.database;
		password = secret.password;
	}

	// Now, try to get options from the ATTACH info
	auto host_opt = options.find("host");
	if (host_opt != options.end()) {
		host = host_opt->second.get().ToString();
	}

	auto user_opt = options.find("user");
	if (user_opt != options.end()) {
		user = user_opt->second.get().ToString();
	}

	auto database_opt = options.find("database");
	if (database_opt != options.end()) {
		database = database_opt->second.get().ToString();
	}

	auto password_opt = options.find("password");
	if (password_opt != options.end()) {
		password = password_opt->second.get().ToString();
	}

	idx_t buffer_size = 1024 * 1024; // Default buffer size is 1MB
	auto buffer_size_opt = options.find("buffer_size");
	if (buffer_size_opt != options.end()) {
		auto &buffer_size_val = buffer_size_opt->second.get();
		if (buffer_size_val.type().id() != LogicalTypeId::INTEGER) {
		}
		auto buffer_size_int = buffer_size_val.GetValue<int32_t>();
		if (buffer_size_int <= 0) {
			throw InvalidInputException("Teradata ATTACH option 'buffer_size' must be a positive integer");
		}
		buffer_size = UnsafeNumericCast<idx_t>(buffer_size_int);
	}

	// Lastly, parse parameters from the logon string
	if (!info.path.empty()) {

		string host_str;
		string user_str;
		string pass_str;

		auto beg = info.path.c_str();
		auto end = beg + info.path.size();
		auto ptr = beg;

		// Parse the logon string format: "host/user[,password][,account]"
		// although, 'account' is unused by us for now.
		while (ptr != end && *ptr != '/') {
			host_str += *ptr;
			ptr++;
		}
		if (ptr != end && *ptr == '/') {
			ptr++; // Skip the '/'

			while (ptr != end && *ptr != ',') {
				user_str += *ptr;
				ptr++;
			}
			if (ptr != end && *ptr == ',') {
				ptr++; // Skip the ','
				while (ptr != end && *ptr != ',') {
					pass_str += *ptr;
					ptr++;
				}
			}
		}

		if (!host_str.empty()) {
			host = host_str;
		}
		if (!user_str.empty()) {
			user = user_str;
		}
		if (!pass_str.empty()) {
			password = pass_str;
		}
	}

	if (user.empty()) {
		throw InvalidInputException("Teradata ATTACH must contain a 'user', either in the logon string, passed as "
		                            "options or in defined in a secret");
	}
	if (host.empty()) {
		throw InvalidInputException("Teradata ATTACH must contain a 'host', either in the logon string, passed as "
		                            "options or in defined in a secret");
	}
	if (database.empty()) {
		database = user; // Default to user database if not specified
	}

	// Construct the connection string
	auto connection_string = host + "/" + user;
	if (!password.empty()) {
		connection_string += "," + password;
	}

	// Create the catalog and connect to the teradata system
	auto result = make_uniq<TeradataCatalog>(db, connection_string, database, buffer_size);

	// Set the database path
	result->GetConnection().Execute("DATABASE " + KeywordHelper::WriteOptionallyQuoted(database) + ";");

	return std::move(result);
}

static unique_ptr<TransactionManager> TeradataCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	auto &td_catalog = catalog.Cast<TeradataCatalog>();
	return make_uniq<TeradataTransactionManager>(db, td_catalog);
}

TeradataStorageExtension::TeradataStorageExtension() {
	attach = TeradataAttach;
	create_transaction_manager = TeradataCreateTransactionManager;
}

} // namespace duckdb


================================================
FILE: src/teradata_storage.hpp
================================================
#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class TeradataStorageExtension final : public StorageExtension {
public:
	TeradataStorageExtension();
};

} // namespace duckdb


================================================
FILE: src/teradata_table_entry.cpp
================================================
#include "teradata_table_entry.hpp"
#include "teradata_catalog.hpp"
#include "teradata_transaction.hpp"
#include "teradata_query.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

TeradataTableEntry::TeradataTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {

	for (auto &col : columns.Logical()) {
		// Use the default mapping from DuckDB to Teradata types if were creating a table from within duckdb
		teradata_types.push_back(TeradataType::FromDuckDB(col.GetType()));
	}
}

TeradataTableEntry::TeradataTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, TeradataTableInfo &info)
    : TableCatalogEntry(catalog, schema, info), teradata_types(std::move(info.teradata_types)) {
	D_ASSERT(teradata_types.size() == columns.LogicalColumnCount());
}

unique_ptr<BaseStatistics> TeradataTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction TeradataTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &td_catalog = catalog.Cast<TeradataCatalog>();
	auto &transaction = TeradataTransaction::Get(context, catalog);

	auto result = make_uniq<TeradataBindData>();

	// Setup the bind data
	result->schema_name = schema.name;
	result->table_name = name;
	result->SetCatalog(td_catalog);
	result->SetTable(*this);

	for (idx_t col_idx = 0; col_idx < columns.LogicalColumnCount(); col_idx++) {
		auto &col = columns.GetColumnMutable(LogicalIndex(col_idx));
		result->names.push_back(col.GetName());
		result->types.push_back(col.GetType());
	}

	result->is_read_only = transaction.IsReadOnly();
	result->is_materialized = false;
	result->td_types = teradata_types;

	// Set the bind data
	bind_data = std::move(result);

	// Return the table function
	auto function = TeradataQueryFunction::GetFunction();
	return function;
}

TableStorageInfo TeradataTableEntry::GetStorageInfo(ClientContext &context) {
	// TODO;
	TableStorageInfo info;
	return info;
}

} // namespace duckdb


================================================
FILE: src/teradata_table_entry.hpp
================================================
#pragma once

#include "teradata_type.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class TeradataTableInfo final : public CreateTableInfo {
public:
	// If we are populating a table entry from an existing teradata table, we also pass on the teradata types
	vector<TeradataType> teradata_types;
};

class TeradataTableEntry final : public TableCatalogEntry {
public:
	// Create a Teradata table entry from within DuckDB
	TeradataTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

	// Create a Teradata table entry by providing data gathered from Teradata
	TeradataTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, TeradataTableInfo &info);

	// Get the statistics of a specific column
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	// Get the scan function of the table
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	//! Returns the storage info of this table
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

public:
	// The raw teradata typess
	vector<TeradataType> teradata_types;
};

} // namespace duckdb


================================================
FILE: src/teradata_table_set.cpp
================================================
#include "teradata_table_set.hpp"
#include "teradata_table_entry.hpp"
#include "teradata_schema_entry.hpp"
#include "teradata_catalog.hpp"
#include "teradata_transaction.hpp"

#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

static string GetDuckDBToTeradataTypeString(const LogicalType &type) {
	// Convert the DuckDB type to a Teradata type, then get the string representation of that type
	return TeradataType::FromDuckDB(type).ToString();
}

static string GetTeradataColumnsDefSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints, bool use_primary_index) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key
	// columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			vector<string> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after
				// all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << KeywordHelper::WriteQuoted(column.Name(), '"') << " ";
		ss << GetDuckDBToTeradataTypeString(column.Type());
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY NOT NULL";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.Generated()) {
			throw NotImplementedException("Generated columns are not supported in Teradata");
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT (" << column.DefaultValue().ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";

	if (!use_primary_index) {
		// Also dont make a primary index
		ss << " NO PRIMARY INDEX";
	}

	return ss.str();
}

static string GetTeradataCreateTableSQL(const CreateTableInfo &info, bool use_primary_index) {
	std::stringstream ss;

	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!info.schema.empty()) {
		ss << KeywordHelper::WriteQuoted(info.schema, '"');
		ss << ".";
	}
	ss << KeywordHelper::WriteQuoted(info.table, '"');
	ss << GetTeradataColumnsDefSQL(info.columns, info.constraints, use_primary_index);
	ss << ";";

	return ss.str();
}

optional_ptr<CatalogEntry> TeradataTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &transaction = TeradataTransaction::Get(context, catalog);


	Value use_primary_index_value;
	bool use_primary_index = true;
	if (context.TryGetCurrentSetting("teradata_use_primary_index", use_primary_index_value)) {
		use_primary_index = use_primary_index_value.GetValue<bool>();
	}

	auto &base = info.Base();
	const auto create_sql = GetTeradataCreateTableSQL(base, use_primary_index);

	// Execute the sql statement
	transaction.GetConnection().Execute(create_sql);

	auto tbl_entry = make_uniq<TeradataTableEntry>(catalog, schema, base);
	return CreateEntry(std::move(tbl_entry));
}

void TeradataTableSet::LoadEntries(ClientContext &context) {
	const auto &td_catalog = catalog.Cast<TeradataCatalog>();

	auto &conn = td_catalog.GetConnection();

	/*
	    "The DBC.ColumnsV[X] views provide complete information for table columns but provide only limited information
	    for view columns. For view columns, DBC.ColumnsV[X] provides a NULL value for ColumnType, DecimalTotalDigits,
	    DecimalFractionalDigits, CharType, ColumnLength, and other attributes related to data type."

	    Therefore, we can filter on "ColumnType IS NOT NULL" to get only tables, without having to join with the
	    DBC.TablesV view.
	 */

	// TODO: Sanitize the schema name
	const auto query = StringUtil::Format("SELECT T.TableName, C.ColumnName, C.ColumnType, C.ColumnLength "
	                                      "FROM dbc.TablesV AS T JOIN dbc.ColumnsV AS C "
	                                      "ON T.TableName = C.TableName AND T.DatabaseName = C.DatabaseName "
	                                      "WHERE T.DatabaseName = '%s' AND (T.TableKind = 'T' OR T.TableKind = 'O') "
	                                      "ORDER BY T.TableName, C.ColumnId",
	                                      schema.name);

	const auto result = conn.Query(query, false);

	TeradataTableInfo info;
	info.schema = schema.name;

	bool skip_table = false;

	// Iterate over the chunks in the result
	for (auto &chunk : result->Chunks()) {
		chunk.Flatten();
		const auto count = chunk.size();
		auto &tbl_name_vec = chunk.data[0];
		auto &col_name_vec = chunk.data[1];
		auto &col_type_vec = chunk.data[2];
		auto &col_size_vec = chunk.data[3];

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto tbl_name = FlatVector::GetData<string_t>(tbl_name_vec)[row_idx];
			const auto col_name = FlatVector::GetData<string_t>(col_name_vec)[row_idx];
			const auto col_type = FlatVector::GetData<string_t>(col_type_vec)[row_idx];
			const auto col_size = FlatVector::GetData<int32_t>(col_size_vec)[row_idx];

			if (tbl_name != info.table) {
				// We have a new table
				if (!info.table.empty() && !skip_table) {
					// Finish the previous table
					entries[info.table] = make_uniq<TeradataTableEntry>(catalog, schema, info);
				}
				skip_table = false;

				// Reset the info
				info = TeradataTableInfo();
				info.table = tbl_name.GetString();
				info.schema = schema.name;
			}

			// Add the columns
			try {
				TeradataType td_type = TeradataType::FromShortCode(col_type.GetData());
				if (td_type.HasLengthModifier()) {
					td_type.SetLength(col_size);
				}

				const auto duck_type = td_type.ToDuckDB();
				info.columns.AddColumn(ColumnDefinition(col_name.GetString(), duck_type));
				info.teradata_types.push_back(td_type);
			} catch (...) {
				// Ignore the column type (and this table) for now, just make this work
				skip_table = true;
			}
		}
	}

	if (!info.table.empty() && !skip_table) {
		// Finish the last table
		entries[info.table] = make_uniq<TeradataTableEntry>(catalog, schema, info);
	}
}

} // namespace duckdb


================================================
FILE: src/teradata_table_set.hpp
================================================
#pragma once

#include "teradata_catalog_set.hpp"

namespace duckdb {

struct BoundCreateTableInfo;

class Catalog;

class TeradataTableSet final : public TeradataInSchemaSet {
public:
	explicit TeradataTableSet(TeradataSchemaEntry &schema) : TeradataInSchemaSet(schema) {
	}

	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb


================================================
FILE: src/teradata_transaction.cpp
================================================
#include "teradata_transaction.hpp"
#include "teradata_catalog.hpp"

namespace duckdb {

TeradataTransaction::TeradataTransaction(TeradataCatalog &catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), con(catalog.GetConnection()) {

	// TODO:
	// transaction_state = TeradataTransactionState::TRANSACTION_NOT_YET_STARTED;
}

TeradataTransaction::~TeradataTransaction() {
}

TeradataTransaction &TeradataTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<TeradataTransaction>();
}

void TeradataTransaction::Start() {
	con.Execute("BEGIN TRANSACTION;");
}

void TeradataTransaction::Commit() {
	con.Execute("END TRANSACTION;");
}

void TeradataTransaction::Rollback() {
	con.Execute("ABORT;");
}

} // namespace duckdb


================================================
FILE: src/teradata_transaction.hpp
================================================
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class TeradataCatalog;
class TeradataConnection;

class TeradataTransaction final : public Transaction {
public:
	TeradataTransaction(TeradataCatalog &catalog, TransactionManager &manager, ClientContext &context);
	~TeradataTransaction() override;

	TeradataConnection &GetConnection() const {
		return con;
	}

	void Start();
	void Commit();
	void Rollback();

	static TeradataTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	// TODO: This shouldnt be passed like this
	TeradataConnection &con;
};

} // namespace duckdb



================================================
FILE: src/teradata_transaction_manager.cpp
================================================
#include "teradata_transcation_manager.hpp"

namespace duckdb {

TeradataTransactionManager::TeradataTransactionManager(AttachedDatabase &db, TeradataCatalog &catalog)
    : TransactionManager(db), teradata_catalog(catalog) {
}

// TODO: Everything below this point is a stub
Transaction &TeradataTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<TeradataTransaction>(teradata_catalog, *this, context);

	transaction->Start();

	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData TeradataTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &td_transaction = transaction.Cast<TeradataTransaction>();

	td_transaction.Commit();

	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);

	return ErrorData();
}

void TeradataTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &td_transaction = transaction.Cast<TeradataTransaction>();

	td_transaction.Rollback();

	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void TeradataTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// Not applicable?
}

} // namespace duckdb


================================================
FILE: src/teradata_transcation_manager.hpp
================================================
#pragma once

#include "teradata_catalog.hpp"
#include "teradata_transaction.hpp"

#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class TeradataTransactionManager final : public TransactionManager {
public:
	explicit TeradataTransactionManager(AttachedDatabase &db, TeradataCatalog &catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force) override;

private:
	TeradataCatalog &teradata_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<TeradataTransaction>> transactions;
};

} // namespace duckdb


================================================
FILE: src/teradata_type.cpp
================================================
#include "teradata_type.hpp"

namespace duckdb {

string TeradataType::ToString() const {
	switch (id) {
	case TeradataTypeId::INVALID:
		return "INVALID";
	case TeradataTypeId::ARRAY:
		return "ARRAY";
	case TeradataTypeId::BYTE:
		return "BYTE(" + to_string(width) + ")";
	case TeradataTypeId::VARBYTE:
		return "VARBYTE(" + to_string(width) + ")";
	case TeradataTypeId::BLOB:
		return "BLOB(" + to_string(width) + ")";
	case TeradataTypeId::CHAR:
		return "CHAR(" + to_string(width) + ")";
	case TeradataTypeId::VARCHAR:
		return "VARCHAR(" + to_string(width) + ")";
	case TeradataTypeId::CLOB:
		return "CLOB(" + to_string(width) + ")";
	case TeradataTypeId::DATE:
		return "DATE";
	case TeradataTypeId::TIME:
		return "TIME(" + to_string(width) + ")";
	case TeradataTypeId::TIMESTAMP:
		return "TIMESTAMP(" + to_string(width) + ")";
	case TeradataTypeId::TIME_TZ:
		return "TIME(" + to_string(width) + ") WITH TIME ZONE";
	case TeradataTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP(" + to_string(width) + ") WITH TIME ZONE";
	case TeradataTypeId::ST_GEOMETRY:
		return "ST_GEOMETRY";
	case TeradataTypeId::MBR:
		return "MBR";
	case TeradataTypeId::INTERVAL_YEAR:
		return "INTERVAL_YEAR";
	case TeradataTypeId::INTERVAL_YEAR_TO_MONTH:
		return "INTERVAL_YEAR_TO_MONTH";
	case TeradataTypeId::INTERVAL_MONTH:
		return "INTERVAL_MONTH";
	case TeradataTypeId::INTERVAL_DAY:
		return "INTERVAL_DAY";
	case TeradataTypeId::INTERVAL_DAY_TO_HOUR:
		return "INTERVAL_DAY_TO_HOUR";
	case TeradataTypeId::INTERVAL_DAY_TO_MINUTE:
		return "INTERVAL_DAY_TO_MINUTE";
	case TeradataTypeId::INTERVAL_DAY_TO_SECOND:
		return "INTERVAL_DAY_TO_SECOND";
	case TeradataTypeId::INTERVAL_HOUR_TO_MINUTE:
		return "INTERVAL_HOUR_TO_MINUTE";
	case TeradataTypeId::INTERVAL_HOUR_TO_SECOND:
		return "INTERVAL_HOUR_TO_SECOND";
	case TeradataTypeId::INTERVAL_MINUTE:
		return "INTERVAL_MINUTE";
	case TeradataTypeId::INTERVAL_MINUTE_TO_SECOND:
		return "INTERVAL_MINUTE_TO_SECOND";
	case TeradataTypeId::INTERVAL_SECOND:
		return "INTERVAL_SECOND";
	case TeradataTypeId::JSON:
		return "JSON";
	case TeradataTypeId::BYTEINT:
		return "BYTEINT";
	case TeradataTypeId::SMALLINT:
		return "SMALLINT";
	case TeradataTypeId::INTEGER:
		return "INTEGER";
	case TeradataTypeId::BIGINT:
		return "BIGINT";
	case TeradataTypeId::DECIMAL:
		return "DECIMAL(" + to_string(width) + ", " + to_string(scale) + ")";
	case TeradataTypeId::FLOAT:
		return "FLOAT";
	case TeradataTypeId::NUMBER:
		return "NUMBER";
	case TeradataTypeId::ANYTYPE:
		return "ANYTYPE";
	case TeradataTypeId::VARIANT:
		return "VARIANT";
	case TeradataTypeId::PERIOD_DATE:
		return "PERIOD_DATE";
	case TeradataTypeId::PERIOD_TIME:
		return "PERIOD_TIME";
	case TeradataTypeId::PERIOD_TIME_TZ:
		return "PERIOD_TIME_TZ";
	case TeradataTypeId::PERIOD_TIMESTAMP:
		return "PERIOD_TIMESTAMP";
	case TeradataTypeId::PERIOD_TIMESTAMP_TZ:
		return "PERIOD_TIMESTAMP_TZ";
	case TeradataTypeId::UDT_DISTINCT:
		return "UDT_DISTINCT";
	case TeradataTypeId::UDT_STRUCTURED:
		return "UDT_STRUCTURED";
	case TeradataTypeId::XML:
		return "XML";
	default:
		throw NotImplementedException("Unimplemented Teradata type");
	}
}

LogicalType TeradataType::ToDuckDB() const {
	switch (id) {
	case TeradataTypeId::INVALID:
		return LogicalType::INVALID;
	case TeradataTypeId::BYTE:
	case TeradataTypeId::VARBYTE:
	case TeradataTypeId::BLOB:
		return LogicalType::BLOB;
	case TeradataTypeId::CHAR:
	case TeradataTypeId::VARCHAR:
	case TeradataTypeId::CLOB:
		return LogicalType::VARCHAR;
	case TeradataTypeId::DATE:
		return LogicalType::DATE;
	case TeradataTypeId::TIME:
		return LogicalType::TIME;
	case TeradataTypeId::TIMESTAMP:
		// Round to closest duckdb precision
		switch (width) {
		// Seconds
		case 0:
			return LogicalType::TIMESTAMP_S;
		// Milliseconds
		case 1:
			return LogicalType::TIMESTAMP_MS;
		case 2:
			return LogicalType::TIMESTAMP_MS;
		case 3:
			return LogicalType::TIMESTAMP_MS;
		// Microseconds
		case 4:
			return LogicalType::TIMESTAMP;
		case 5:
			return LogicalType::TIMESTAMP;
		case 6:
			return LogicalType::TIMESTAMP;
		default:
			throw NotImplementedException("Unimplemented Teradata type");
		}
	case TeradataTypeId::TIME_TZ:
		return LogicalType::TIME_TZ;
	case TeradataTypeId::TIMESTAMP_TZ:
		return LogicalType::TIMESTAMP_TZ;
	case TeradataTypeId::BYTEINT:
		return LogicalType::TINYINT;
	case TeradataTypeId::SMALLINT:
		return LogicalType::SMALLINT;
	case TeradataTypeId::INTEGER:
		return LogicalType::INTEGER;
	case TeradataTypeId::BIGINT:
		return LogicalType::BIGINT;
	case TeradataTypeId::DECIMAL:
		// TODO: Not correct.
		return LogicalType::DECIMAL(width, scale);
	case TeradataTypeId::FLOAT:
		// Teradata only supports 8-byte floats (REAL and DOUBLE PRECISION and FLOAT are all the same)
		return LogicalType::DOUBLE;
	case TeradataTypeId::JSON:
		return LogicalType::JSON();
	case TeradataTypeId::INTERVAL_HOUR_TO_SECOND:
		// Convert to interval
		return LogicalType::INTERVAL;
	default:
		throw NotImplementedException("Unimplemented Teradata type");
	}
}

unordered_map<string, TeradataTypeId> TeradataType::code_map = {
    {"++", TeradataTypeId::ANYTYPE}, // TD_ANYTYPE
    {"A1", TeradataTypeId::INVALID}, // UDT
    {"AT", TeradataTypeId::TIME},
    {"BF", TeradataTypeId::BYTE},
    {"BO", TeradataTypeId::BLOB},
    {"BV", TeradataTypeId::VARBYTE},
    {"CF", TeradataTypeId::CHAR},
    {"CO", TeradataTypeId::CLOB},
    {"CV", TeradataTypeId::VARCHAR},
    {"D ", TeradataTypeId::DECIMAL},
    {"DA", TeradataTypeId::DATE}, // DATE
    {"DH", TeradataTypeId::INTERVAL_DAY_TO_HOUR},
    {"DM", TeradataTypeId::INTERVAL_DAY_TO_MINUTE},
    {"DS", TeradataTypeId::INTERVAL_DAY_TO_SECOND},
    {"DY", TeradataTypeId::INTERVAL_DAY},
    {"F ", TeradataTypeId::FLOAT},
    {"HM", TeradataTypeId::INTERVAL_HOUR_TO_MINUTE},
    {"HR", TeradataTypeId::INTERVAL_HOUR},
    {"HS", TeradataTypeId::INTERVAL_HOUR_TO_SECOND},
    {"I1", TeradataTypeId::BYTEINT},
    {"I2", TeradataTypeId::SMALLINT},
    {"I8", TeradataTypeId::BIGINT},
    {"I ", TeradataTypeId::INTEGER},
    {"MI", TeradataTypeId::INTERVAL_MINUTE},
    {"MO", TeradataTypeId::INTERVAL_MONTH},
    {"MS", TeradataTypeId::INTERVAL_MINUTE_TO_SECOND},
    {"N ", TeradataTypeId::NUMBER},
    {"PD", TeradataTypeId::PERIOD_DATE},
    {"PM", TeradataTypeId::PERIOD_TIMESTAMP_TZ},
    {"PS", TeradataTypeId::PERIOD_TIMESTAMP},
    {"PT", TeradataTypeId::PERIOD_TIME},
    {"PZ", TeradataTypeId::PERIOD_TIME_TZ},
    {"SC", TeradataTypeId::INTERVAL_SECOND},
    {"SZ", TeradataTypeId::TIMESTAMP_TZ},
    {"TS", TeradataTypeId::TIMESTAMP},
    {"TZ", TeradataTypeId::TIME_TZ},
    {"UT", TeradataTypeId::INVALID}, // "UDT"
    {"YM", TeradataTypeId::INTERVAL_YEAR_TO_MONTH},
    {"YR", TeradataTypeId::INTERVAL_YEAR},
    {"AN", TeradataTypeId::INVALID}, // "UDT",
    {"XM", TeradataTypeId::XML},
    {"JN", TeradataTypeId::JSON},
    {"DT", TeradataTypeId::INVALID}, // "DATASET",
    {"??", TeradataTypeId::ST_GEOMETRY},
};

bool TeradataType::HasLengthModifier() const {
	switch (id) {
	case TeradataTypeId::BYTE:
	case TeradataTypeId::VARBYTE:
	case TeradataTypeId::BLOB:
	case TeradataTypeId::CHAR:
	case TeradataTypeId::VARCHAR:
	case TeradataTypeId::CLOB:
		return true;
	default:
		return false;
	}
}

TeradataType TeradataType::FromDuckDB(const LogicalType &type) {
	if (type.IsJSONType()) {
		return TeradataTypeId::JSON;
	}

	switch (type.id()) {
	// Boolean (does not exist in Teradata, so map to byteint)
	case LogicalTypeId::BOOLEAN:
		return TeradataTypeId::BYTEINT;

	// Integer types
	case LogicalTypeId::TINYINT:
		return TeradataTypeId::BYTEINT;
	case LogicalTypeId::SMALLINT:
		return TeradataTypeId::SMALLINT;
	case LogicalTypeId::INTEGER:
		return TeradataTypeId::INTEGER;
	case LogicalTypeId::BIGINT:
		return TeradataTypeId::BIGINT;

	// Only DOUBLEs are supported
	case LogicalTypeId::DOUBLE: {
		return TeradataTypeId::FLOAT;
	}
	// Decimal type
	case LogicalTypeId::DECIMAL: {
		TeradataType decimal_type = TeradataTypeId::DECIMAL;
		decimal_type.SetWidth(DecimalType::GetWidth(type));
		decimal_type.SetScale(DecimalType::GetScale(type));
		return decimal_type;
	}
	// Time types
	case LogicalTypeId::TIMESTAMP_SEC: {
		TeradataType ts_type = TeradataTypeId::TIMESTAMP;
		ts_type.SetWidth(0);
		return ts_type;
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		TeradataType ts_type = TeradataTypeId::TIMESTAMP;
		ts_type.SetWidth(3);
		return ts_type;
	}
	case LogicalTypeId::TIMESTAMP: {
		TeradataType ts_type = TeradataTypeId::TIMESTAMP;
		// Microseconds
		ts_type.SetWidth(6);
		return ts_type;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		TeradataType ts_type = TeradataTypeId::TIMESTAMP_TZ;
		ts_type.SetWidth(6);
		return ts_type;
	}
	case LogicalTypeId::DATE:
		return TeradataTypeId::DATE;

	case LogicalTypeId::TIME: {
		TeradataType t_type = TeradataTypeId::TIME;
		t_type.SetWidth(6); // Duckdb store microsecond precision
		return t_type;
	}
	case LogicalTypeId::TIME_TZ: {
		TeradataType t_type = TeradataTypeId::TIME_TZ;
		t_type.SetWidth(6); // Duckdb store microsecond precision
		return t_type;
	}
	// Varchar
	case LogicalTypeId::VARCHAR: {
		// Since DuckDB types are variable size, set the length to the maximum
		TeradataType char_type = TeradataTypeId::VARCHAR;
		char_type.SetLength(TeradataType::MAX_TYPE_LENGTH);
		return char_type;
	}

	// Blob
	case LogicalTypeId::BLOB: {
		// Since DuckDB types are variable size, set the length to the maximum
		TeradataType blob_type = TeradataTypeId::VARBYTE;
		blob_type.SetLength(TeradataType::MAX_TYPE_LENGTH);
		return blob_type;
	}
	default:
		throw NotImplementedException("Cannot convert DuckDB type '%s' to Teradata type", type.ToString());
	}
}

} // namespace duckdb


================================================
FILE: src/teradata_type.hpp
================================================
#pragma once
#include "teradata_common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class TeradataTypeId : uint8_t {
	INVALID = 0,

	// An ARRAY data type is used for storing and accessing multidimensional data.
	// The ARRAY data type can store many values of the same specific data type in a sequential or matrix-like format.
	ARRAY,

	// Byte data types store raw data as logical bit streams. These data types are stored in the client system format
	// and are not translated by the database. The data is transmitted directly from the memory of the client system.
	BYTE,
	VARBYTE,
	BLOB,

	// Character data types represent characters that belong to a given character set.
	// These data types represent character data.
	CHAR,
	VARCHAR,
	CLOB,

	// DateTime data types represent date, time, and timestamp values
	DATE,
	TIME,
	TIMESTAMP,
	TIME_TZ,
	TIMESTAMP_TZ,

	// Geospatial data types represent geographic information and provides a way for applications that manage, analyze,
	// and display geographic information to interface with the database.
	ST_GEOMETRY,
	MBR,

	// Interval data types represent a span of time. For example, an interval value can represent a time span that
	// includes a number of years, months, days, hours, minutes, or seconds.
	INTERVAL_YEAR,
	INTERVAL_YEAR_TO_MONTH,
	INTERVAL_MONTH,
	INTERVAL_DAY,
	INTERVAL_DAY_TO_HOUR,
	INTERVAL_DAY_TO_MINUTE,
	INTERVAL_DAY_TO_SECOND,
	INTERVAL_HOUR,
	INTERVAL_HOUR_TO_MINUTE,
	INTERVAL_HOUR_TO_SECOND,
	INTERVAL_MINUTE,
	INTERVAL_MINUTE_TO_SECOND,
	INTERVAL_SECOND,

	// The JSON data type represents data that is in JSON (JavaScript Object Notation) format.
	JSON,

	// Numeric data types represent a numeric value that is an exact numeric number (integer or decimal) or an
	// approximate numeric number (floating point).
	BYTEINT,
	SMALLINT,
	INTEGER,
	BIGINT,
	DECIMAL, // Alias: NUMERIC,
	FLOAT,   // Alias: DOUBLE, REAL
	NUMBER,  // Distinct from DECIMAL

	// Parameter data types are data types that can be used only with input or result parameters in a function, method,
	// stored procedure, or external stored procedure.
	ANYTYPE,
	VARIANT,

	// A Period data type represents a time period, where a period is a set of contiguous time granules that extends
	// from a beginning bound up to but not including an ending bound.
	PERIOD_DATE,
	PERIOD_TIME,
	PERIOD_TIME_TZ,
	PERIOD_TIMESTAMP,
	PERIOD_TIMESTAMP_TZ,

	// UDT (User-Defined Type) data types are custom data types that you define to model the structure and behavior of
	// the data used by your applications.
	UDT_DISTINCT,
	UDT_STRUCTURED,

	// The XML data type represents XML content. The data is stored in a compact binary form that preserves
	// the information set of the XML document, including the hierarchy information and type information derived from
	// XML validation.
	XML,

	// Extra, only used when binding
	LONGVARBYTE,
	LONGVARGRAPHIC,
};

class TeradataType {
public:
	static constexpr auto MAX_TYPE_LENGTH = 64000;

	// NOLINTNEXTLINE: Allow implicit conversion from `TeradataTypeId`
	TeradataType(TeradataTypeId id) : id(id) {
	}

	TeradataTypeId GetId() const {
		return id;
	}
	int64_t GetLength() const {
		return width;
	}
	int64_t GetWidth() const {
		return width;
	}
	int64_t GetScale() const {
		return scale;
	}

	void SetLength(const int64_t length_p) {
		width = length_p;
	}
	void SetWidth(const int64_t width_p) {
		width = width_p;
	}
	void SetScale(const int64_t scale_p) {
		scale = scale_p;
	}
	bool HasLengthModifier() const;

	string ToString() const;
	LogicalType ToDuckDB() const;

	static TeradataTypeId FromShortCode(const string &code) {
		const auto entry = code_map.find(code);
		if (entry == code_map.end()) {
			return TeradataTypeId::INVALID;
		}
		return entry->second;
	}

	static TeradataType FromDuckDB(const LogicalType &type);

	bool IsDecimal() const {
		return id == TeradataTypeId::DECIMAL;
	}

	bool operator==(const TeradataType &rhs) const {
		return id == rhs.id && width == rhs.width && scale == rhs.scale;
	}
	bool operator!=(const TeradataType &rhs) const {
		return !(*this == rhs);
	}

private:
	TeradataTypeId id = TeradataTypeId::INVALID;
	int64_t width = 0;
	int64_t scale = 0;

	// From eg. "BF" to TeradataTypeId::BYTE
	static unordered_map<string, TeradataTypeId> code_map;
};

} // namespace duckdb


================================================
FILE: src/teradata/coperr.h
================================================
#ifndef COPERR_H
#define COPERR_H
#define COPERR_H_REV "20.00.00.09"
/*******************************************************************************
 *
 *
 *  WARNING: Because coperr.h is shared and is used as a header
 *           file for mainframe SAS/C compiles under VM/CMS, the logical
 *           record length (i.e., line length) must not exceed 80
 *           characters.
 *-----------------------------------------------------------------------------*
 *                   <-- ( 8 0    c h a r a c t e r s) -->
 *******************************************************************************
 *  TITLE:       COPERR.H .. Error code definitions for "C"          20.00.00.09
 *
 *  Purpose      To contain the error codes for the workstation
 *               code written in "C".
 *
 *  Copyright 1983-2025 by Teradata Corporation.  All rights reserved.
 *
 *  Description  This include file contains the error codes for
 *               the workstation version of BTEQ, CLIV2, MTDP,
 *               and MOSI. The text associated with the codes is
 *               is contained in a file which is referenced by
 *               the OsGetErr routine using the input code value
 *               to index into the file.
 *
 *  History
 *   F.1   86Dec10  DAA  DCR3075  Coded
 *   F.2   87Jan16  DAA  DR8678
 *   F.3   87Jan20  DAA  DR8678   Fix spacing of comments
 *   F.4   87Feb20  KKL  DR9315   Add EM_NOHOST; Reassign
 *                                MOSI errno numbers
 *   F.5   87Feb26  KKL  DR9379   Added ER_CONNID, ER_MAXSESS
 *   F.6.  87Mar17  DAA  DR9596   Fixed mtdpcount value
 *   F.7   87Apr29  DAA  DR9899   Changed DBRPUTZ
 *   F.8   87Jun05  DAA  DCR3430  Vax support
 *   F.9   87Jul01  DAA  DCR3430  Vax support
 *   F.10  87Jul21  DAA  DCR3780  Add options parcel
 *   F.11  87Aug11  DAA  DR10661  Wake process on break
 *   F.12  87Aug25  DAA  DR10809  Fix multisession Reconn
 *   F.13  87Nov15  SCT  DR10471  Narrowed to <= 65 cols.
 *   F.14  87Nov15  DAA  DR11311  Add errmsg version No.
 *   F.15  87Nov15  DAA  DR 10471 Fix it
 *   F.16  88Jan28  DRP  DR11926  (MR# fe87-18102a0)
 *   F.17  88Apr04  DAA  DR11970  Fix floc/fvar ncompatibility
 *   F.18  88Apr04  DAA  DR11970  Fix incorrect release
 *   F.19  88Apr22  KKL  DR12497  Add ER_CONNREFUSED
 *   F.20  88Apr22  KKL  DR12497  Reship DR12497
 *   F.21  88Jul01  WBJ  DR10377  Add BADRSUP
 *   G0_00 88Jul25  SN   Created  For Release G.0
 *   G0_01 88Oct07  WBJ  DR12820  Enhance error reporting
 *   G0_02 88Oct13  WBJ  DCR4029 & 4278 new option errors
 *   G0_03 89Dec15  KXO  DCR5239  Add Error 327, 328, 329
 *         90Feb02  KXO  DR18467  Add Error 330
 *   G0_04 90Sep14  KXO           Revised for UNIXTOOL KIT
 *   G0_05 91Jan05  DJL  DCR5366  Tandem port
 *   G0_06 90Aug16  GLL  DR19967  Add MOSI 143,CLI 331
 *   H0_00 93May20  SNT  DCR6711  Created for Release H.0
 *   H0_01 93Aug19  Bg1  DCR5571  Add for Cli User Exit.
 *   H0_02 94Mar11  JAE  DR28060  Added REQOVFLOW (CLI 333).
 *   H0_03 94Mar25  JAE  DCR6172  Added 2PC error codes,
 *                                changed USREXT to
 *                                349, changed REQOVFLOW to
 *                                350.
 *   H0_04 94Apr04  JAE  DCR6172  Added EM_NOSESSNO for 2PC.
 *   H0_05 94Mar25  km5  DCR6842&6852 Added SQLUSREXT (CLI 351).
 *   HN_00 94Aug19  JAE  Added new error codes for NDM.
 *   HN_01 95FEB09  ALB  DR32294 Merged 5.0 and AFCAC code.
 *   H3_00 95Aug23  JAE  DR32294 Completed code merge by removing #ifdef NDM.
 *   H3_01 95Sep13  TH4  DR34047  Added the #ifndef XXXX #define XXXX.
 *   04.00.00.01 TH4 96Jul12 DR36785 Version number change.
 *   04.01.00.02 JAE 96Nov11 DR38038 Changed 312 error name to INVALIDFN.
 *   04.01.00.03 TH4 96Dec30 DR38016 Don't kill application when PCLI deadlock.
 *   04.01.00.04 JAE 97Jan13 DR38017 Added new PCLI error messages (359-368)
 *   04.01.00.05 TH4 97Feb26 DR38910 Added new PCLI error message 369.
 *   04.02.00.00 JTH July97  dr40000 Code merge
 *   04.03.00.00 TH4 98Feb05 DR40973 Porting Parallel CLI to WinCLI.
 *   04.04.02.00 TH4 99Feb02 DR37947 new error code for cancel logon.
 *   04.05.00.00 CSG 2000Mar03 DR48735 TDSP Support
 *   04.05.00.01 CSG       2000Mar22 DR50139 TDSP Report Error 374 (NOTDSP)
 *   04.06.00.00 ASG       2001Jan17 DR52052 Support SSO
 *   04.06.01.00 CSG       2001Sep05 DR57420 Added error 376 (NOBUFFER).
 *   04.06.01.01 cc151010  2001Aug30 DR52050 Support multi-threaded CLI.
 *   04.06.02.00 cc151010  2002Jan11 DR59375 Add NOACTIVEREQUEST(385).
 *   04.06.02.01 CSG       2002Jan28 DR57144 CLI should not allow options
 *                                           var_len_fetch=Y and parcel_mode=N
 *   04.06.02.02  CSG      2002Mar26 DR51742 support req_proc_opt=B in
 *                                           parcel mode
 *   04.06.07.00  ASG      2002Apr02 DR57583 LOB Support
 *   04.07.00.01  CSG      2002Jun17 DR61637 Support enlarged parcel usage.
 *   04.07.00.02  CSG      2002Aug22 DR52059 L10N support.
 *   04.07.01.00  CSG      2003Mar03 DR61527 Error codes for DR60695,
 *                                           DR67168, DR 61527 and DR 66712.
 *   04.07.01.01 mg180007  2003Mar12 DR67744 Remove HPUX compilation warnings.
 *   04.07.01.02 CSG       2003Mar24 DR67771 Replace UCS2 with UTF16.
 *   04.07.01.03 mg180007  2003Apr24 DR68084 renamed SSO errors to GSS
 *   04.08.00.00 mg180007  2003Jul22 DR68511 clean up defines, prototypes
 *                                   DR68139 self-sufficient headers
 *   04.08.00.01 mg180007  2004Jan07 DR68829 changed return type for CatOpen()
 *                                           changed EM_DLLLOADFAIL comment
 *   04.08.00.02 ASG       2003Dec05 DR68838 Add CLI501 (array ops)
 *   04.08.00.03 ASG       2004Jan28 DR68835 APH / 1 MB Responses
 *   04.08.00.04 ASG       2004Feb10 DR58352 ESS
 *   04.08.00.05 ASG       2004Feb19 DR85431 universal headers
 *   04.08.00.06 mg180007  2004May18 DR68842 security (507, 508)
 *   04.08.00.07 mg180007  2004May29 DR68842 security (509)
 *   04.08.00.08 ASG       2004Sep17 DR89408 Fix 64-bit prob w/ array ops feat
 *   04.08.00.09 mg180007  2004Nov11 DR88745 ICU loading (510)
 *   04.08.01.00 mg180007  2005Feb14 DR92232 merge and update copyright year
 *   04.08.01.01 mg180007  2005Apr21 DR93472 added header revision info
 *   04.08.01.02 ASG       2005Jun23 DR94622 dual-component feature support
 *   04.08.01.03 mg180007  2005Aug09 DR93472 fixed header revision info
 *   04.08.02.00 bh185000  2005Dec15 DR98024 Support for MaxDecimalPrecision
 *                                   DR98025 Support for AGKR_option
 *                                   DR98026 Support for StatementInfo
 *   04.08.02.01 ASG       2006Feb14 DR102061 reconfig for 80-byte lines
 *   04.09.00.00 bh185000  2006May30 DR102528 Support for DynamicResultSets
 *   04.09.00.01 mg180007  2006Jul11 DR101287 Support for Default Connect
 *   12.00.00.02 fs185009  2006Nov29 DR108852 version change to 12.0.x
 *   12.00.00.03 bh185000  2006Nov30 DR108909 Added an error code for workload
 *   13.00.00.00 mg180007  2008Jan09 DR117140 copyright update
 *   13.00.00.01 fs185009  2008Jan14 DR107850 added 3 more icu load error codes
 *   13.00.00.02 mg180007  2008Feb27 DR112838 TDUSR loading (530)
 *   13.01.00.00 bh185000  2008Nov17 DR116354 Support Extended Object Name
 *   13.10.00.00 kl185018  2009Jun23 DR133468 TTU13.1 Version Renumbering
 *                                            to TTU13.10
 *   14.00.00.00 bh185000  2010Oct06 DR121202 Support Statement Independence
 *   14.00.00.01 mg180007  2010Dec10 DR140617 Added BADMSGLEN
 *   14.00.00.02 bh185000  2011Feb24 DR145224 Support Array Data Type
 *   14.10.00.00 bh185000  2012Jan10 CLAC-28875 Support XML Data Type
 *   14.10.00.01 mg180007  2012Jan31 CLAC-663   Added TDWALLET errors 543,544
 *   14.10.00.02 bh185000  2012May16 CLAC-29283 Support Extended Multiload
 *   14.10.00.03 mg180007  2012Jun13 CLAC-29119 Support Automatic Redrive
 *   14.10.00.04 hs186016  2013Feb21 CLAC-30478 Support TASM FastFail
 *   15.00.00.00 hs186016  2013Nov14 CLAC-30554 Support SLOB
 *   15.10.00.00 mg180007  2014Apr28 CLAC-32368 version and copyright update
 *   16.00.00.00 bh185000  2015Jun24 CLAC-29115 Support 1MB Response Rows
 *   17.00.00.00 hs186016  2018Nov13 CLIWS-7163 version and copyright update
 *   17.00.00.01 vt186023  2019May22 CLIWS-7192 Support TLS v1.2
 *   17.10.00.00 hs186016  2020Jul27 CLIWS-7606 Update version
 *   17.10.00.01 hs186016  2020Aug10 CLIWS-7327 Verify server certificate
 *   17.10.00.02 hs186016  2020Sep03 CLIWS-7647 Handle gateway error 8059
 *   17.10.00.03 rp255060  2021Jan25 CLIWS-7635 Connection string messages
 *   17.10.00.04 rp255060  2021Apr09 CLIWS-7901 Return error on invalid sslmode
 *   17.10.00.05 hs186016  2021Jun03 CLIWS-7751 Support mainframe on z/OS
 *   17.20.00.00 hs186016  2022Jan01 CLIWS-8198 version and copyright update
 *   17.20.00.01 hs186016  2022Mar22 CLIWS-8295 Support RACF JWT for mainframe
 *   17.20.00.02 hs186016  2022Apr12 CLIWS-8295 Add more return codes from RACF JWT
 *   17.20.00.03 rp255060  2022Jul08 CLIWS-8410 Browser error code
 *   17.20.00.04 rp255060  2022Jul12 CLIWS-8233 OCSP support
 *   17.20.00.05 hs186016  2022Sep09 CLIWS-8512 Provide detailed info when TLS fails
 *   17.20.00.06 rp255060  2022Dec01 CLIWS-8614 CRL support
 *   17.20.00.07 hs186016  2022Dec22 CLIWS-8618 Update the IDT error messages for z/OS
 *   20.00.00.00 hs186016  2023May11 CLIWS-8696 version and copyright update
 *   20.00.00.01 rp255060  2023Aug01 CLIWS-8661 Proxy server support
 *   20.00.00.02 rp255060  2024Feb29 CLIWS-9071 Client credentials flow support
 *   20.00.00.03 rp255060  2024Mar01 CLIWS-9074 Improve error message when IdP certificate not trusted
 *   20.00.00.04 hs186016  2024Mar19 CLIWS-9070 Device Authorization Grant (device flow) support
 *   20.00.00.05 rp255060  2024Jun14 CLIWS-9221 Add error for Fips mode failure
 *   20.00.00.06 rp255060  2024Sep18 CLIWS-9380 Add bad logmech data error
 *   20.00.00.07 rp255060  2024Nov11 CLIWS-9492 New OIDC token cache
 *   20.00.00.08 hs186016  2025Jan09 CLIWS-9597 Fix with SSLMODE=verify-full, when OCSP related
 *                                              validation error occurs, CLI returns error without
 *                                              checking CRL (z/OS only)
 *   20.00.00.09 hs186016  2025Jan29 CLIWS-9637 Support TLS 1.3
 ******************************************************************************/


/***************************************************************/
/*  Instructions for setting up error codes and class specifier*/
/*  tables.                                                    */
/*                                                             */
/*  The error message generating routine has been written in a */
/*  very generic fashion to permit its future use in many other*/
/*  applications with any variety of error message text.  To   */
/*  provide enough information for meaningful processing of the*/
/*  error number codes into actual text messages, while still  */
/*  permitting the flexibility of modular development, error   */
/*  class specifier tables are used to indicate base error code*/
/*  values for specific errors. These tables include a base    */
/*  error number for the class, a count of the error codes as- */
/*  signed to the class, and an offset value to be subtracted  */
/*  from an error code to produce a true index into the error  */
/*  file pointer structure used by the OsGetErr routine.       */
/*                                                             */
/*  The only restriction imposed by this method is that the    */
/*  initialization of the error class specifier array be done  */
/*  from lowest base error number to highest, since OsGetErr   */
/*  scans the list of tables until the error code is greater   */
/*  than or equal to the base error code of a class table.     */
/*  Also, error codes within a class should be allocated in    */
/*  order, that is, without skipping any numbers.  If numbers  */
/*  are skipped, dummy error messages will have to be kept in  */
/*  the error message file, as place holders, since no search  */
/*  methods, or indirect referencing mechanism is used to      */
/*  fetch error text.  This method also requires that all      */
/*  error codes be referenced in the application only by the   */
/*  macro definition given in this file (this is good pro-     */
/*  gramming practice anyway).                                 */
/*                                                             */
/*  Two macro definitions in this file are used to provide     */
/*  flexibility  between applications and hosts.  ERRFNAME is  */
/*  used to define the name of the error message file for      */
/*  OsGetErr, thus providing a single point of control for     */
/*  various systems, and ERRTYPCNT specifies the number of     */
/*  error classes and, therefore, of error class tables.       */
/*                                                             */
/*                                                             */
/***************************************************************/

#include "coptypes.h" /* DR68139 */

#define  ERRVERSION   "Version is 20.00"
#define  ERRTYPCNT     3              /* # of error classes    */
                                      /* (MOSI, MTDP, & CLIV2) */

struct  ErrClassSpec
{
    Int32      base;          /* base error number for  class  */
    Int32      offset;        /* offset value for computing    */
                              /* error file index              */
    Int32      max;           /* maximum allowed error index   */
                              /* for this class                */
};


/***************************************************************/
/*                                                             */
/*                       MOSI - error codes                    */
/*                                                             */
/***************************************************************/

/*==================*/
/*   Error Class    */
/*==================*/
#define  EC_MEMORY       101  /* memory management             */
#define  EC_TIME         102  /* time management               */
#define  EC_ENVIRONMENT  103  /* system environment management */
#define  EC_DATA         104  /* data management               */
#define  EC_HANDLER      109  /* interrupt management          */
#define  EC_NETWORK      150  /* network management            */
#define  EC_MTDP         201  /* mtdp; should be in mtdpif.h   */

/*==================*/
/*   Error Reason   */
/*==================*/

#define  MOSIBASE        100  /* base error number for MOSI    */
#define  ER_SUCCESS        0  /* successful function completion*/
#define  ER_OPSYS        100  /* operating system specific     */
#define  ER_SUPPORT      101  /* function not supported        */
#define  ER_MEMSIZE      111  /* invalid memory size           */
#define  ER_INSUFF       112  /* insufficient memory           */
#define  ER_MEMBLOCK     113  /* invalid storage block         */
#define  ER_VALUE        114  /* invalid time value            */
#define  ER_UNIT         115  /* invalid time unit             */
#define  ER_SOCKET       116  /* cannot open a socket          */
#define  ER_HOST         117  /* cannot find host              */
#define  ER_ACCEPT       118  /* cannot accept from remote host*/
#define  ER_SHUTDOWN     119  /* cannot shutdown connection    */
#define  ER_CONNECT      120  /* cannot connect w/ remote host */
#define  ER_NAMEFORMAT   121  /* invalid name format           */
#define  ER_SERVER       122  /* cannot find entry in services */
                              /* file                          */
#define  ER_BUFSIZE      123  /* invalid buffer size           */
#define  ER_SEND         124  /* cannot send data              */
#define  ER_RECEIVE      125  /* cannot receive data           */
#define  ER_ACTUAL       126  /* cannot send amount of data as */
                              /* req'd                         */
#define  ER_RESET        127  /* net hardware !reset or loaded */
#define  ER_RECBUFSIZE   128  /* Rsp Buffer not large enough   */
#define  ER_RET          129  /* cannot retrieve the data      */
#define  ER_LINKDOWN     130  /* the link between workstation  */
                              /* & dbc is down; dbc could be   */
                              /* down                          */
#define  ER_NODATA       131  /* data is not here yet          */
#define  ER_STRINGSIZE   132  /* invalid string size           */
#define  ER_CONNID       133  /* invalid connectid             */
#define  ER_MAXSESS      134  /* num of sessions opened exceed-*/
                              /* ing limit                     */
#define  ER_NOTDONE      135  /* write/send in progress        */
#define  ER_SEQUENCE     136  /* read/write calls out of order */
#define  ER_NOREQ        137  /* no such request               */
#define  ER_TIMEOUT      138  /* read wait timeout             */
#define  ER_BREAK        139  /* break received during wait    */
#define  ER_BADHOST      140  /* bad hosts file                */
#define  ER_CONNREFUSED  141  /* connection refused            */
#define  ER_NOHOST       142  /* cAn't open hostS file DR12820 */
#define  ER_UNKNOWNINT   143  /* DR19967 Unknown external interrupt */

/* CLIWS-7192 --> */ /* TLS error codes from MOSI */
#define  ER_TLS_CLIMETHOD     144 /* TLS connection method creation error */
#define  ER_TLS_APPCONTEXT    145 /* SSL_CTX object creation error        */
#define  ER_TLS_HANDLE        146 /* TLS structure creation error         */
#define  ER_TLS_SETCONNID     147 /* Connect TLS object with connid error */
#define  ER_TLS_LOADCERTSTORE 148 /* Set CA certificates location error   */
#define  ER_TLS_VERIFYPARAM   149 /* Get TLS verification parameter error */
#define  ER_TLS_CONNTIMEOUT   150 /* TLS connection timed out             */
#define  ER_TLS_CONNECT       151 /* TLS handshake error                  */
#define  ER_TLS_WRITE         152 /* TLS write error                      */
#define  ER_TLS_READ          153 /* TLS read error                       */
/* <-- CLIWS-7192 */
#define  ER_TLS_CERT_REVOKED  154 /* OCSP status error CLIWS-8233, CLIWS-8614 */
#define  ER_CLOSED_BY_REMOTE  155 /* Connection closed by remote CLIWS-8512 */
#define  ER_TLS_OCSP_STATUS   156 /* OCSP status error CLIWS-8614 */
#define  ER_TLS_CRL_STATUS    157 /* CRL status error CLIWS-8614          */
#define  ER_NOPROXYHOST       158 /* Could not resolve proxy hostname. */
#define  ER_PROXYAUTHERR      159 /* Proxy authentication error */
#define  ER_PROXYCONNREFUSED  160 /* Proxy server connection refused */
#define  ER_TLS_CERT_VAL_ERR  161 /* Certificate validation error, CLIWS-9597 */

#define  EM_OK             0  /* no error                      */

#define  MOSICNT          62  /* DR12820, CLIWS-7192, CLIWS-7327, CLIWS-8512 */
                              /* CLIWS-9597: 61 -> 62 */

#if !(defined(I370) || (defined(IBM370))) || defined(CLI_MF_GTW)    /* CLIWS-7751 */
/*=============================*/
/*    Error Codes from MTDP    */
/*=============================*/

#define  MTDPBASE        200  /* base error number for mtdp    */

#define  EM_VERSION      202  /* wrong software version        */
#define  EM_BUFSIZE      203  /* invalid or too small buf size */
#define  EM_MOSI         204  /* error coming from MOSI routine*/
#define  EM_FUNC         205  /* invalid mtdp function call    */
#define  EM_ASSIGN       206  /* err parcel rec'd during assign*/
#define  EM_NETCONN      207  /* err in doing a network connect*/
#define  EM_NOTIDLE      208  /* not ready to accept new req   */
#define  EM_REQID        209  /* request id not recognized     */
#define  EM_NOTACTIVE    210  /* no START/CONT outstanding     */
#define  EM_NODATA       211  /* no data received from dbc     */
#define  EM_DATAHERE     212  /* data is ready from dbc        */
#define  EM_ERRPARCEL    213  /* error parcel received         */
#define  EM_NOSCS        214  /* SCS not found in mtdp         */
#define  EM_CONNECT      215  /* error pcl rec'd during connect*/
#define  EM_BUFOVERFLOW  216  /* rsp buffer size !large enough */
#define  EM_TIMEOUT      217  /* Time out waiting for a RESP   */
#define  EM_BREAK        218  /* Break was hit                 */
#define  EM_DBC_CRASH_B  219  /* The DBC has CRASHED before    */
                              /* the request was sent          */
#define  EM_DBC_CRASH_A  220  /* The DBC has CRASHED after the */
                              /* request was sent              */
#define  EM_PUBENCRYPT   221  /* The Public encryption routine */
                              /* failed                        */
#define  EM_CONV         222  /* error in data conversion      */
#define  EM_ALLOCATE     223  /* error in allocating memory BLK*/
#define  EM_NOHOST       224  /* no DBC found in hosts file    */
#define  EM_SUPPORT      225  /* function not supported        */
#define  EM_CHARCODE     226  /* invalid char set code DCR4029 */
#define  EM_CHARNAME     227  /* invalid char set name DCR4029 */
#define  EM_NOSESSNO     228  /* session id not supplied,H0_04 */


/* DR52052, DR68084: GSS/SSO-related error conditions */
#define  EM_SENDAUTHREQFAIL    229  /* authreq parcel send failed  */
#define  EM_RECVAUTHRSPFAIL    230  /* authrsp parcel recv failed  */
#define  EM_RETMAUTHRSPFAIL    231  /* authrsp parcel retm failed  */
#define  EM_INVPCLAUTHRSP      232  /* invalid authrsp parcel      */
#define  EM_DLLLOADFAIL        233  /* LoadLibrary failed          */
#define  EM_GETPROCFAIL        234  /* GetProcAddr failed (UNUSED) */
#define  EM_GSSINITFAIL        235  /* call to gss_init failed     */
#define  EM_GSSCALLFAIL        236  /* call to gss_call failed     */
#define  EM_SENDGSSREQFAIL     237  /* ssoreq parcel send failed   */
#define  EM_RECVGSSRSPFAIL     238  /* ssorsp parcel recv failed   */
#define  EM_RETMGSSRSPFAIL     239  /* ssorsp parcel retm failed   */
#define  EM_INVPCLGSSRSP       240  /* invalid ssorsp parcel       */
#define  EM_ERRFAILPCLAUTHRSP  241  /* error or failure auth rsp   */
#define  EM_ERRFAILPCLGSSRSP   242  /* error or failure sso rsp    */
#define  EM_SSOUNAVAILABLE     243  /* SSO not available           */
#define  EM_SSOLOGONFAIL       244  /* SSO logon failed by gateway */
#define  EM_TIMEEXCEEDED       245  /* MTCLI: timeout from DBCHWL  */
#define  EM_LOCKFAILED         246  /* MTCLI: locking error        */
#define  EM_NOUTF16SUPPORT     247  /* NO UTF16 characterset support*/
                                    /* DR67771, Renamed UCS2 as UTF16*/
#define  EM_TLSCONN            248  /* TLS connection failed       */
#define  EM_WEBSOCKETCONN      249  /* WebSocket connection failed */
#define  EM_MUSTUSETLS         250  /* CLIWS-7647: Must use TLS because*/
                                    /*   gateway's TLS mode is require */
#define  EM_CLOSED_BY_REMOTE   251  /* CLIWS-8512: Connection closed by remote*/
#define  EM_NOTFOUND           252  /* CLIWS-7948: not found */
#define  EM_NOPROXYHOST        253  /* CLIWS-8661: Could not resolve proxy hostname. */
#define  EM_PROXYAUTHERR       254  /* CLIWS-8661: Proxy authentication error */
#define  EM_PROXYCONNREFUSED   255  /* CLIWS-8661: Proxy server connection refused */
#define  EM_TLS_CERT_VAL_ERR   256  /* CLIWS-9597: Certificate validation error on z/OS */

#define  MTDPCNT          56        /* CLIWS-9597: 55 -> 56 */

/***************************************************************/
/*                                                             */
/*                      CLIV2 - error codes                    */
/*                                                             */
/***************************************************************/

#define  CLI2BASE        300  /* base error number for cliv2   */
#define  NOMEM           300  /* out of memory error           */
#define  SESSOVER        301  /* exceeded max no of sessions   */
#define  BADBUFRQ        302  /* invalid buffer size           */
#define  BADLOGON        303  /* invalid logon string          */
#define  NOSESSION       304  /* specified session doesn't     */
                              /* exist                         */
#define  NOREQUEST       305  /* specified req does not  exist */
#define  BADPARCEL       306  /* invalid parcel received       */
#define  REQEXHAUST      307  /* request data exhausted        */
#define  BUFOVFLOW       308  /* parcel data too large for req */
                              /* buffer                        */
#define  REWINDING       309  /* rewinding request             */
#define  NOREW           310  /* cannot rewind the request     */
#define  NOACTIVE        311  /* no active sessions            */
#define  INVALIDFN       312  /* wrong function code           */
#define  DBRPUTZ         312  /* wrong function code           */
#define  BADID           313  /* bad indentifier field in logon*/
                              /* string                        */
#define  NOPASSWORD      314  /* no password in logon string   */
#define  SESSCRASHED     315  /* session has crashed           */
#define  NOTINIT         316  /* cliv2 has not been initialized*/
#define  BADENVIRON      317  /* invalid environment variable  */
#define  NOSPB           318  /* no SPB file found             */
#define  BADSPB          319  /* SPB file has bad format       */
#define  BADRECON        320  /* Bad reconnect options combin- */
                              /* ation                         */
#define  BADLEN          321  /* Bad dbcarea length            */
#define  BADOPTS         322  /* Bad fvar/floc options         */
#define  BADRSUP         323  /* using data !permitted in RSUP */
                              /* DR10377                       */
#define  BADIDAT         324  /* bad indicdata option DCR4278  */
#define  BADREQMODE      325  /* bad req mode  option DCR4278  */
#define  BADDLIST        326  /* Descriptor list length not    */
                              /* multiple of 8                 */

                              /***** added Dec89 DCR5239   *****/
                              /*****  Parameterized SQL    *****/
#define  BADDBCX         327  /* invalid DBCAREA ext eyecatcher*/
                              /* must contain 'DBCX'           */
#define  BADELMTYPE      328  /* invalid DBCAREA ext element   */
                              /* type : must be from 1 to 4096 */
#define  BADELMLEN       329  /* for ptr type element,         */
                              /* fixed len, 10                 */
                              /*** END OF Parameterized SQL ****/

                              /***** added Feb90 DR18467   *****/
                              /***HUT Interface - Direct Req ***/
#define  BAD_MSG_CLASS   330  /* dbcarea.msgclass must be      */
                              /* SysMsgHUTClass : constant 25  */
#define  UNKNOWNINT      331  /* DR19967 unknown external interrupt */
#define  UNVAL2PC        332  /* Unrecognized sess_2pc_mode value   */
#define  NOSESSNO        333  /* DBC session id not supplied   */
#define  DBXMISS         334  /* No DBCAREA extension supplied */
#define  BAD2PXID        335  /* dbx2_id in DBCA2PX is not 'DBX2'      */
#define  BAD2XLEN        336  /* dbx2_len contains incorrect length    */
#define  MAINONLY        337  /* Mainframe only fields contain data    */
#define  UN2FUNC         338  /* dbx2_func contains unrecognized value */
#define  N2PCSESS        339  /* not a 2PC session                     */
#define  PEXTRA          340  /* extraneous parameters are supplied    */
#define  PMISSING        341  /* required parameters are missing       */
#define  PLENMAX         342  /* parameter length is longer than max.  */
#define  UNDBRIPF        343  /* init_with_2pc specifies unrecognized  */
                              /* value.                                */
#define  CRNOCOM         344  /* current run unit not completed.       */
#define  ABT2REQ         345  /* abort required for the current run-   */
                              /* unit on the session                   */
#define  SIOTRUNC        346  /* area too small to describe all        */
                              /* sessions -- truncated                 */
#define  NONETH          347  /* function not supported on network-    */
                              /* attached hosts.                       */
#define  SSINDOUBT       348  /* Session is in doubt.                  */
#define  USREXT          349  /* DCR5571 Cli User Exit function*/
#define  REQOVFLOW       350  /* DR 28060 -  Request size      */
                              /* exceeds maximum.              */
#define  SQLUSREXT       351  /* DCR6842&6852 User SQL exit    */
                              /* H3_00 */
                              /* HN_00 - NDM Error Codes       */
#define  BADTXSEM        352  /* Unrecognized Trans. Semantics option */
#define  BADLANCONF      353  /* Unrecognized Language Conf. option.  */
#define  NOQINFO         354  /* The requested query information is   */
                              /* unavailable.                         */
#define  QEPITEM         355  /* Requested item in DBCHQEP is invalid.*/
#define  QEPALEN         356  /* Return area length is too small.     */
#define  QEPAREA         357  /* No return area supplied in DBCHQEP.  */
/* DR40973 */
/* DR68511 --> */
#define  PDEADLOCK       358  /* DR38016, Parallel CLI deadlock. */

/* DR 38017 - Error messages (359 - 360) are for "Parallel CLI" (PCLI)   */

#define  PCLIENV         359  /* Invalid PCLI environment variable(s).   */
#define  PSEMINIT        360  /* Could not initialize PCLI semaphores.   */
#define  PSHMINIT        361  /* Could not initialize PCLI shared memory */
#define  PSEMCREATE      362  /* Could not setup specific PCLI semaphore.*/
#define  PALLOCSHM       363  /* Could not allocate shared memory for    */
                              /* specific PCLI structure.                */
#define  PALLOCLOCK      364  /* Could not setup lock semaphore for      */
                              /* specific PCLI structure.                */
#define  PATTNINIT       365  /* Could not initialize CLIV2ATTN flag.    */
#define  PCOMMPIPE       366  /* Could not setup communication pipe      */
                              /* between application server and recovery */
                              /* server.                                 */
#define  PRECOVFORK      367  /* Could not fork recovery server.         */
#define  PSERVINIT       368  /* MTDP Servers did not initialize.        */
#define  PMTDPCLEAN      369  /* DR38910, Error on creating MTDPCLEAN.   */
#define  CANCELLOGON     370  /* DR37947, new code for cancel logon.     */
#define  BADSEGNUM       371  /* DR48735, TDSP, Segmented request	 */
			      /* exceed the  limit set by Server	 */
#define  BADSEGOPTS      372  /* DR48735, TDSP, Invalid options for TDSP */
			      /* Segmented Request.			 */
#define  BADSEGMENT      373  /* DR48735, TDSP, Invalid Sequence	 */
			      /* of Segments for Segmented Transaction   */
#define  NOTDSP          374  /* DR50139, TDSP, Added error message  	 */
			      /* No TDSP support in the server		 */
#define  BADEXTENSION    375  /* SSO: DR52052 - bad dbcarea extension    */
#define  NOBUFFER        376  /* DR57420: No buffer provided for request */
                              /* mode 'B'                                */
#define  BADUEFUNC       377  /* DR52050 Invalid function number         */
#define  BADUECNTA       378  /* DR52050 Invalid context area pointer    */
#define  BADUEKIND       379  /* DR52050 Invalid event kind              */
#define  BADUEDATA       380  /* DR52050 Invalid data                    */
#define  BADAPPLQSEM     381  /* DR52050 Couldn't Find/Add sess semaphore*/
#define  BUSYAPPLQSEM    382  /* DR52050 Sess semaphore is busy          */
#define  NOAPPLQSEM      383  /* DR52050 Sess semaphore not found        */
#define  APPLQSEM_NOTACTIVE  384 /* DR52050 Sess semaphore not active    */
#define  NOACTIVEREQUEST 385  /* DR59375 No active request found         */
#define  BADMODE         386  /* DR57144 var_len_fetch err in buffer mode */
#define  BADMOVEMODE     387  /* DR57144 loc_mode err in buffer mode      */
#define  BADPROCOPT      388  /* DR51742 invalid request processing optn  */
#define  NOMULTIPART     389 /* DR57583 Multipart mode not supp by server */
#define  BADRETURNOBJ    390 /* DR57583 Unrecognized Return-object option */
#define  BADCONTDATA     391 /* DR57583 Unrecognized Continued-data option */
#define  INCONTDATA      392 /* DR57583 Inconsistent Continued-data option */
#define  NOALTSUPPORT    393 /* DR61637 No APH support at the server       */
#define  QEPTDP          394 /* DR61527 NULL TDP pointer                   */
#define  NOCURPOSSUP     395 /* DR60695 Keep-response=P is not supported  */
#define  BADKEEPRESP     396 /* DR60695 Positioning requires Keep-response=P */
#define  BADCURPOS       397 /* DR60695 Specified positioning not supported */
#define  NORESPFORREPOS  398 /* DR60695 Curr resp ineligible for positioning */
#define  STMTINVALID     399 /* DR60695 Statement ineligible for positioning */

/**************************************************************/
/* Error Codes 400 - 499 are reserved for WINCLI              */
/* CLIv2 error codes continue at 500                          */
/**************************************************************/

#define  NOENCRYPTION    500 /* DR66712 Encrypt not supported/enabled on gtw */
/* <-- DR68511 */
#define  NOARRAYOPS      501 /* DR68838 Array-operations not supported. */
#define  BADARRAYOPS     502 /* DR89804 Invalid parm combo for array-ops */
#define  BADESSFLAG      503 /* DR58352 Unrecognized  statement_status val */
#define  NOESSSUP        504 /* DR58352 ESS not supported by server. */
#define  BADAPHRESP      505 /* DR68835 Unrecognized consider_APH_resps val */
#define  NOAPHRESP       506 /* DR68835 Server does not supp APH responses */
#define  BADLOGMECH      507 /* DR68842 requested logon mech not available */
#define  USREXTMAXLEN    508 /* DR68842 User exit exceeded max allowable len */
#define  LOGDATAMAXLEN   509 /* DR68842 logmech_data_len exceeded max length */
#define  ICULOADERROR    510 /* DR88745 dynamic loading of ICU library */
#define  BADEXMPTWAT     511 /* DR94622 Inv exempt_sess_from_DBCHWAT spec */
#define  BADSTATEMENTOPT 512 /* DR98026 Invalid parameter for StatementInfo */
#define  NOSTATEMENTOPT  513 /* DR98026 StatementInfo not supported */
#define  BADMAXDECIMAL   514 /* DR98024 Invalid parm for MaxDecimalPrecision */
#define  NOMAXDECIMAL    515 /* DR98024 MaxDecimalPrecision not supported */
#define  BADIDENTITYDATA 516 /* DR98025 Invalid parameter for AGKR_option  */
#define  NOIDENTITYDATA  517 /* DR98025 AGKR_option not supported */
#define  BADDEFCNX       518 /* DR101287 invalid create_default_connection */
#define  FNCLOADERROR    519 /* DR101287 can't get global symbol object*/
#define  DEFCNXSYMERROR  520 /* DR101287 can't locate FNC_Default_Connect() */
#define  DEFCNXCALLERROR 521 /* DR101287 FNC_Default_Connect() call failed */
#define  FNCUNLOADERROR  522 /* DR101287 can't free global symbol object */
#define  XSPNODATAENC    523 /* DR101287 no data enc for default connection */
#define  BADDYNRESULTSET 524 /* DR102528 Invalid parameter for DYN_result */
#define  NODYNRESULTSET  525 /* DR102528 DYN_result not supported */
#define  BADWORKLOADPTR  526 /* DR108909 Invalid Workload pointer */
#define  ICULOADLIBERR   527 /* DR107850 Unable to load tdicu libraries*/
#define  ICULOADSYMBERR  528 /* DR107850 Unable to load tdicu symbols*/
#define  ICUUNLOADERR    529 /* DR107850 Unable to unload tdicu*/
#define  TDUSRLOADERR    530 /* DR112838 TDUSR env-var vs library mismatch */
#define  BADTRANSFORMSOFF   531 /* DR116354 Invalid param for StmtInfo UDT */
#define  NOTRANSFORMSOFF    532 /* DR116354 No StmtInfo UDT support        */
#define  BADPERIODASSTRUCT  533 /* DR116354 Invalid param Period as Struct */
#define  NOPERIODASSTRUCT   534 /* DR116354 No Period as Struct support    */
#define  BADTRUSTEDSESSIONS 535 /* DR116354 Invalid param Trusted Sessions */
#define  NOTRUSTEDSESSIONS  536 /* DR116354 No Trusted Sessions support    */
#define  BADEXTOBJECTNAME   537 /* DR116354 Invalid param for EONP support */
#define  BADMULTISTATEMENTERRORS 538 /* DR121202 Invalid Stat Independence */
#define  NOMULTISTATEMENTERRORS  539 /* DR121202 No Statement Independence */
#define  BADMSGLEN          540 /* DR140617 fetched more data than expected */
#define  BADARRAYTRANSFORMSOFF 541 /* DR145224 Invalid Array Data Type     */
#define  BADXMLRESPONSEFORMAT  542 /* CLAC-28875 Invalid XML Data Type     */
#define  TDWALLETERROR        543 /* CLAC-663 Teradata Wallet Error        */
#define  TDWALLETNOTINSTALLED 544 /* CLAC-663 Teradata Wallet not installed */
#define  BADEXTENDEDLOADUSAGE 545 /* CLAC-29283 Support Extended Multiload  */
#define  BADAUTOREDRIVE       546 /* CLAC-29119 Support Automatic Redrive */
#define  BADTASMFASTFAIL      547 /* CLAC-30478 Support TASM FastFail     */
#define  NOSLOBSUPPORT        548 /* CLAC-30554 Support SLOB              */
#define  BAD1MBRESPROWS       549 /* CLAC-29115 Support 1MB Response rows */
#define  BADCONSTRFORMAT      550 /* CLIWS-7635 Invalid connection string attr */
#define  BADCONSTRVALUE       551 /* CLIWS-7635 Invalid connection string value */
#define  UNKNOWNCONSTRATTRIBUTE 552 /* CLIWS-7635 Unknown conn string attribute */
#define  BADSSLMODE           553 /* CLIWS-7901 Invalid sslmode value */
/* CLIWS-8296 --> */
#define  RACFJWTMISSINGUSERID 554 /* Userid is required in logon string for RACFJWT */
#define  RACFJWTPASSWORD      555 /* Password should be omitted in logon string for RACFJWT */
#define  IDTUSERIDUNDEF       556 /* Userid is not defined */
#define  IDTPASSTICKET        557 /* PassTicket Replay Protection is not bypassed */
#define  IDTRACROUTEFAIL      558 /* Error occurred with the RACROUTE REQUEST=VERIFY macro */
#define  IDTSMALLWORKAREA     559 /* JWT workarea length is not large enough */
#define  IDTTOKENERROR        560 /* Token is not set up correctly */
#define  IDTNOTAPFAUTH        561 /* Data set name or Task is not APF-authorized */
#define  IDTLONGUSERID        562 /* Userid is too long. Valid length is 1-8 bytes */
#define  IDTUSERIDNOTAUTH     563 /* Userid is not RACF authorized to use the ICSF API Service */
#define  IDTUSERABEND         564 /* Abnormal USER Abend condition is detected for Userid */
#define  IDTSYSTEMABEND       565 /* Abnormal SYSTEM Abend condition is detected for Userid */
#define  IDTINVALIDTOKEN      566 /* Invalid configuration for PKCS#11 Token */
#define  IDTUNEXPECTEDERROR   567 /* Unexpected error occurred from IDT module */
#define  RACFJWTERROR         568 /* Error occurred while processing RACFJWT in CLI */
/* <-- CLIWS-8296 */
#define  IDPURLNOTFOUND       569 /* CLIWS-7948 IdP metadata URL not found */
#define  AUTHENDPOINTNOTFOUND 570 /* CLIWS-7948 Authorization endpoint not found */
#define  TOKENENDPOINTNOTFOUND 571 /* CLIWS-7948 Token endpoint not found */
#define  BADHTTPRESPONSE       572 /* CLIWS-7948 Bad HTTP response code */
#define  HTTPSERVERERROR       573 /* CLIWS-7948 Error in local HTTP server */
#define  HTTPTIMEOUT           574 /* CLIWS-7948 Timeout waiting for reqeust or response */
#define  AUTHCODENOTFOUND      575 /* CLIWS-7948 Authorization code not found */
#define  JWTNOTFOUND           576 /* CLIWS-7948 JWT not found */
#define  BADHTTPREQEUST        577 /* CLIWS-7948 Bad HTTP request received */
#define  BADSSLCRCVALUE        578 /* CLIWS-8233 Bad sslcrc value  CLIWS-8614 */
#define  BROWSERCMDERROR       579 /* CLIWS-8410 Error running browser command */
/* CLIWS-8618 --> */
#define  IDTPASSTICKETERR      580 /* PassTicket Authorization failure for Userid */
#define  IDTRACROUTEERR        581 /* RACROUTE Authorization failure */
#define  IDTACEENOTFOUND       582 /* Unable to determine submitter Userid for Jobname */
#define  IDTTOKENNOTICSFDEF    583 /* PKCS#11 Token is not ICSF defined */
#define  IDTUNDEFRESOURCE      584 /* Profile resource name is not defined */
#define  IDTUNAUTHACCESS       585 /* Userid is unauthorized to access */
#define  IDTINVALIDRSAKEYLEN   586 /* PKCS#11 Token RSA key length is invalid */
/* <-- CLIWS-8618 */
#define  MISSINGJWSPRIVKEY     587 /* CLIWS-9071 LOGMECH=BEARER requires jws_private_key value in connection string*/
#define  LOADPRIVKEYERROR      588 /* CLIWS-9071 Error loading private key */
#define  UNSUPPORTEDJWSALG     589 /* CLIWS-9071 Unsupported JWS algorithm */
#define  MAKEJWTERROR          590 /* CLIWS-9071 Error creating JWT for bearer flow */
#define  PEERCERTERROR         591 /* CLIWS-9074 IdP server cert failed validation */
#define  UNKNOWNHTTPERROR      592 /* CLIWS-9074 An unknown HTTP error occurred */
#define  DEVICEAUTHERROR       593 /* CLIWS-9070 Device Authorization Grant error */
#define  DEVICETIMEOUT         594 /* CLIWS-9070 Timed out waiting for confirm user-code for Device Auth Grant */
#define  DEVICEFILEOPENERR     595 /* CLIWS-9070 File open error for writing user-code for Device Auth Grant */
#define  ACCESSDENIED          596 /* CLIWS-9070 Authorization request was denied */
#define  EXPIREDTOKEN          597 /* CLIWS-9070 The device code has expired. Try to logon again */
#define  FIPSINITERROR         598 /* CLIWS-9221 Error initializing FIPS mode */
#define  BADLOGMECHDATA        599 /* CLIWS-9380 Error in logmech data value */
#define  BADLOGMECHNAME        600 /* CLIWS-9492 Error in logmech name value */
#define  CLAIMNOTFOUND         601 /* CLIWS-9492 Claim not found in JWT */
#define  JWTERROR              602 /* CLIWS-9492 Error processing JWT */
#define  BADSSLPROTOCOL        603 /* CLIWS-9637 Bad sslprotocol value */

/* 04.02.00.00 end */
/***************************************************************/
/* constant CLI2CNT need to be updated whenever errmsg is added*/
/***************************************************************/
                     /* H3_00 - CLI2CNT changed from 52 to 58. */
                     /* HN_00 - NDM Support                    */
/* DR40973 */
#define  CLI2CNT          191 /* CLI2CNT including NDM codes   */
                              /* 71 + 3, 3 added for TDSP, DR48735 */
                              /* 74 + 1,   added for TDSP, DR50139 */
                              /* 75 + 1,   added for SSO,  DR52052 */
                              /* 76 + 1,   added for NOBUFFER,DR57420*/
                              /* 77 + 8,   added for MTCLI,DR52050*/
                              /* 85 + 1,   added for DR59375      */
                              /* 86 + 2,   added for DR57144      */
                              /* 88 + 1,   added for DR51742      */
                              /* 89 + 4,   added for DR57583      */
                              /* 93 + 1,   added for DR61637      */
                              /* 94 + 6,   added for DR61527      */
                              /*                 for DR67168      */
                              /*                 for DR60695      */
                              /*                 for DR68835      */
                              /*                 for DR58352      */
                              /* DR38016,  added PDEADLOCK     */
                              /* DR 38017, added 10 messages.  */
                              /* DR 38910, added PMTDPCLEAN.   */
                              /* DR 37947, added CANCELLOGON.  */
                              /* DR 48735, added BADSEGNUM     */
                              /* DR 48735, added BADSEGOPTS    */
                              /* DR 48735, added BADSEGMENT    */
                              /* DR 50139, added NOTDSP        */
                              /* DR 94622, added BADEXMPTWAT   */
                              /* DR 98026, added BADSTATEMENTOPT, 106 to 107 */
                              /* DR 98026, added NOSTATEMENTOPT,  107 to 108 */
                              /* DR 98024, added BADMAXDECIMAL,   108 to 109 */
                              /* DR 98024, added NOMAXDECIMAL,    109 to 110 */
                              /* DR 98025, added BADIDENTITYDATA, 110 to 111 */
                              /* DR 98025, added NOIDENTITYDATA,  111 to 112 */
                              /* DR 102528, added BADDYNRESULT, 112 to 113 */
                              /* DR 102528, added NODYNRESULT, 113 to 114 */
                              /* DR 108909, added BADWORKLOADPTR, 114 to 115 */
                              /* DR 107850, added ICULOADLIBERR, 115 to 116 */
                              /* DR 107850, added ICULOADSYMBERR, 116 to 117 */
                              /* DR 107850, added ICUUNLOADERR, 117 to 118 */
                              /* DR 116354, added EONP and other, 118 to 125 */
                              /* DR 121202, added Stat. Indep.,   125 to 127 */
                              /* DR 140617, added BADMSGLEN,      127 to 128 */
                              /* DR 145224, added Array Data Type 128 to 129 */
                              /* CLAC-28875, added XML Data Type  129 to 130 */
                              /* CLAC-663,   added WDWALLET error 130 to 132 */
                              /* CLAC-29283, added Extended ML    132 to 133 */
                              /* CLAC-29119, added Auto Redrive   133 to 134 */
                              /* CLAC-30478, added TASM FastFail  134 to 135 */
                              /* CLAC-30554, added SLOB support   135 to 136 */
                              /* CLAC-29115, added 1MB Resp Rows  136 to 137 */
                              /* CLIWS-7635, connect string errs  138 to 140 */
                              /* CLIWS-7901, invalid sslmode err  140 to 141 */
                              /* CLIWS-8295, support RACF JWT     141 to 156 */
                              /* CLIWS-7948, federated SSO        156 to 165 */
                              /* CLIWS-8233, OCSP support         165 to 166 */
                              /* CLIWS-8410, browser cmd err      166 to 167 */
                              /* CLIWS-8618, Update IDT error     167 to 174 */
                              /* CLIWS-9071, Client Credential    174 to 180 */
                              /* CLIWS-9070, Device Flow          180 to 185 */
                              /* CLIWS-9221, FIPS mode            185 to 186 */
                              /* CLIWS-9380, Logmech data         186 to 187 */
                              /* CLIWS-9492, Logmech name         187 to 188 */
                              /* CLIWS-9492, Claim not found      188 to 189 */
                              /* CLIWS-9492, JWT error            189 to 190 */
                              /* CLIWS-9637, sslprotocol error    190 to 191 */

/***************************************************************/
/* following defines  no of record offset cells for errmsg.txt */
/***************************************************************/

#define  ERROR_REX       (CLI2CNT+MTDPCNT+MOSICNT)  /* DR12820 */

/************************************************************/
/*          Following are error routines in GENERR.C        */
/************************************************************/

 /* DR52059, These routines are called in  cliv2.c          */
 Int32 CatOpen(); /* DR68829: changed return type from void */
 UInt32 CatGetMsg(char *mes, UInt16 msglen, Int16 set, Int16 MesgNo);
 void CatClose();

#endif

/***************************************************************/
/*                                                             */
/*                      END OF COPERR.H                        */
/*                                                             */
/***************************************************************/
#endif  /* COPERR_H */



================================================
FILE: src/teradata/coptypes.h
================================================
#ifndef COPTYPES_H
#define COPTYPES_H
#define COPTYPES_H_REV "20.00.00.00"
/*******************************************************************************
 *
 *
 *  WARNING: Because coptypes.h is shared and is used as a header
 *           file for mainframe SAS/C compiles under VM/CMS, the logical
 *           record length (i.e., line length) must not exceed 80
 *           characters.
 *-----------------------------------------------------------------------------*
 *                   <-- ( 8 0    c h a r a c t e r s) -->
 *******************************************************************************
 *  TITLE:       COPTYPES.H .. Major type definitions for "C"        20.00.00.00
 *
 *  Purpose      To contain the major type definitions for
 *               the workstation code written in "C".
 *
 *  Copyright 1983-2023 by Teradata Corporation.  All rights reserved.
 *
 *  Description  This include file contains the major type
 *               definitions for the workstation version of
 *               BTEQ and CLIV2.
 *
 *  History
 *    D.1   86Jul28  KKL  DCR3074  Coded
 *    D.2   87Jan30  DRP  DR8682   add link down state MTDP
 *    F.3   87Feb26  KKL  DR9379   Added SysMaxSess
 *    F.4   87Apr29  DAA  DR9321   fix SysMaxRequestSize
 *    F.5   87Oct28  SCT  DR 10471 Narrowed to <=65 columns
 *    F.6   87Nov17  DAA  DR 10471 fix it
 *    F.7   88Jan28  WHY  DR 11926 (MR# fe87-18102a0)
 *    G0_01 88Sep23  MBH  DR 14058 Shipped as CAPH file
 *    G0_02 89Jun05  KKL  DR 16460 Added OS dependent stuff
 *    G0_03 89Jun05  KKL  DR 16460 Re-ship DR16460
 *    G0_04 89Jun07  KKL  DR 16494 redefine errno by !using
 *                                 stdlib.h
 *    G0_05 90Feb05  KXO  DR 18467 Add constant for DirectReq
 *    G0_06 90May07  KBC  DR 19675 Use proper errono definition
 *                                 for Microsoft C5.1 or later
 *    G0_07 90Sep14  KXO           Revised for UNIX TOOL KIT
 *    G0_08 90Sep25  KBC  DR 19675 Fix to still compile under C5.0
 *    G0_09 90Nov13  KBC  DR 20832 Remove errno definition for PC.
 *                                 PC programs must include stdlib.h
 *                                 to get proper definition of errno,
 *                                 since it is compiler level
 *                                 dependent.
 *    G0_10 91Jan03  JML           Fix for syntax error induced
 *                                 during code beautification (G0_07).
 *    G0_11 91Jan05  DJL  DCR 5366 Tandem interface.
 *    G0_12 91Feb15  GLL  DCR5689  Add definition for FILEPTR
 *    G0_13 92Jan13  BTA  DCR5957  Support Restructured Mosidos
 *    H0_00 93May20  SNT  DCR6711  Created for Release H.0
 *    H0_01 93Aug17  Bg1  DR28043  Convert the unsigned char type to char.
 *    HN_00 94Sep12  JAE  DCR6848  Added NDM support.
 *    HN_01 94Nov07  SNT  DCR6848  Removed NDM compile flag.
 *    HN_02 95FEB09  ALB  DR32294 Merged 5.0 and AFCAC code.
 *    HN_03 95Mar13  BG1  DR32448  Defined BOOL_CLI, character type
 *                                 boolean for Cli only.
 *    HN_04 95Mar30  ALB  DR28043  Changed Boolean typedebf back to unsigned
 *                                 char, added typedef for XWINDOWS support.
 *    HN_05 95Mar30  JAE  DR32448  Corrected fix from BG1 (added #ifndef)
 *    H3_00 95Sep13  TH4  DR34047  Added the #ifndef XXXX #define XXXX.
 *    H3_01 96Mar08  SYY        DR35631  Merge WinCLI 3.0 changes into main
 *    04.00.00.01 TH4 96Jul12   DR36785 Version number change.
 *    04.04.00.00 MDV 98Jul07   DR43005 Changed the value of SysMaxParcelSize
 *                                      to accommodate the 64K row size.
 *    04.05.00.00 CSG 2000Apr10 DR50943 Added STRING_CLI for compiling with
 *                                      C++ compilers.
 *    04.06.00.00 ASG 2001Jan17 DR52052 Support SSO
 *    04.06.01.00 CSG 2001May25 DR55267 Native 64bit support
 *    04.06.02.00 CSG 2002Mar27 DR58353 Sub-Second support
 *    04.07.00.00 CSG 2002May08 DR58369 RFC: Port 64 bit CLIv2 to HPUX.
 *    04.07.00.01 CSG 2002May18 DR58915 RFC: Port 64 bit CLIv2 to AIX.
 *    04.07.00.02 CSG 2002Jun04 DR59337 Port CLI to 64 Bit Windows XP.
 *    04.07.00.03 CSG 2002Jun04 DR57320 Support Increased response size.
 *    04.07.00.04 CSG 2002Jun17 DR61637 Support enlarged parcel usage.
 *    04.07.00.05 CSG 2002Aug27 DR60830 Port CLI to 64bit linux
 *    04.07.01.00 ASG 2003Feb10 DR66590 Add 64-bit integer data type.
 *    04.07.01.01 mg180007 2003Apr24 DR68084 renamed TimeStamp to time_stamp
 *    04.08.00.00 mg180007 2003Jul22 DR68511 clean up defines, prototypes
 *    04.08.00.01 ASG      2004Feb23 DR85431 universalize headers
 *    04.08.00.02 ASG      2004Jun22 DR87725 avoid name collision with BTEQ
 *    04.08.01.00 mg180007 2005Apr21 DR93472 added header revision info
 *    04.08.01.01 bh185000 2005Jun10 DR96357 added ifndef to resolve the issue
 *    04.08.02.00 ASG      2006Feb14 DR102061 reconfig for 80-bye lines
 *    04.08.02.01 ASG      2006Feb16 DR67535 accommodate TPT data types
 *    04.09.00.00 ASG      2006Sep06 DR103365 handle exapanded hash buckets
 *    04.09.00.01 bh185000 2006Oct03 DR105994 sepetate errpt_t and error_t
 *    12.00.00.02 fs185009 2006Nov29 DR108852 version change to 12.0.x
 *    13.00.00.00 mg180007 2008Jan09 DR117140 copyright update
 *    13.10.00.00 kl185018 2009Jun23 DR133468 TTU13.1 Version Renumbering
 *                                            to TTU13.10
 *    14.10.00.00 mg180007 2012Feb03 CLAC-663 added cli_buffer_desc
 *    14.10.00.01 mg180007 2012Apr16 CLAC-663 fixed cli_buffer_desc
 *    14.10.00.02 mg180007 2012Apr28 CLAC-29437 use newer HPUX-IA toolset
 *    15.10.00.00 mg180007 2014Apr28 CLAC-32368 version and copyright update
 *    17.00.00.00 hs186016 2018Nov13 CLIWS-7163 version and copyright update
 *    17.10.00.00 hs186016 2020Jul27 CLIWS-7606 version change to 17.10
 *    17.10.00.01 hs186016 2021Jun03 CLIWS-7751 Support mainframe on z/OS
 *    17.20.00.00 hs186016 2022Jan01 CLIWS-8198 version and copyright update
 *    20.00.00.00 hs186016 2023May11 CLIWS-8696 version and copyright update
 ******************************************************************************/

#ifndef TPT_BUILD                       /* DR67535 */
/* DR68511 */
#ifndef BOOLT_CLI                       /* HN_05, added #ifndef  */
typedef char              bool_t;       /* boolean data types    */
typedef bool_t            flag_t;       /* DR32448 HN_03 bg1     */
#endif
#endif                                  /* DR67535 */

/* DR55267, 58369, 58915: defined generalized macro for 64 bit platforms */
/* DR59337: added implicit macro _WIN64 */
/* DR60830: Added implicit macro __ia64__ for 64bit linux on IA64 */
/* CLAC-29437: added _ILP32 for hpux-ia64.32 */
#if (defined(_LP64) || defined(__LP64__) || defined(__64BIT__) || \
    defined(_WIN64) || defined (__ia64__)) && !defined(_ILP32)
#define CLI_64BIT
#endif
#ifndef TPT_BUILD       /* DR67535 */
/*================*/
/*  General       */
/*================*/

/*
** Single Byte Data Types
*/
typedef unsigned char      Byte;         /* A 1 byte Unsigned Int     */

/*
** Two Byte Data Types
*/
typedef short              Int16;        /* A 2 byte integer          */
typedef short *            Int16_Ptr;    /* A Pointer to a 2 byte int */
typedef unsigned short     Word;         /* A 2 byte Unsigned Int     */
typedef unsigned short     UInt16;       /* A unsigned 2 byte integer */
typedef unsigned short *   UInt16_Ptr;   /* ptr unsgn 2 byte integer  */

/*
** Four Byte Data Types
*/
/* DR55267:  Int32 and UInt32 defined as int and unsigned int */
typedef int                Int32;        /* A 4 byte integer            */
typedef int *              Int32_Ptr;    /* A ptr to a 4 byte int       */
typedef unsigned int       UInt32;       /* A 4 byte unsigned integer   */
typedef unsigned int *     UInt32_Ptr;   /* A ptr to a 4 byte unsgn int */
typedef unsigned int       Double_Word;  /* A 4 byte Unsigned Int       */

/*
** Misc. Data Types
*/
typedef unsigned           protag_t;
typedef char *             PtrType;
/* DR87725 --> */
#if defined(__MVS__) && !defined(CLI_MF_GTW) /* CLIWS-7751 */
typedef void *             storid_t;
#else
typedef char *             storid_t;
#endif
/* <-- DR87725 */

/* DR 66590 --> */
typedef long long          Int64;
typedef unsigned long long UInt64;
/* <-- DR 66590 */

#ifndef STRING_CLI                     /* DR50943, For C++ compilers */
   typedef char *          string;     /* char array pointer         */
#endif

typedef char *             string_t;   /* zeroterminated string */
typedef unsigned           systag_t;
typedef Int32              time_stamp; /* The Time Stamp type *//* DR68084 */
               /* ->  DR58353  */
#endif         /* DR67535 */
typedef struct save_time
{
    Int32 Days;
    Int32 Hours;
    Int32 Minutes;
} SAVETIME;

/* Utilities can use this structure to fetch  min and max    */
/* timestamp precision suppoted  on a platform using DBCHQE  */
typedef struct
{
    Int16 minimum;
    Int16 maximum;
} TSP_QUERY;
/* DR 58353   <- */

typedef struct cli_buffer_desc_struct { /* CLAC-663 -> */
#ifdef CLI_64BIT
    UInt64      length;
#else
    UInt32      length;
#endif
    void       *value;
} cli_buffer_desc, *cli_buffer_t;

#define CLI_C_EMPTY_BUFFER {0, NULL}    /* <- CLAC-663 */

/*================*/
/*  Error type    */
/*================*/

/* DR55267: type changed to UInt32 */
typedef  struct  err_code
{
    UInt32      e_class;
    UInt32      e_reason;
    UInt32      e_syst;
} *errpt_t
#ifndef NO_CLIV2_ERROR_T              /* DR96357, DR105994 */
, error_t;
#else
;
#endif                                /* DR96357, DR105994 */

/***************************************************************/
/*            Some CONSTANTS needed by BTEQ and CLI.           */
/***************************************************************/

#define  Ok                  0
#define  NilPtr              0
#define  TRUE                1
#define  FALSE               0

#define  DATAHERE            1
#define  NODATA              0

#define  IDLE                0
#define  ACTIVE              1

#define  OWNMOSI             1
#define  OWNMTDP             2
#define  OWNCLI              3
#define  SCS_VC_DOWN         2
#define  VERSION             2

#define  SysMaxDelayTime     50    /* max time to wait before  */
                                   /* polling MTDP (in 10ths of*/
                                   /* a second). On PC, this   */
                                   /* should be a value < 10   */
#define  SysMaxErrMsg        256
#define  SysMaxWarnMsg       128   /* DR85431 */
/* DR85431 --> */
#if defined(__MVS__) && !defined(CLI_MF_GTW)     /* CLIWS-7751 */
#define  SysMaxName          32
#define  SysMaxParcelSize    64260               /* DR44785, 39481 */
#else
#define  SysMaxName          30
#define  SysMaxParcelSize    65473 /* DR43005 */
#endif
/* <-- DR85431 */
#define  SysMaxRequestSize   8192

/*========================================*/
/* FOR APH and Extended Resposnse support */
/*========================================*/
#define  SysExtMaxParcelSize 1048514  /* DR57320 */
#define  SysAltMaxRequestSize 1048500 /* DR61637 */

#ifdef WIN32
#define  SysMaxSess          200  /* Max session No supported */
#else
#define  SysMaxSess          64   /* Max session No supported */
#endif
#define  SysMaxAnsiMsg       128   /* Max length of FIPS Flagger msg   */
#define  ParMaxFIPSFlags     10    /* Max FIPS flagged items per query */

/*=============================================================*/
/* DR18467 FEB90 KXO, Direct Req.                              */
/* following values should be same as in DBCTYPES.H of HUT(ARC)*/
/*=============================================================*/

#define  MSG_HUT_CLASS       25    /* SysMsgHUTClass   of HUT  */
#define  MSG_NO_ADDR          0    /* MsgNoAddr        of HUT  */
#define  MSG_TRANSIENT_ADDR   1    /* MsgTransientAddr of HUT  */
#define  MSG_PROCBOX_ADDR     2    /* MsgProcBoxAddr   of HUT  */
#define  MSG_HASHBOX_ADDR     3    /* MsgHashBoxAddr   of HUT  */
#define  MSG_GROUPBOX_ADDR    4    /* MsgGroupBoxAddr  of HUT  */
#define  MSG_EXPHASHBOX_ADDR 12    /* DR103365 */
#define  MAILBOX_SIZE         6

/*=============================================================*/
/*  END of DR18467 FEB90 KXO, Direct Req.                      */
/*=============================================================*/


/***************************************************************/
/* The followings are considered platform/protocol dependent.  */
/* Until we figure out where/how to group these into a more    */
/* portable fashion, here will be their temp home.             */
/***************************************************************/
/* Suggestions:                                                */
/*   1. bool_t should be used instead of 'Boolean' which may   */
/*      has a different definition from different compiler.    */
/*   2. Applications should have "errno" declared in coptypes.h*/
/*      due to different "errno" declaration from Microsoft    */
/*      Compiler.                                              */
/*                                                             */
/***************************************************************/

/* DR68511 --> */
#ifndef TPT_BUILD                  /* DR67535 */
#ifndef XWINDOWS                   /*  ALB - HN-04  DR 28043     */
typedef unsigned char Boolean;     /*  Boolean data types        */
#endif
#endif                             /* DR67535 */
/* <-- DR68511 */


/* CLIWS-7751 --> */
#if defined (CLI_MF_GTW)
    #define CLI_INLINE
#else
/* CLAC-32983 --> */
    #if defined(WIN32) || defined(AIX)
        #define CLI_INLINE __inline
    #else
        #define CLI_INLINE inline
    #endif
/* <-- CLAC-32983 */
#endif  /* CLI_MF_GTW */
/* <-- CLIWS-7751 */

/***************************************************************/
/*                       End of COPTYPES.H                     */
/***************************************************************/
#endif  /* COPTYPES_H */



================================================
FILE: src/teradata/dbcarea.h
================================================
#ifndef DBCAREA_H
#define DBCAREA_H
#define DBCAREA_H_REV "20.00.00.01"
/*******************************************************************************
 *  TITLE:       DBCAREA.H .. CLIv2 type definitions for "C"         20.00.00.01
 *
 *  Purpose      To contain the type definitions needed for
 *               interfacing to CLIv2 from a "C" program.
 *
 *  Copyright 1987-2024 by Teradata Corporation.  All rights reserved.
 *
 *  Description  This include file contains the type
 *               definitions needed for an application program
 *               to make calls to CLIv2.
 *
 *  History
 *
 *    F.1    87Nov23  DRP  DR 11416 Coded
 *    F.2    88Jan25  KKE  DR11889  Fix 4  compatiblity
 *                                  w/ other DBCAREAs
 *    F.3    88Feb26  DRP  DR11889  add a ;
 *    G0_00  88Jul25  SN   Created  for Release G.0
 *    G0_01  88Oct06  WBJ  DCR4029 & other G.0 mods
 *    G0_02  88Oct26  WBJ  DCR4029  Fix dupe element
 *    G0_03  89Dec15  KXO  DCR5239  Add DBCAREA EXT(ParamSQL)
 *           90Jan15  KXO  DR18467  DirectReq (HUT interface)
 *    G0_04  90May07  KBC  DR19675  Add CLI prototypes
 *    G0_05  90Sep14  KXO           Revised for UNIX TOOL KIT
 *    G0_06  90Nov13  KBC  DR20832  Add Record_Error
 *    G0_07  90Dec18 HEIDI DR21134  Remove Record_Error (put in cliv2.c)
 *    G0_08  91Jan05  DJL  DCR5366  Tandem Interface
 *    G0_09  91May02 HEIDI DR21506  ifdef out prototyping for PP2(II)
 *    G0_10  91Dec01  BGN  DCR6174  First revision for OS/2
 *    G0_11  91Dec31  KBC  DCR5966  Support for Windows WinCLIV2.DLL
 *    H0_00  93May20  SNT  DCR6711  Created for Release H.0
 *    H0_01  94Apr04  JAE  DCR6172  Merged 2PC code for Genesis II
 *    H0_02  94May13  JAE  DR29062  Changed give_msg to conf_logoff
 *    H0_03  94Jun29  JAE  DR29062  Added give_msg back.
 *    H0_04  95Mar08  JAE  DCR6949  #define for delay options, AP Reset.
 *    H0_05  95Mar15  ALB  DR32387  added connect_wait to CLIV2options.
 *    H0_06  95Mar28  JAE  DCR6847,6848  NDM Support.
 *    H0_07  95Mar28  JAE  DR24722  This change included this DR as well
 *                                  as other mainframe changes. H0_07 is
 *                                  used here to label the code merge.
 *    H3_00  95Aug03  MDV           Merged NDM code and other fixed DRs.
 *                                  Added sess_2pc_mode by JAE.
 *                                  Added DBF2PC function code by JAE.
 *                                  Added init_with_2pc by JAE.
 *                                  Added DBFIWPF, DBFRECV, DBFALSC,
 *                                  DBFCMD function codes by JAE.
 *                         DR24722  Cleanup prototypes for IBM370 by AFK.
 *                         DCR6669  Added NP changes G0_10 - G0_12 by RPA.
 *                         DR29485  ActivityCount and QUIET by AFK.
 *                         DCR6847  NDM support by JAE.
 *    H3_01  95Aug14  TH4  DR33969  Added extern "C" for C++ linker.
 *    H3_02  95Aug23  JAE  DR32294  Removed #ifdef NDM .
 *    H3_03  96Mar08  SYY  DR35631  Merge WinCLI 3.0 changes into mainstream.
 *
 *    04.00.00.01 TH4 96Jul12 DR36785 Version number change.
 *    04.02.00.00 JTH July97  DR40000 Code merge
 *    04.02.00.01 TH4 97Sep05 DR36456 64KB row support.
 *    04.02.00.02 TH4 97Sep10 DR40417 ANSI Date/Time Support.
 *    04.04.00.00 JTH 98Feb26 DR41392 consolidate this header file
 *                For Windows use def PCDOS/PCDOSWIN/WIN32
 *                For IBM mainframe use def IBM370,I370
 *                Unix is the default
 *    04.04.00.01 DHF 98Jun12 DR41392 Updated to add WIN32 also.
 *
 * Revision    Date       DCR   DID      Comments
 * ----------- --------   ----- -------- ---------------------------------------
 * 04.04.00.02 08/11/1998 43449 TH4      Fix the pointer to context area
 * 04.05.00.00 01/18/2000 45724 CSG      CLI2 C++ safe
 * 04.05.00.01 03/03/2000 48735 CSG      TDSP Support
 * 04.05.00.02 08/04/2000 48823 ASG      OS/390 USS
 * 04.06.01.00 05/25/2001 55267 CSG      Native 64bit support
 * 04.06.01.01 09/03/2001 52050 ASG      Multi-threaded support.
 * 04.06.01.02 11/05/2001 58769 ASG      Support TWB operators with OS/390 C/C++
 * 04.06.02.00 03/27/2002 58353 CSG      CLI Sub-Second Timing Capability
 * 04.07.00.00 05/08/2002 58369 CSG      Native 64 bit support to CLIv2 on HPUX
 * 04.07.00.01 05/18/2002 58915 CSG      Native 64 bit support to CLIv2 on AIX.
 * 04.07.00.02 06/06/2002 59337 CSG      Port CLI to 64 Bit Windows XP.
 * 04.07.00.03 06/17/2002 61637 CSG      Support enlarged parcel usage.
 * 04.07.00.04 06/03/2002 57583 ASG      LOB support
 * 04.07.00.05 08/22/2002 52059 CSG      L10N support
 * 04.07.00.06 09/17/2002 64147 CSG      JDBC compilation fails on AIX.
 * 04.07.01.00 03/31/2003 60695 ASG      Cursor positioning.
 * 04.07.01.01 04/08/2003 67881 cc151010 Remove DBCHQE() prototype.
 * 04.07.01.02 04/03/2003 66712 ASG      Data encryption.
 * 04.07.01.03 04/24/2003 68084 mg180007 changed TimeStamp to time_stamp
 * 04.08.00.00 07/22/2003 68511 mg180007 clean up defines, prototypes
 *                        68139          self-sufficient headers
 *                        68140          fix EXPENTRY definition
 * 04.08.00.01 12/02/2003 68838 ASG      Array Support for DML Operations
 * 04.08.00.02 09/28/2003 68835 ASG      APH / 1 MB Responses
 * 04.08.00.03 09/29/2003 69228 CSG      Support for character object state
 * 04.08.00.04 02/10/2004 85431 ASG      Universalize headers
 * 04.08.00.05 02/10/2004 58352 ASG      ESS
 * 04.08.00.06 01/27/2004 84603 CSG      Add pack(1) for Win-32 platform
 * 04.08.00.07 05/14/2004 68842 mg180007 TTU 8.0 Security support
 * 04.08.00.08 06/01/2004 87586 ASG      Fix linkage specifier for TWB
 * 04.08.00.09 08/09/2004 89199 DRL      Re-add fix for DR83781
 * 04.08.00.10 09/17/2004 89408 ASG      Fix 64-bit problem with array ops
 * 04.08.00.11 12/06/2004 91963 ASG      Correct 32- vs. 64-bit discrepancy
 * 04.08.01.00 02/14/2005 92232 mg180007 merge and update copyright year
 * 04.08.01.01 03/01/2005 90995 mg180007 Support Unicode credentials
 * 04.08.01.02 04/21/2005 93472 mg180007 Added header revision info
 * 04.08.01.03 06/23/2005 94622 ASG      dual-component feature support
 * 04.08.01.04 08/11/2005 97745 fs185009 DBCHSAD supporting 64-bit pointers
 * 04.08.02.00 12/15/2005 98024 bh185000 Support for MaxDecimalPrecision
 *                        98025 bh185000 Support for AGKR_option
 *                        98026 bh185000 Support for StatementInfo
 * 04.08.02.01 02/14/2006 102061 ASG      reconfig for 80-byte lines
 * 04.09.00.00 05/30/2006 102528 bh185000 Support for DynamicResultSets
 * 04.09.00.01 07/11/2006 101287 mg180007 Support for Default Connection
 * 04.09.00.02 11/02/2006 101663 mg180007 Added send_delegated_credentials
 * 04.09.00.03 11/28/2006 108909 bh185000 Added workload_len and workload_ptr
 * 12.00.00.04 11/29/2006 108552 fs185009 version change from 4.09.x to 12.0.x
 * 12.00.00.05 02/06/2007 110710 bh185000 Use low-order byte to keep MF happy
 * 13.00.00.00 01/09/2008 117140 mg180007 copyright update
 * 13.01.00.00 11/17/2008 116354 bh185000 Support Extended Object Name Parcel
 * 13.01.00.01 12/09/2008 123516 bh185000 Support StmtInfo UDT Transforms Off
 * 13.01.00.02 12/18/2008 122304 bh185000 Support Trusted Sessions Security
 * 13.01.00.03 03/29/2009 126915 bh185000 Support TASM Utility Management
 * 13.10.00.00 06/23/2009 133468 kl185018 Version change from 13.01 to 13.10
 * 14.00.00.00 10/06/2010 121202 bh185000 Support Statement Independence
 * 14.00.00.01 12/15/2009 118581 mg180007 Added logon_timeout
 * 14.00.00.02 02/15/2011 145358 bh185000 Added automaticRedrive
 * 14.00.00.03 02/24/2011 145224 bh185000 Support Array Data Type
 * 14.10.00.00 01/10/2012 CLAC-28875 bh185000 Support XML Data Type
 * 14.10.00.01 03/22/2012 CLAC-28857 mg180007 Support ESS option 'D'
 * 14.10.00.02 05/16/2012 CLAC-29283 bh185000 Support Extended Multiload
 * 14.10.00.03 07/20/2012 CLAC-29878 hs186016 Fix comment in D8XIELEM structure
 * 14.10.00.04 10/24/2012 CLAC-29918 hs186016 Remove invalid request_mode 'D'
 * 14.10.00.05 02/21/2013 CLAC-30478 hs186016 Support TASM FastFail
 * 15.00.00.00 11/14/2013 CLAC-30554 hs186016 Support SLOB
 * 15.10.00.00 04/28/2014 CLAC-32368 mg180007 version and copyright update
 * 15.10.00.01 07/30/2014 CLAC-31559 bh185000 SLOB Server to Client support
 * 15.10.00.02 08/06/2014 CLAC-32067 hs186016 Support timer in DBCHCL
 * 15.10.00.03 09/12/2014 CLAC-32071 hs186016 Support User Selectable directory
 * 16.00.00.00 06/24/2015 CLAC-29115 bh185000 Additional fix for 1MB resp row
 * 16.00.00.01 07/14/2015 CLAC-33706 hs186016 Fix DBCHWL timer upon DBS crash
 * 17.00.00.00 11/13/2018 CLIWS-7163 hs186016 version and copyright update
 * 17.10.00.00 06/08/2020 CLIWS-7550 hs186016 Support TLS 1.2
 * 17.10.00.01 07/14/2020 CLIWS-7581 hs186016 Support HTTP/WebSocket
 * 17.10.00.02 08/13/2020 CLIWS-7604 hs186016 Support sslmode: allow and prefer
 * 17.10.00.03 01/26/2021 CLIWS-7635 rp255060 Connection string support
 * 17.10.00.04 06/03/2021 CLIWS-7751 hs186016 Support mainframe on z/OS
 * 17.20.00.00 01/01/2022 CLIWS-8198 hs186016 version and copyright update
 * 17.20.00.01 07/12/2022 CLIWS-8233 rp255060 OCSP support
 * 17.20.00.02 08/10/2022 CLIWS-8471 rp255060 Disable OCSP until ready for release
 * 17.20.00.03 12/01/2022 CLIWS-8614 rp255060 Support CRL on non-z/OS
 * 20.00.00.00 05/11/2023 CLIWS-8696 hs186016 version and copyright update
 * 20.00.00.01 06/27/2024 CLIWS-9273 rp255060 Add o_port to DBCAREA
 ******************************************************************************/

/***************************************************************/
/*                DBCAREA  EXTENSION  structure                */
/*                            for                              */
/*                      Parameterized SQL                      */
/***************************************************************/

/***************************************************************/
/* DBCAREA Extension structure                                 */
/*     DBCAREAX is an extension to the DBCAREA and is of       */
/*     variable length. A field (extension_pointer) in the     */
/*     DBCAREA points to the DBCAREAX. DBCAREAX contains 2     */
/*     logical sections: header & repeatable elements.         */
/*                                                             */
/*     Applications with 64bit addressing should use D8CAIRX   */
/*     and related structures(begining with d8).               */
/*                                                             */
/***************************************************************/

/* DR85431 --> */
#include "coptypes.h" /* DR68139 */
/* <-- DR85431 */

typedef struct D8CAIRX {             /* DR55267: 64 bit support*/
    char             d8xiId[4];      /* Eyecatcher, predefined */
                                     /* string "IRX8"          */
/* DR58369: 64 bit support on HPUX */
/* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    Byte             d8xiNxt4[4];    /* Reserved: must be zero */
#else
    struct D8CAIRX*  d8xiNext;       /* Next-extension pointer */
#endif
    UInt32           d8xiSize;       /* Length of extension    */
    Byte             d8xiLvl;        /* Area's level           */
    Byte             d8xiFil1[3];    /* Reserved: must be zero */
/* DR58369: 64 bit support on HPUX */
/* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    struct D8CAIRX*  d8xiNext;       /* Next-extension pointer */
#else
    Byte             d8xiNxt8[8];    /* Reserved: must be zero */
#endif
} D8CAIRX;
                                                   /*->DR61637 */
/***************************************************************/
/*  DR61637: Extension element                                 */
/*  Two new structures are defined for supporting APH.         */
/*  Use of structure elements D8XILMNT and D8XILPTR defined by */
/*  64-bit implementation is  deprecated.                      */
/*  The two new element structures can be used to delivar both */
/*  APH and Non APH parcels to CLI.                            */
/*  These are compatible for both 32 and 64 bit modes          */
/***************************************************************/

/***************************************************************/
/* D8XIELEM:  Header for the parcel element                    */
/* dbxieLen,  should be used to specify the total length of    */
/*            the element.                                     */
/*            Inline Method (i.e. d8xiepPt is null)            */
/*            d8xieLen = sizeof (D8XIELEM)  + sizeof(D8XIEP)   */
/*                            + sizeof (parcel data)           */
/*            Pointer Method (i.e. d8xiepPt is not null)       */
/*            d8xieLen = sizeof (D8XIELEM)  + sizeof(D8XIEP)   */
/***************************************************************/

typedef struct D8XIELEM {
    UInt16  d8xieLen;       /* Length of element               */
    UInt16  d8xieTyp;       /* Type of element                 */
                            /* 0 - Use of non APH              */
                            /* 1 - Use APH                     */
} D8XIELEM;

/***************************************************************/
/* D8XIEP:   Parcel element                                    */
/* d8xiepPt,                                                   */
/*           If it is zero or null, the parcel body data       */
/*           is contained within the element beginning after,  */
/*           and contiguous to, this element.                  */
/*           If not zero or null, the body data is addressed   */
/*           by it.                                            */
/*                                                             */
/*           If the length of the element would exceed 65535,  */
/*           then the pointer must be used to address the body */
/*           data.                                             */
/***************************************************************/

/* DR84603: Added pack(1) for Win-32 */

#if defined (_WIN64) || defined (WIN32)

#pragma pack(push,1)

#endif

typedef struct D8XIEP {
    UInt16    d8xiepF;       /* Parcel flavor                  */
    UInt16    d8xiepR1;      /* Reserved: must be zero         */
    UInt32    d8xiepLn;      /* Length of body                 */
                             /* if d8xiepPt NULL               */
                             /*  set d8xiepLn to 0             */
                             /* else                           */
                             /*  set d8xiepLn to body length   */
    Byte      d8xiepA[4];    /* Reserved: must be zero         */
#ifdef CLI_64BIT
    Byte      d8xiepP4[4];   /* Reserved: must be zero         */
#else
    char*   d8xiepPt ;       /* Pointer to body,if not inline  */
#endif
#ifdef CLI_64BIT
    char*    d8xiepPt;       /* Pointer to body,if not inline  */
#else
    Byte    d8xiepP8[8];     /* Reserved: must be zero         */
#endif
    Byte   d8xiepL8[8];      /* Reserved: must be zero         */
} D8XIEP;


/* DR84603: pop packing for Win-32 */
#if defined (_WIN64) || defined (WIN32)

#pragma pack(pop)

#endif

/* Value for d8xiId (EYE-CATCHER)                              */
static char D8XIIIRX[4] = {'I','R','X','8'};

/* Values for d8xiLvl                                          */
#define D8XIL64  ((Byte) 0)    /*64bit                         */
#define D8XILERS ((Byte) 1)    /*Element restructure           */

/* Value for d8xieTyp                                          */
#define D8XIETP ((UInt16) 1)   /* Parcel element               */

/* USE OF THE BELOW DEFINED EXTENSION ELEMENT STRUCTURES IS    */
/* DEPRECATED                                                  */
                                                   /* DR61637<-*/

/**********************************************************************/
/* Extension element                                                  */
/*   One or more elements follow the header. If d8xilTyp is less then */
/*   4096, the element describes a parcel while greater values might  */
/*   be used for other types of elements, none are currently defined. */
/*                                                                    */
/* Extension parcel-element pointer                                   */
/*    If high order bit is not set, the element is a parcel  whose    */
/*    header consists of dbxilTyp and dbxilLen and whose data         */
/*    immediately follows. If D8XILTP is set, the element describes   */
/*    the length of and pointer to the parcel data whose flavor is    */
/*    specified in d8xilTyp.                                          */
/**********************************************************************/

typedef struct D8XILMNT {            /* DR55267: 64 bit support */
    UInt16   d8xilTyp;               /* Type of element         */
    UInt16   d8xilLen;               /* Length of element       */
} D8XILMNT;

typedef struct D8XILPTR {             /* DR55267: 64 bit support */
    Byte     d8xilpF2[2];             /* Reserved: must be zero  */
 /* DR58369: 64 bit support on HPUX */
 /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    Byte     d8xilpP4[4];             /* Reserved: must be zero  */
#else
    char     d8xilpPt[4];             /* Pointer to parcel-data  */
#endif
    UInt32   d8xilpF3[4];             /* Reserved: must be zero  */
    UInt32   d8xilpLn;                /* Length of parcel-data   */
    Byte     d8xilpA[4];              /* Reserved: must be zero  */
 /* DR58369: 64 bit support on HPUX */
 /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char     d8xilpPt[8];             /* Pointer to parcel-data  */
#else
    Byte     d8xilpP8[8];             /* Reserved: must be zero  */
#endif
} D8XILPTR;

 /* DR58369: 64 bit support on HPUX */
 /* DR58915: Native 64bit support for AIX */
#ifndef CLI_64BIT
typedef struct DBCAREAX
{
    char               x_eyecatcher[4]; /* predefined string  'DBCX'    */
    struct DBCAREAX*   x_next_ptr;      /* to build a list if necessary */
    short              x_total_length;  /* header & sum of all elements */
    short              x_reserved;      /* future use                   */
    char               x_elements;      /* start of elements            */
} DBCAREAX;

/*****************************************************************/
/*  The header information is followed by at least one variable  */
/*  DBCAREA EXTENSION ELEMENT.                                   */
/*                                                               */
/*  There are two different types of elements: PARCEL DATA       */
/*  ELEMENT & PARCEL POINTER ELEMENT. Parcel Data Element        */
/*  consists of Element Type Code, Length and Variable Length    */
/*  Data. The high order bit of Type Code is not set.            */
/*                                                               */
/*  Parcel Pointer Element consists of Element Type Code, Element*/
/*  Length, Parcel Data Length & Parcel Data Pointer. The high   */
/*  order bit of Type Code is set for Parcel Pointer Element.    */
/*  The value of low order 15bits of Type Code must be less      */
/*  than 4096.                                                   */
/*****************************************************************/

/*****************************************************************/
/*           DBCAREA EXTENSION DATA ELEMENT structure            */
/*****************************************************************/

typedef struct  x_element
{
    short      x_elm_type;           /* pcl body or pcl ptr type*/
    short      x_elm_length;         /* total length of element */
    char       x_elm_data;           /* first byte of pclbody   */
} x_element;


typedef struct  xp_element
{
    short      x_elm_type;           /* pcl body or pcl ptr type*/
    short      x_elm_length;         /* total length of element */
    short      x_pelm_len;           /* length of parcel data   */
    char       x_pelm_addr[4];       /* address to parcel data  */
                                     /*   char array to avoid 2 */
                                     /*   compiler padding bytes*/
} xp_element;

#endif /* CLI_64BIT */

/* CLAC-30554 --> */
/***************************************************************/
/*  Smart LOB (SLOB) Support                                   */
/***************************************************************/
typedef struct SLOB_data_struct
{
    UInt16     version;               /* version of the implementation      */
    UInt16     unused;                /* always 0 (zero)                    */
    UInt32     max_num_SLOBs_in_row;  /* Maximum # of SLOBs in a row        */
#ifndef CLI_64BIT
    char       SLOB_data_reserved[4];
#endif
    char       ***SLOB_data;          /* 2-D array of pointers to SLOB data */
#ifndef CLI_64BIT
    char       SLOB_ParameterOrderNo_reserved[4];
#endif
    UInt32     *SLOB_ParameterOrderNo;/* ParameterOrderNo of SLOBs          */
} SLOB_data_struct;
/* <-- CLAC-30554 */


/***************************************************************/
/*  Main DBCAREA structure                                     */
/***************************************************************/

typedef struct DBCAREA
{

/***************************************************************/
/*  Header Information                                         */
/***************************************************************/

    char       eyecatcher[8];
    Int32      total_len;

/***************************************************************/
/*  General inputs                                             */
/***************************************************************/
    Int32       func;             /* requested function        */
    Int32       i_sess_id;        /* connection id             */
    Int32       i_req_id;         /* request number            */
    Int32       req_buf_len;      /* desired length of req buf */
    Int32       resp_buf_len;     /* desired length of rsp buf */
    Int32       max_num_sess;     /* desired mas sessions      */
    Int32       token;            /* use request token         */

/***************************************************************/
/*  Values for Function Field                                  */
/***************************************************************/

#define         DBFCON   1        /* connect a DBC session     */
#define         DBFDSC   2        /* LOFOFF (disconnect) )     */
#define         DBFRSUP  3        /* perform runstartup        */
#define         DBFIRQ   4        /* initiate request          */
#define         DBFFET   5        /* fetch response data       */
#define         DBFREW   6        /* rewind & fetch            */
#define         DBFABT   7        /* abort last request        */
#define         DBFERQ   8        /* end last request          */
#define         DBFPRG   9        /* directed request          */
/* Added 2PC Function Codes - H0_01, JAE                       */
#define         DBF2PC  10        /* Added for 2PC, JAE        */
#define         DBFIWPF 11        /* Added for 2PC, JAE        */
#define         DBFRECV 12        /* Added for 2PC, JAE        */
#define         DBFALSC 13        /* Added for 2PC, JAE        */
#define         DBFCMD  14        /* Added for 2PC, JAE        */
#define         DBFCRQ  15        /* Added for LOB DR57583     */

#define         DBFMAX   DBFCRQ   /* DAA 87MAR12 DR9522        */
                                  /* Updated DBFMAX to DBFCMD  */
                                  /* H0_01, JAE                */

/***************************************************************/
/*  General outputs                                            */
/***************************************************************/
    Int32       return_cd;
    Int32       o_sess_id;
    Int32       o_req_id;
    char        o_dbcpath[8];
    Int32       o_dbc_sess_id;
    short       o_host_id;
    char        sess_status;
    char        fill_1;
    Int32       tdp_req_no;
    Int32       cur_req_buf_len;
    Int32       cur_resp_buf_len;

/***************************************************************/
/*  Function Argument and Information Structures               */
/*  The following functions use general input/output           */
/*  arguments:                                                 */
/*              DBFDSC                                         */
/*              DBFRSUP                                        */
/*              DBFABT                                         */
/*              DBFERQ                                         */
/***************************************************************/

/***************************************************************/
/*  The connect function sub structures.                       */
/***************************************************************/

    char      i_dbcpath[8];
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char      logon_ptr_4[4];    /* Reserved */
#else
    char      *logon_ptr;        /* For 32 bit mode */
#endif
    Int32     logon_len;
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char      run_ptr_4[4];  /* Reserved */
#else
    char      *run_ptr;      /* For 32 bit mode */
#endif
    Int32     run_len;

/***************************************************************/
/*  The IRQ function sub structures.                           */
/***************************************************************/

    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char      req_ptr_4[4];      /* Reserved */
#else
    char      *req_ptr;
#endif                    /* For 32 bit mode */
    Int32     req_len;
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char      using_data_ptr_4[4];   /* Reserved */
#else
    char      *using_data_ptr;   /* For 32 bit mode */
#endif
    Int32     using_data_len;        /* DR68838 */
    short     msgclass;       /*  DirectReq DR18467            */
    short     msgkind;        /*        .                      */
    char      mailbox[6];     /*        .                      */
    short     reserved;
    Int32     open_reqs;

/***************************************************************/
/*  The Fetch function sub structures.                         */
/***************************************************************/

   /************************************************************/
   /*  Input or output arguments work as  mode operations:     */
   /*                                                          */
   /*     Move mode   - Application inputs address of and max  */
   /*                   length of return area. CLI outputs data*/
   /*                   and actual length of returned data.    */
   /*     Locate mode -                                        */
   /*     Parcel mode - CLI outputs address and length & flavor*/
   /*                   for this parcel                        */
   /*     Buffer mode - CLI outputs address and length & flavor*/
   /*                   for first parcel in response buffer    */
   /************************************************************/

    Int32     fet_max_data_len;
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char      fet_data_ptr_4[4];         /* Reserved */
#else
    char      *fet_data_ptr;             /* For 32 bit mode */
#endif
    Int32     fet_ret_data_len;          /* DR11889           */
    Int32     fet_parcel_flavor;
    unsigned char fet_error_ind;         /* H0_07             */
    char       fill_2[3];

/***************************************************************/
/*  Time Stamps from various phases of operation.:for VM & VMS */
/***************************************************************/
/* changed timestamp fields to be unsigned                     */
                                        /* -> DR58353          */

    char       hsisvc_time[8];          /* unused in NW-CLI    */
    UInt32     tdpwait_time;            /* when req. is sent   */
                                        /* to MTDP             */
    UInt32     tdpdbo_time;             /* unused in NW-CLI    */
    UInt32     tdpdbi_time;             /* unused in NW-CLI    */
    UInt32     tdpxmm_time;             /* unused in NW-CLI    */
    UInt32     srbsched_time;           /* First response      */
                                        /* recieved by CLI     */

                                        /* DR 58353     <-     */

/***************************************************************/
/* Pointer to International Character Set Code or Name DCR4029 */
/***************************************************************/
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char       inter_ptr_4[4];   /* Reserved */
#else
    char       *inter_ptr;     /* DCR4029 */ /* For 32 bit mode */
#endif

/***************************************************************/
/*  Time Stamps for UNIX, VAX/VMS, and MS/DOS                  */
/***************************************************************/

    time_stamp        mtdp_sent;         /* DR11889, DR68084   */
    time_stamp        mtdp_received;     /* DR11889, DR68084   */
    char              fill_3[16];        /* DCR4029            */
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char             extension_pointer_4[4];   /* Reserved */
#else
    void             *extension_pointer; /* DCR5239 Param. SQL */
#endif


/***************************************************************/
/*  Options Flags.  Character flags are used for portability.  */
/*  Legal values are 'Y' or 'N' unless specified otherwise.    */
/***************************************************************/

    char       change_opts;
    char       resp_mode;          /* Record 'R', Field 'F', or */
                                   /* Indicator 'I' mode for    */
                                   /* returned data, 'M' for    */
                                   /* multipart indicator mode  */
    char       use_presence_bits;
    char       keep_resp;

/* H0_04 delay options for AP Reset */
#define wait_across_delay wait_across_crash
#define tell_about_delay tell_about_crash

    char       wait_across_crash;
    char       tell_about_crash;
    char       connect_wait;        /* H0_05, DR32387-ALB    */
    char       loc_mode;
    char       var_len_req;
    char       var_len_fetch;
    char       save_resp_buf;
    char       two_resp_bufs;
    char       ret_time;
    char       parcel_mode;
    char       wait_for_resp;
    char       req_proc_opt;        /* prepare 'P', execute 'E',*/
                                    /* Parameterized SQL 'S'    */
    char       msg_security;        /* DR11889                  */

                                    /* Following three options  */
                                    /* added for DCR 4029       */
    char       charset_type;        /* char set code or name    */
                                    /* 'C'code, 'N'name provided*/
    char       connect_type;        /* Host Run or Connect Logon*/
                                    /* 'R' run logon,           */
                                    /* 'C' connect logon(used on*/
                                    /*                   host)  */
    char       request_mode;        /* options for req handling */
                                    /* 'P' parcelmode,          */
                                    /* 'B' buffermode,          */
                                    /* 'D' descriptor list      */
/* Added 2PC Options - H0_01, JAE                              */
    char       sess_2pc_mode;       /* 'Y' or 'N' for 2 PC Mode */
    char       init_with_2pc;       /* 'T','N','V' for DBFIWPF  */
                                    /* ('V' is mainframe only)  */
#define iwpf_function init_with_2pc
                                    /* H3_02                    */
                                    /* H0_06,  NDM Support      */
    char       tx_semantics;        /* Transaction Semantics    */
                                    /* 'D' server default       */
                                    /* 'T' Teradata DBS         */
                                    /* 'A' ANSI                 */
    char       lang_conformance;    /* Language Conformance     */
                                    /* 'N' None                 */
                                    /* '2' FIPS127-2            */
                                    /* '3' FIPS127-3            */

/***************************************************************/
/*  Optional Message.                                          */
/***************************************************************/

    char       fill_4[2];
    short      msg_len;
    char       msg_text[76];
    Int32      route_desc_codes;
    char       fill_5[16];         /* DR36456 */
    char       fill_6[2];          /* DR36456, DR40417 */
    char       date_form;          /* DR40417 */
    char       maximum_parcel;     /* DR36456 */
    char       dbcniLid[2];        /* DR52059, Language ID     */
                                   /* EN - English             */
                                   /* JA - Japanese            */

    char       fill_8[2];          /* DR47056, On Mainframes   */
    char       dbriSeg;            /*  TDSP Field, DR48735     */
                                   /* 'N' Default              */
                                   /* 'F' First segment        */
                                   /* 'I' Intermediate segment */
                                   /* 'L' Last segment         */
                                   /* 'C' Cancel Request       */

                                   /* DR 57583 (LOB)...        */
        char       return_object;  /* Return Objects as...     */
                                   /* 'D' Data                 */
                                   /* 'T' TX-related locators  */
                                   /* 'S' Spool-related locator*/
    #define dbriROb return_object

                                   /* DR 57583 (LOB)...        */
    char       continuation_code;  /* Continue data            */
                                   /* 'F' First continuation   */
                                   /* 'I' Intermediate cont.   */
                                   /* 'L' Last/only cont.      */
                                   /* 'C' Cancel continuation  */
    #define dbniCnt continuation_code

    char      data_encryption;     /* DR 66712...              */
                                   /* 'Y' Use encryption       */
                                   /* 'N' Use clear text       */
    #define dbriDEn data_encryption

    char      fill_8a;             /* reserved  - DR68835 */

    char statement_status;         /* DR 58352 Statement-Status    */
    #define       dbcniSS statement_status
                                   /* 'O' Original                 */
                                   /* 'E' Enhanced                 */
                                   /* 'D' Description, CLAC-28857  */

    char       continued_char_state;  /*continued-characters-state*/
                              /* 'L'Shift State across messages */
                              /* 'U'Unlock Shift state for each */
                              /*    message, if Multiindicator  */
                              /* parcel spans more than 1 message*/
    #define     dbcriCCS continued_char_state


    char       consider_APH_resps; /* DR68835, APH Responses */
                                /* 'Y' OK to receive APH responses */
                                /* 'N' Don't send APH responses */
    #define    dbriAPH consider_APH_resps

    char       return_statement_info; /* DR98026, StatementInfo */
    char       return_identity_data;  /* DR98025, AGKR_option   */
/*  char       fill_9[1];          DR98025: used all fill area  */
                                /* DR98026: changed from 2 to 1 */
                                /* DR68835: changed from 3 to 2 */
                                /* DR69228: changed from 6 to 3 */
                                /* DR68835: changed from 6 to 5 */
                                /* DR66712: changed from 7 to 6 */
                                   /* DR57583 */
                                   /* DR58353: changed fill9   */
                                   /* from 3 to 21             */

    UInt16     positioning_stmt_number; /* statement number         */

#define  dbfiPSNm positioning_stmt_number

/* DR83781 & DR89199 */
#ifdef WIN32
   unsigned __int64 positioning_value;   /* row position or BLOB/CLOB offset */
#define dbfiPVal positioning_value
#else
#ifdef   _NO_LONGLONG
   unsigned long         positioning_value_hw;
   unsigned long         positioning_value_lw;
#else
   unsigned long long positioning_value;   /* row position or LOB/CLOB offset */
#define dbfiPVal positioning_value
#endif
#endif

    UInt16     positioning_action; /* action:                       */
                                   /* 0 - position to next parcel   */
                                   /* 1 - position to specified row */
                                   /* 2 - position to BLOB offset   */
                                   /* 3 - position to CLOB ofset    */

#define dbfiPAct positioning_action

#define   DBFIPANP    0		   /*   -Next parcel               */
#define   DBFIPASR    1        /*   -Statement m row n         */
#define   DBFIPASB    2		   /*   -Stmt m byte-offset n      */
#define   DBFIPASC    3        /*   -Stmt m char-offset n      */
#define   DBFIPAMX    3        /*    (Maximum value)           */

/* <--- DR60695 */


    Int16      dbciTSP;            /* DR58353: Field for app.  */
                                   /* to mention time. precis. */
    char       dbclevel;           /* DR55267: binary 1,if enlarged */
                                   /* DR58353: changed offset   */
                                   /* of dbclevel from 352-372  */
/* CLAC-30554 --> */
#define   DBCLEXP1    1        /* First expansion  */
#define   DBCLEXP2    2        /* Second expansion */
#define   DBCLVLC     2        /* Current level    */
/* <-- CLAC-30554 */
    char       dbcoMsgC;           /* Unused                    */
    Int16      dbcoMsgR;           /* DR52059, Msg Return code  */
                                   /* the error message returns */
                                   /* 0 - no error occured      */
                                   /* 1 - if msg is truncated   */
    Int16      dbcoMsgL;           /* DR52059, MessageLength    */
                                   /* retuned in Area provided  */
    Int16      dbciMsgM;           /* DR52059, Msg Area Length  */
                                   /* provided by the app.      */
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char       dbciMsgP4[4];    /* Reserved */
#else
    char       *dbciMsgP;       /* DR52059, Pointer to msg. area */
                                /*          provided by app.     */
#endif

/**********************************************************************/
/* Native 64 bit support for Cliv2                                    */
/**********************************************************************/
    /* DR55267: Native 64bit support         */
    /* DR58369: Native 64bit support on HPUX */
    /* DR58915: Native 64bit support for AIX */

#ifdef CLI_64BIT
    char      req_buf_len_4[8];             /* Reserved */
    char      resp_buf_len_4[8];            /* Reserved */
    char      curr_req_buf_len_4[8];        /* Reserved */
    char      cur_resp_buf_len_4[8];        /* Reserved */
    char      *logon_ptr;                   /* Level 1  */
    char      *run_ptr;                     /* Level 1  */
    char      req_len_4[8];                 /* Reserved */
    char      *req_ptr;                     /* Level 1  */
    char      fill_11[4];                   /* Reserved */
    char      fill_12[4];                   /* Reserved */
    char      *using_data_ptr;              /* Level 1  */
    char      fill_12a[8];                  /* DR89408 */ /* DR91963 */
    char      fet_max_data_len_4[8];        /* Reserved */
    char      fill_13[4];                   /* Reserved */
    char      fill_14[4];                   /* Reserved */
    char      *fet_data_ptr;                /* Level 1  */
    char      fet_ret_data_len_4[8];        /* Reserved */
    char      *inter_ptr;                   /* Level 1  */
    void      *extension_pointer;           /* Level 1  */
    char      *dbciMsgP;            /* DR52059, Pointer to Msg area    */
                                    /*          provided by app.       */
#else
    char      req_buf_len_8[8];     /* Reserved */
    char      resp_buf_len_8[8];    /* Reserved */
    char      curr_req_buf_len_8[8];/* Reserved */
    char      cur_resp_buf_len_8[8];/* Reserved */
    char      logon_ptr_8[8];       /* Reserved */
    char      run_ptr_8[8];         /* Reserved */
    char      req_len_8[8];         /* Reserved */
    char      req_ptr_8[8];         /* Reserved */
    char      fill_11[4];           /* Reserved */
    char      fill_12[4];           /* Reserved */
    char      using_data_ptr_8[8];  /* Reserved */
    char      fill_12a[8];          /* DR89408  */
    char      fet_max_data_len_8[8];/* Reserved */
    char      fill_13[4];           /* Reserved */
    char      fill_14[4];           /* Reserved */
    char      fet_data_ptr_8[8];    /* Reserved */
    char      fet_ret_data_len_8[8];/* Reserved */
    char      inter_ptr_8[8];       /* Reserved */
    char      extension_pointer_8[8];/* Reserved */
    char      dbciMsgP8[8];          /* Reserved */
#endif
    char      fill_15[4];           /* DR58353: added new filler       */
    char      fill_16;              /* DR58353: added new filler       */
    char      dbcoTSS1;             /* DR58353: Track overflow of Time1*/
    char      dbcoTSS2;             /* DR58353: Unsed in NW-CLI        */
    char      dbcoTSS3;             /* DR58353: Unsed in NW-CLI        */
    char      dbcoTSS4;             /* DR58353: Unsed in NW-CLI        */
    char      dbcoTSS5;             /* DR58353: Track overflow of Time5*/
    char      exempt_sess_from_DBCHWAT; /* DR94622: dual-component     */
    char      create_default_connection;/* DR101287: default connection*/
    UInt32    using_data_count;     /* DR68838: Data array count       */
#define dbriUDC using_data_count    /* DR68838: MF compatibility       */
    char      logmech_name[8];      /* DR68842: Security support    -> */
#ifndef CLI_64BIT
    char      logmech_data_ptr_reserved[4]; /* Reserved */
#endif
    char     *logmech_data_ptr;
    UInt32    logmech_data_len;
#define MAX_LOGMECH_DATA_LEN 65535  /* <- DR68842: Security support    */
/* DR89408 --> */
    char      mechdata_Unicode_set; /* DR90995: Unicode credentials    */
    char      dynamic_result_sets_allowed; /* DR102528, Dynamic Result */
    unsigned char SP_return_result; /* DR102528, SP return result      */
    char      send_delegated_credentials; /* DR101663                  */
#ifndef CLI_64BIT
    char     using_data_ptr_array_reserved[4];
#endif
    char    *using_data_ptr_array;
#ifndef CLI_64BIT
    char     using_data_len_array_reserved[4];
#endif
    int     *using_data_len_array;
    UInt16   max_decimal_returned;  /* DR98024, MaxDecimalPrecision    */
/* DR108909 --> */
    char     transformsOff;         /* DR123516, offset 586            */
    char     periodAsStruct;        /* DR123516, offset 587            */
    unsigned int workload_len;
#ifndef CLI_64BIT
    char     workload_ptr_reserved[4];
#endif
    char    *workload_ptr;          /* DR110710, use low-order byte    */
/* <-- DR108909 */
    short    logon_timeout;         /* DR118581 */
    UInt16   wait_time;             /* CLAC-32067, CLAC-33706 */
    char     fill_17[8];            /* DR122304, CLAC-32067 offset 604 to 611*/
    char     trustedRequest;        /* DR122304, offset 612            */
    char     columnInfo;            /* DR116354, offset 613            */
    char     utilityWorkload;       /* DR126915, offset 614            */
    char     multiStatementErrors;  /* DR121202, offset 615            */
    char     fill_18[4];            /* DR145358, offset 616 to 619     */
    char     automaticRedrive;      /* DR145358, offset 620            */
    char     extendedLoadUsage;     /* CLAC-29283, offset 621          */
    char     fill_19[14];           /* CLAC-29283, offset from 15 to 14*/
    char     tasmFastFailReq;       /* CLAC-30478, offset 636          */
    char     xmlResponseFormat;     /* CLAC-28875, offset 637          */
    char     arrayTransformsOff;    /* DR145224, offset 638            */
    char     fill_20[1];            /* DR145224, offset 639            */
                                    /* DR145224, offset from 19 to 17  */
                                    /* DR145358, change from 24 to 19  */
                                    /* DR121202, change from 25 to 24  */
                                    /* DR126915, change from 26 to 25  */
                                    /* DR116354, change from 40 to 26  */
                                    /* DR98024, change from 56 to 54   */
                                    /* DR108909, change from 54 to 40  */
                                    /* DR68838: Reserved               */
/* <-- DR89408 */


/* ===============>>> TO USE MEMBERS BELOW THIS LINE <<<==================== */
/*                                                                           */
/* Struct members below this point must be wrapped in the following if       */
/* condition statement before use:                                           */
/*                                                                           */
/*   if (DBCLEVEL >= DBCLEXP2)                                               */
/*   {                                                                       */
/*       ...                                                                 */
/*   }                                                                       */
/*                                                                           */
/* This will allow older apps built with a dbcarea size of 640 bytes or less */
/* to function with a new CLI.                                               */
/*                                                                           */
/* ========================================================================= */

#ifndef CLI_64BIT
    char     using_SLOB_data_struct_reserved[4];  /* Reserved          */
#endif
    struct SLOB_data_struct
            *using_SLOB_data_struct;/* CLAC-30554, use low-order byte  */
    UInt64   MaxSingleLOBBytes;     /* CLAC-31559, offset 648          */
    UInt64   MaxTotalLOBBytesPerRow;/* CLAC-31559, offset 656          */
    char     fill_21[16];           /* offset 664 Reserved for mainframe*/
#ifndef CLI_64BIT
    char     connect_ptr_reserved[4];   /* Reserved                    */
#endif
    char    *connect_ptr;           /* CLIWS-7635, offset 680 (0x02A8) */
    UInt32   connect_len;           /* CLIWS-7635, offset 688 (0x02B0) */
    char     sslmode;               /* CLIWS-7550, offset 692 (0x02B4) */
    char     o_sslmode;             /* CLIWS-7604, offset 693 (0x02B5) */
    UInt16   o_port;                /* CLIWS-9273, offset 694 (0x02B6) */

/* CLIWS-7581 --> */
#define CLI_SSLMODE_ALLOW           'A'
#define CLI_SSLMODE_DISABLE         'D'
#define CLI_SSLMODE_PREFER          'P'
#define CLI_SSLMODE_REQUIRE         'R'
#define CLI_SSLMODE_VERIFY_CA       'C'
#define CLI_SSLMODE_VERIFY_FULL     'F'
/* <-- CLIWS-7581 */

    char     fill_22[326];          /* (reserved)  offset 696 (0x02B8) */
    char     largeRow;              /* CLAC-29115, offset 1022(0x03FE) */
    char     fill_23[1];            /* CLAC-29115, offset 1023(0x03FF) */
} DBCAREA;                          /* H0_07 */
/* When you add a new field, BE SURE TO UPDATE THE SIZE OF 'fill_22' properly */

/***************************************************************/
/*  END of MAIN DBCAREA  structure                             */
/***************************************************************/

#ifdef IBMASM_NAMES                /* Begin of H0_07 merge     */
/***************************************************************/
/*  Name definitions to match DBCAREA macro                    */
/***************************************************************/
#pragma options copts(dollars)
#define DBCAID eyecatcher
#define DBCSIZE total_len
#define DBCAFUNC func
#define DBCICONN i_sess_id
#define DBCICCMD 0xFFFFFFFF
#define DBCIREQN i_req_id
#define DBCIRBRL req_buf_len
#define DBCIFBRL resp_buf_len
#define DBCISMAX max_num_sess
#define DBCIURQN token
#define         DBF$CON  DBFCON
#define         DBF$DSC  DBFDSC
#define         DBF$RSUP DBFRSUP
#define         DBF$IRQ  DBFIRQ
#define         DBF$FET  DBFFET
#define         DBF$REW  DBFREW
#define         DBF$ABT  DBFABT
#define         DBF$ERQ  DBFERQ
#define         DBF$PRG  DBFPRG
#define         DBF$IWPF DBFIWPF
#define         DBF$CMD  DBFCMD
#define         DBF$CRQ  DBFCRQ
#define         DBF$MAX  DBF$CRQ
#define DBCRCODE return_cd
#define DBCOCONN o_sess_id
#define DBCOREQN o_req_id
#define DBCOSID  o_dbc_path
#define DBCOSIT  DBCOSID
#define DBCOSISN o_dbc_sess_id
#define DBCOSILH o_host_id
#define DBCOFLGS sess_status
#define DBCOFLG1 DBCOFLGS
#define DBCOFLG2 fill_2
#define DBF1$POL 0x80
#define DBF1$TRX 0x40
#define DBF1$CLR 0x20
#define DBF1$ID  0x210
#define DBCOARQN tdp_req_no
#define DBCORBLA cur_req_buf_len
#define DBCOFBLA cur_resp_buf_len
#define DBCNITDP i_dbcpath
#define DBCNISSN i_dbcpath
#define DBCNIVMN i_dbcpath
#define DBCNILSP logon_ptr
#define DBCNILSL logon_len
#define DBCNIRSP run_ptr
#define DBCNIRSL run_len
#define DBRIRQP req_ptr
#define DBRIRQL req_len
#define DBRIUDP using_data_ptr
#define DBRIUDPL using_data_len
#define DBROREQC open_reqs
#define DBFIFDL fet_max_data_len
#define DBFXFDP fet_data_ptr
#define DBFOFDL fet_ret_data_len
#define DBFOPFLV fet_parcel_flavor
#define DBFOEFLG fet_error_ind
#define DBCTSTMP hsisvc_time
#define DBCTIME1 tdpwait_time
#define DBCTIME2 tdpdbo_time
#define DBCTIME3 tdpdbi_time
#define DBCTIME4 tdpxmm_time
#define DBCTIME5 srbsched_time
#define DBCCSNP inter_ptr
#define DBCCSCP inter_ptr
#define DBCAXP extension_pointer
#define DBOSETO change_opts
#define DBOSE$Y 'Y'
#define DBOSE$N 'N'
#define DBORMOD resp_mode
#define DBORM$R 'R'
#define DBORM$F 'F'
#define DBORM$I 'I'
#define DBOIDTA use_presence_bits
#define DBOID$Y 'Y'
#define DBOID$N 'N'
#define DBOKRSP keep_resp
#define DBOKR$Y 'Y'
#define DBOKR$N 'N'
#define DBOKR$P 'P'					/* DR60695 */
#define DBOCRSW wait_across_crash
#define DBOCW$Y 'Y'
#define DBOCW$N 'N'
#define DBOCRTL tell_about_crash
#define DBOCL$Y 'Y'
#define DBOCL$N 'N'
#define DBORSV2 connect_wait         /* for W/S only */
#define DBOFLOC loc_mode
#define DBOFLO$Y 'Y'
#define DBOFLO$N 'N'
#define DBORVAR var_len_req
#define DBORVA$Y 'Y'
#define DBORVA$N 'N'
#define DBOFVAR var_len_fetch
#define DBOFVA$Y 'Y'
#define DBOFVA$N 'N'
#define DBOFSVB save_resp_buf
#define DBOFSB$Y 'Y'
#define DBOFSB$N 'N'
#define DBO2FBU two_resp_bufs
#define DBO2FB$Y 'Y'
#define DBO2FB$N 'N'
#define DBORTS  ret_time
#define DBORT$Y  'Y'
#define DBORT$N  'N'
#define DBOBTPM parcel_mode
#define DBOBT$Y  'Y'
#define DBOBT$N  'N'
#define DBOBSYW  wait_for_resp
#define DBOBS$Y  'Y'
#define DBOBS$N  'N'
#define DBOFUNT req_proc_opt
#define DBOFU$E  'E'
#define DBOFU$P  'P'
#define DBOFU$S  'S'
#define DBORSV1  msg_security
#define DBOSCS charset_type
#define DBOSC$C 'C'
#define DBOSC$N 'N'
#define DBOCTYPE connect_type
#define DBOCT$R 'R'
#define DBOCT$C 'C'
#define DBOQMOD request_mode
#define DBOQM$P 'P'
#define DBOQM$B 'B'
#define two_phase_commit sess_2pc_mode
#define DBO2PC sess_2pc_mode
#define DBO2P$Y 'Y'
#define DBO2P$N 'N'
#define DBRIPF init_with_2pc
#define DBRIP$N 'N'
#define DBRIP$V 'V'
#define DBRIP$T 'T'
                                                    /* H3_02 - begin NDM */
#define DBCNITSM tx_semantics
#define DBCNITD 'D'
#define DBCNITT 'T'
#define DBCNITA 'A'
#define DBCNILCS lang_conformance
#define DBCNILN 'N'
#define DBCNILF2 '2'
#define DBCNILF3 '3'
                                                    /* H3_02 - end NDM   */
#define DBCMSGA fill_8
#define DBCMSGL msg_len
#define DBCMSG msg_text
#define DBCMSGRD route_desc_codes

#define DBRIROB return_object
#define DBRIROBD 'D'
#define DBRIROBT 'T'
#define DBRIROBS 'S'

#define DBNICNT continuation_code
#define DBNICNTF 'F'
#define DBNICNTI 'I'
#define DBNICNTL 'L'
#define DBNICNTC 'C'

#endif

/* DR102528 --> */
#define DBC_RetnRsltRqstr        1    /* -Requester           */
#define DBC_RetnRsltAppl         2    /* -Application         */
#define DBC_RetnRsltCaller       3    /* -Caller              */
#define DBC_RetnRsltRqstrAppl    4    /* -Requester & Appl    */
#define DBC_RetnRsltRqstrCaller  5    /* -Requester & Caller  */
/* <-- DR102528 */

/*                                  DR 52050-> */
#ifndef cli_session_defined
#define cli_session_defined
typedef struct cli_session {
    Int32  session;
    Int32  reserved1;
    Int32  reserved2;
} SESSIONS, *SESSIONS_PTR;
#endif

#define CLI_WIN_TIMEOUT  1  /* Timeout event */
struct cli_win_event {
    Int32 kind;  /* event type */
    void *data;  /* pointer to user supplied data */
};

#define INSERT_EVENT 1
#define DELETE_EVENT 2
/*                                  ->DR 52050 */

#ifndef NOPROT /* DR21506 */

#if (defined(__MVS__) && !defined(__cplusplus)) /* DR87586 */
#ifndef CLI_MF_GTW  /* CLIWS-7751 */
#pragma linkage(DBCHATTN,OS)
#pragma linkage(DBCHBRK,OS)
#pragma linkage(DBCHCL,OS)
#pragma linkage(DBCHCLN,OS)
#pragma linkage(DBCHERR,OS)
#pragma linkage(DBCHFER,OS)
#pragma linkage(DBCHFERL,OS)
#pragma linkage(DBCHINI,OS)
#pragma linkage(DBCHPEC,OS)
#pragma linkage(DBCHREL,OS)
#pragma linkage(DBCHSAD,OS)
#pragma linkage(DBCHUEC,OS)
#pragma linkage(DBCHUE,OS)
#pragma linkage(DBCHWAT,OS)
#pragma linkage(DBCHWL,OS)
#endif  /* CLI_MF_GTW */ /* CLIWS-7751 */
#endif

#ifndef EXPENTRY
#ifdef WIN32
#define EXPENTRY __stdcall /* DR68139, DR68140, DR86451 */
#else  /* WIN32 */
#define EXPENTRY
#endif /* WIN32 */
#endif /* ifndef EXPENTRY */

/* DR85431 --> */
#ifdef __cplusplus
#if defined(__MVS__) && !defined(CLI_MF_GTW)
extern "OS" {
#else
extern "C" {
#endif
#endif
Int32  EXPENTRY DBCHATTN(Int32 *);
Int32  EXPENTRY DBCHBRK (void);
Int32  EXPENTRY DBCHCL  (Int32 *, char *, struct DBCAREA *);
Int32  EXPENTRY DBCHCLN (Int32 *, char *);
Int32  EXPENTRY DBCHERO (Int32 *, char *, struct DBCAREA *);
Int32  EXPENTRY DBCHERR (Int32, struct DBCAREA *, errpt_t);
Int32  EXPENTRY DBCHFER (Int32, register errpt_t, char *);
Int32  EXPENTRY DBCHFERL(Int32, register errpt_t, char *, UInt16, UInt16*, Int16*);
Int32  EXPENTRY DBCHINI (Int32 *, char *, struct DBCAREA *);
Int32  EXPENTRY DBCHPEC (Int32 *, Int32 *);
Int32  EXPENTRY DBCHREL (Int32 *, char *, Int32, char *, char *);
void   EXPENTRY DBCHSAD (Int32 *, void **, void *);
Int32  EXPENTRY DBCHUEC (Int32 *, char *, Int32 *); /* <-DR59337 */
Int32  EXPENTRY DBCHUE  (Int32 *, char **, struct cli_win_event *, Int32);
Int32  EXPENTRY DBCHWAT (Int32 *, char *, Int32 *, Int32 *);
Int32  EXPENTRY DBCHWL  (Int32 *, char **, SESSIONS_PTR, Int32 *, Int32 *);

char * EXPENTRY OsGetCOPCLIVersion(void);

#ifdef __cplusplus
}
#endif

/* CLIWS-7751 --> */
#ifdef CLI_MF_GTW
#pragma export(DBCHATTN)
#pragma export(DBCHBRK)
#pragma export(DBCHCL)
#pragma export(DBCHCLN)
#pragma export(DBCHERO)
#pragma export(DBCHERR)
#pragma export(DBCHFER)
#pragma export(DBCHFERL)
#pragma export(DBCHINI)
#pragma export(DBCHPEC)
#pragma export(DBCHREL)
#pragma export(DBCHSAD)
#pragma export(DBCHUEC)
#pragma export(DBCHUE)
#pragma export(DBCHWAT)
#pragma export(DBCHWL)
#pragma export(OsGetCOPCLIVersion)
#endif  /* CLI_MF_GTW */
/* <-- CLIWS-7751 */

#endif  /* NOPROT */

/***************************************************************/
/*                       END of DBCAREA.H                      */
/***************************************************************/
#endif /* DBCAREA_H */



================================================
FILE: src/teradata/parcel.h
================================================
#ifndef PARCEL_H
#define PARCEL_H
#define PARCEL_H_REV "20.00.00.02"
/*******************************************************************************
 *  TITLE:       PARCEL.H .. Parcel type definitions for "C"         20.00.00.02
 *
 *  Purpose      To contain the parcel type information needed
 *               for MTDP, CLIv2, and application programs.
 *
 *  Copyright 1983-2023 Teradata. All rights reserved.
 *  TERADATA CONFIDENTIAL AND TRADE SECRET.
 *
 *  Description  This include file contains the parcel type
 *               definitions for the workstation version of
 *               BTEQ and CLIv1.
 *
 *  History
 *   D.1   86Jul28  DRP           Coded
 *   D.2   87Feb24  KKL  DR 9381  Corrected some comments
 *   D.3   87Mar17  KKL  DR 9381  changed PclLength to Length
 *                                or PLength
 *   F.1   87Jul22  DAA  DCR3780  added Options Parcel
 *   F.2   87Aug11  DAA  DCR3780  updated AssignRsp for Ver.
 *   F.3   87Jan08  DRP           Release F
 *   F.4   86Jan16  DRP  DR 8832  Fixed Success parcel &
 *                                PCLLENGTHs
 *   F.5   87Mar17  DRP  DR 9341  changed for release F Ver.
 *   F.6   87Apr29  DRP  DR 9923  fixed success parcel
 *   F.7   87Nov03  DAA  DR 11006 added HostID to AssignRsp
 *   F.8   87Nov15  SCT  DR 10471 Narrowed to <=65 characters
 *   F.9   87Nov15  DAA  DR 11006 add semicolon on
 *   F.10  87Nov17  SCT  DR 10471 Fixed my case errors
 *   F.11  87Nov17  SCT  DR 10471 Fixed my case
 *   F.12  88Jan10  DRP  DR 11680 Added  CliPrepInfoType and
 *                                CliPrepColInfoType.
 *   F.13  88Jan28  WHY  DR 11926 (MR# fe87-18102a0)
 *   F.14  88May12  YDS  DCR 4505 Add new SQLstmttypes.
 *   F.15  88Jul13  WBJ  DR 10533 Add PclNOFLAVOR
 *   G0_00 88Jul25  SN            Created for Release G.0
 *   G0_01 88Oct06  WBJ  DCR 4029 & other G.0 mods
 *   G0_02 88Oct17  WBJ  DCR 4029 configresp format refined
 *   G0_03 88Nov03  WBJ  DCR 4029 fix AMPArray def for  3b2
 *   G0_04 89Jun19  KKE  DCR 5094 Define set session
 *                                collation Pcl type.
 *   G0_05 89Dec22  SNT  DR 17876 Define Rel. Mload Pcltype
 *   G0_06 90Sep14  KXO           Revised for UNIXTOOL KIT
 *   G1_01 91Jan29  BUGS DCR4350  CBTEQ port.
 *   G1_02 91Jan29  BUGS DCR4350  Port to VM (GLL's 7/1/91 changes).
 *   G1_03 91Feb20  BUGS DR21617  Compile error (CR before #ifdef ATT)
 *   H0_00 93May20  SNT  DCR6711  Created for Release H.0
 *   H0_01 94Apr04  JAE  DCR6712  Merged 2PC code for Genesis II.
 *   HN_00 94Aug05  JAE  DCR6848  New Parcels for NDM Contract
 *   HN_01 94Nov30  JAE  DCR6848  Changed parcel constant names to
 *                                UPPER CASE for consistency.
 *                                This is also done for the 2PC constants
 *                                as well as the NDM constants.
 *   HN_02       ALB      95Feb09   DR32294 Merged 5.0 and AFCAC code.
 *   H3_00       TH4      95Sep13   DR34047 Added the #ifndef XXXX #define XXXX.
 *   04.00.00.01 TH4      96Jul12   DR36785 Version number change.
 *   04.02.00.00 TH4      97Sep10   DR40417 ANSI Date/Time Support.
 *   04.02.00.01 JTH      97Dec16   DR41042 Max request Length
 *   04.02.00.02 JTH      98jAN06   DR41136 fix regression caused by dr41042
 *   04.02.00.03 MDV      98Mar24   DR41685 Support database triggers.
 *   04.04.00.00 TH4      98Jun22   DR42286 Change the Int32 to UInt32 for AC.
 *   04.05.00.00 CSG      2000Mar03 DR48735 TDSP Support
 *   04.05.00.01 CSG      2000Mar22 DR50139 TDSP Report Error 374 (NOTDSP)
 *   04.06.00.00 ASG      2001Jan17 DR52052 Support SSO
 *   04.06.00.01 ASG      2001Feb26 DR54273 Fix re-logon problem
 *   04.06.01.00 ASG      2001Aug08 DR56467 #define non-standard parcel names
 *   04.06.02.00 CSG      2002Mar28 DR60508 Add PerformUpdate feature support
 *   04.07.00.00 CSG      2002Jun04 DR57320 Support Increased maximum
 *                                  response size
 *   04.07.00.01 CSG      2002Jun14 DR61637 Support enlarged parcel usage.
 *   04.07.00.02 ASG      2002Jun03 DR57583 LOB support
 *   04.07.00.03 CSG      2002Aug28 DR62240 New activity counts in DR59863.
 *   04.07.00.04 CSG      2002Sep16 DR59000 Port WinCli to XP-64 bit.
 *   04.07.01.00 ASG      2003Feb10 DR66590 Add PrepInfoX parcel.
 *   04.07.01.01 mg180007 2003Mar12 DR67744 Remove HPUX compilation warnings.
 *   04.07.01.02 ASG/DHV  2002Jul28 DR52869 Add GSS support
 *                                  mg180007 2003Mar27 merged changes by
 *                                  ASG/DHV into 4.7.x
 *   04.07.01.03 ASG      2003Mar31 DR60695 Cursor positioning.
 *   04.07.01.04 ASG      2003Apr04 DR68008 Add pad byte to ElicitFile.
 *   04.07.01.05 CSG      2003Apr04 DR67892 Add UDF stmt types.
 *   04.07.01.06 CSG      2003Apr16 DR68080 Resolve c++ compile errors
 *   04.07.01.07 ASG      2003Apr23 DR68105 Fix struct FieldType for DataInfoX.
 *   04.07.01.08 ASG      2003May13 DR68259 Fix alignment for SPARC, HPUX, AIX
 *   04.07.01.09 CSG      2003Jun24 DR68615 ppruntim compile error on
 *                                          Solaris Sparc/Define parcel.h
 *                                          own typedefs to resolve HPUX
 *                                          packing problem
 *   04.08.00.00 mg180007 2003Jul22 DR68511 clean up defines, prototypes
 *                                  DR68139 self-sufficient headers
 *   04.08.00.01 ASG      2003Dec30 DR68835 APH / 1 MB Responses
 *   04.08.00.02 CSG      2003Sep29 DR69228 Add support for character
 *                                          object state
 *   04.08.00.03 ASG      2004Feb10 DR58352 ESS
 *   04.08.00.04 CSG      2004Jan27 DR84603 Added pack(1) for Win-32
 *   04.08.00.05 ASG      2004Mar03 DR85431 universalize headers
 *   04.08.00.06 CSG      2004Mar16 DR52058 UTF16 support
 *   04.08.00.07 ASG      2004Apr12 DR86770 fix ErrorInfo parcel flavor
 *   04.08.00.08 mg180007 2004May14 DR68842 TTU 8.0 Security support
 *   04.08.00.09 ASG      2004Jul29 DR88784 Accommodate MF BTEQ
 *   04.08.00.10 ASG      2004Sep01 DR89769 Conform to DR 83892
 *   04.08.00.11 CSG      2005Jun24 DR94488 Port to HP-Itanium
 *   04.08.01.00 ASG      2005Mar10 DR92423 Add tx-isolation stmt type
 *   04.08.01.01 mg180007 2005Mar30 DR91422 HP_ALIGN pragma issue with aCC
 *   04.08.01.02 mg180007 2005Apr21 DR93472 added header revision info
 *   04.08.01.03 mg180007 2005Apr28 DR94976 Gateway UTF credentials support
 *   04.08.01.04 mg180007 2005Aug29 DR96966 Add PCLCLICONFIGTDGSS
 *   04.08.02.00 bh185000 2005Dec15 DR98024 Support for MaxDecimalPrecision
 *                                  DR98025 Support for AGKR_option
 *                                  DR98026 Support for StatementInfo
 *   04.08.02.01 bh185000 2006Jan12 DR101116 Data length need to be Uint16
 *   04.08.02.02 ASG      2006Feb14 DR102061 reconfig for 80-byte lines
 *   04.08.02.03 bh185000 2006Mar08 DR98026 Add StatementInfo defines
 *   04.08.02.04 ASG      2006Apr25 DR98026 Add StatementInfo MetaData detail
 *   04.09.00.00 bh185000 2006May31 DR102528 Add Dynamic Result Sets
 *   04.09.00.01 bh185000 2006Jun26 DR103360 Add Query Banding activity type
 *   04.09.00.02 bh185000 2006Jun26 DR105446 Complete activity type 155 to 172
 *   04.09.00.03 bh185000 2006Aug15 DR106619 Additional define for StatementInfo
 *   04.09.00.04 mg180007 2006Aug17 DR101074 add client version to cfg request
 *   04.09.00.05 mg180007 2006Jul11 DR101287 Add pclusernameresp_t
 *   12.00.00.06 fs185009 2006Nov29 DR108852 version change to 12.0.x
 *   12.00.00.07 ASG      2006Dec04 DR107934 Fix _O_BYTEALIGN check for MF SAS/C
 *   12.00.00.08 mg180007 2006Dec12 DR109200 DynamicResultSets positioning
 *   12.00.00.09 bh185000 2007Feb21 DR111230 Add SP Result Set activity code
 *   12.00.00.10 mg180007 2007Feb22 DR101074 fixed client version parcels
 *   13.00.00.00 bh185000 2007Sep26 DR113468 Support session node id
 *   13.00.00.01 bh185000 2007Nov12 DR115324 Support LOB Loader Phase 2
 *   13.00.00.02 bh185000 2007Nov13 DR115327 Support DDL Replication
 *   13.00.00.03 bh185000 2007Nov13 DR115330 Support User Defined SQL Operators
 *   13.00.00.04 mg180007 2008Jan09 DR117140 copyright update
 *   13.00.00.05 bh185000 2008Jan21 DR119336 Completed list of activity types
 *   13.01.00.00 bh185000 2008Nov17 DR116354 Support Extended Object Name Parcel
 *   13.01.00.01 bh185000 2008Dec03 DR121443 Support Mandatory Access Control
 *   13.01.00.02 bh185000 2008Dec09 DR123516 Support StmtInfo UDT Transforms Off
 *   13.01.00.03 kl185018 2009Mar12 DR125340 32-bit programming for HPUX-ia64
 *   13.01.00.04 bh185000 2009Mar29 DR126915 Support TASM Utility Management
 *   13.10.00.04 kl185018 2009Jun23 DR133468 Version change from 13.01 to 13.10
 *   14.00.00.00 bh185000 2010Oct06 DR121202 Support statement Independence
 *   14.00.00.01 bh185000 2010Dec11 DR145672 Support activity types in TTU14.0
 *   14.00.00.02 bh185000 2011Feb24 DR145224 Support Array Data Type
 *   14.00.01.00 bh185000 2011Jun28 DR147092 Support Client Attribute parcels
 *   14.10.00.00 hs186016 2011Dec15 CLAC-29160 Add missing defs from pclbody.h
 *   14.10.00.01 bh185000 2012Jan10 CLAC-28875 Support XML Data Type
 *   14.10.00.02 mg180007 2012Mar14 CLAC-29474 Add PclRevalidateRefStmt
 *   14.10.00.03 hs186016 2012Mar15 CLAC-29367 Remove unused CliPrepColInfoType
 *                                             and CliPrepColInfoTypeX
 *   14.10.00.04 mg180007 2012Mar28 CLAC-28857 Support ESS
 *   14.10.00.05 mg180007 2012Apr13 CLAC-29422 Activity types for Enhanced
 *                                             Query Capture Facility
 *   14.10.00.06 mg180007 2012Apr23 CLAC-29578 Activity type for Show in XML
 *   14.10.00.07 bh185000 2012May16 CLAC-29283 Support Extended Multiload
 *   14.10.00.08 mg180007 2012May16 CLAC-28857 Added ESS ResponseMode constants
 *   14.10.00.09 hs186016 2012May22 CLAC-29672 Restore CliPrepColInfoType and
 *                                             CliPrepColInfoTypeX into parcel.h
 *   14.10.00.10 en185000 2011Sep21 DR145358, CLAC-29119 AlwaysOn-Redrive
 *   14.10.00.11 mg180007 2012Jun08 CLAC-29222 Support Secuity Policy
 *   14.10.00.12 kl185018 2012Jun29 CLAC-29270 Support TASM FastFail Feature
 *   14.10.00.13 bh185000 2012Jul27 CLAC-29854 Activity types in TTU14.10
 *   14.10.00.14 mg180007 2012Nov05 CLAC-30059 ESS with XSP Default Connection
 *   14.10.00.15 hs186016 2013Jan06 CLAC-18937 NW CLIv2 support for Mac OS X
 *   14.10.00.16 hs186016 2013Feb21 CLAC-30478 Support TASM FastFail(redesigned)
 *   15.00.00.00 bh185000 2013May09 CLAC-30608 Support new activity types
 *   15.00.00.01 hs186016 2013Nov14 CLAC-30554 Support SLOB
 *   15.10.00.00 mg180007 2014Apr28 CLAC-32368 version and copyright update
 *   15.10.00.01 bh185000 2014Jul30 CLAC-32368 SLOB Server to Client support
 *   15.10.00.02 bh185000 2014Aug19 CLAC-32605 Update Activity Types
 *   15.10.00.03 mg180007 2014Sep01 CLAC-31283 Support negotiating mechanisms
 *   15.10.00.04 hs186016 2014Sep17 CLAC-30758 Add Parcel structure for SLOB
 *   16.00.00.00 bh185000 2015Mar12 CLAC-33337 Update Activity Types
 *   16.00.00.01 mg180007 2015Mar23 CLAC-29671 Support more ClientAttributes
 *   16.00.00.02 bh185000 2015Apr20 CLAC-33337 Additional update Activity Types
 *   16.00.00.03 bh185000 2015Jun24 CLAC-29115 Additional fix of 1MB resp row
 *   16.00.00.04 bh185000 2015Sep29 CLAC-34036 Add new activity type
 *   16.00.00.05 bh185000 2015Nov10 CLAC-34171 No Max size in struct definition
 *   16.20.00.00 hs186016 2017Dec06 CLIWS-6627 Add new activity type for
 *                                             Transform Group
 *   16.20.00.01 hs186016 2017Dec06 CLIWS-6720 Add new activity types for Alias
 *   16.20.00.02 hs186016 2018May04 CLIWS-6953 Add new activity type for
 *                                             Incremental Restore
 *   16.20.00.03 hs186016 2018Jul03 CLIWS-7014 Add new activity type for
 *                                             Set Session UDFSearchPath
 *   17.00.00.00 hs186016 2018Oct17 CLIWS-7052 Add new activity type for
 *                                             Execute Analytic Function
 *                                             PCLEXECFUNCSTMT (246)
 *   17.00.00.01 vt186023 2019May22 CLIWS-7192 Support TLS v1.2
 *   17.00.00.02 hs186016 2020Jan06 CLIWS-7256 Add new activity type for
 *                                             Multi-Output Table Support for
 *                                             Analytic functions
 *   17.00.00.03 hs186016 2020Mar10 CLIWS-7544 Add new activity type for
 *                                             Execute Analytic Function
 *                                             PCLEXECFUNCSTMTNOARTOUT (250)
 *   17.10.00.00 hs186016 2020Jul27 CLIWS-7606 version change to 17.10
 *   17.10.00.01 hs186016 2020Sep03 CLIWS-7647 Handle gateway error code 8059
 *   17.10.00.02 hs186016 2021Mar12 CLIWS-7750 Add new activity types for
 *                                             Compute Operational Group (COG)
 *   17.10.00.03 hs186016 2021Mar12 CLIWS-7870 Add new activity types for
 *                                             Spool Map
 *   17.10.00.04 hs186016 2021Mar22 CLIWS-7896 Use pack(1) and pack(pop) for AIX
 *   17.10.00.05 rp255060 2021Mar23 CLIWS-7780 Support ConfigPorts config-response
 *   17.10.00.06 hs186016 2021Apr15 CLIWS-7750 Update new activity types for
 *                                             Compute Operational Group (COG)
 *   17.10.00.07 hs186016 2021Jun03 CLIWS-7751 Support mainframe on z/OS
 *   17.10.00.08 hs186016 2021Jun07 CLIWS-7934 Add new activity types for Native
 *                                             Object Store (NOS) File System
 *   17.20.00.00 hs186016 2022Jan01 CLIWS-8198 version and copyright update
 *   17.20.00.01 rp255060 2022Feb04 CLIWS-7948 Federated authentication support
 *   17.20.00.02 rp255060 2022Oct03 CLIWS-8509 Store encryption mode in ClientAttributeEx
 *   17.20.00.03 js186142 2023Mar16  CLMF-9820 Add jobname and jobid client attributes
 *   17.20.00.04 rp255060 2023Mar27 CLIWS-8666 Support OIDC scope parameter in config response
 *   20.00.00.00 hs186016 2023May11 CLIWS-8696 version and copyright update
 *   20.00.00.01 hs186016 2023Sep15 CLIWS-8852 New Activity Types to support Open Table Format
 *                                             - Create/Alter/Drop DATALAKE
 *   20.00.00.02 hs186016 2023Dec13 CLIWS-8996 Add new Activity Types to support Open Table
 *                                             Format(OTF) - Replace DataLake
 ******************************************************************************/

/***************************************************************/
/*                          INTRODUCTION                       */
/***************************************************************/
/*   A parcel is a unit of information carried in a message &  */
/*   consists of:                                              */
/*                                                             */
/*     1. A "flavor" field (2 bytes) which specifies the kind  */
/*        of parcel.                                           */
/*     2. A length field (2 bytes) which specifies the total   */
/*        parcel length (n) in bytes.                          */
/*     3. The "body" (n-4 bytes) which contains data, the      */
/*        meaning (identification) of which depends on the     */
/*        parcel flavor.                                       */
/*                                                             */
/*   A message may be composed of one or more parcels; however */
/*   a parcel must be contained wholly within a message (i.e., */
/*   parcels cannot span messages).  So the maximum size of a  */
/*   message is 32KBytes - 1, and the fixed part of a message  */
/*   is 78 bytes for a normal request and 85 bytes for TDSP    */
/*   segmented request,  so the maximum size of a parcel is    */
/*                                                             */
/*                     32KBytes - 1 - 78                       */
/*                           OR                                */
/*                     32KBytes - 1 - 85                       */
/*                                                             */
/*                                                             */
/*    IT IS ADVISABLE THAT THE APPLICATIONS *DO NOT*           */
/*            EXCEED THE REQUEST MORE THAN                     */
/*            65000 BYTES FOR 64K SUPPORT                      */
/*                       AND                                   */
/*            32500 BYTES FOR 32K SUPPORT                      */
/*                                                             */
/*                                                             */
/***************************************************************/

/***************************************************************/
/* Notes    Although we would like to hold a parcel flavor in  */
/*          an enumerated type, there would be a problem       */
/*          talking to non-C programs, or even code generated  */
/*          by C's which do different things with enumerated   */
/*          types.  For this reason, constants are defined     */
/*          below of the form PclXYZ where XYZ is an abbrevia- */
/*          tion for the parcel flavor.                        */
/***************************************************************/

#include "coptypes.h"

/***************************************************************/
/*                 TYPEDEFS, PclTYPE & PclHeadType             */
/***************************************************************/

/* DR 68259 ---> */
/* DR84603: Add pack(1) for Win-32 */
#if defined(WIN32) || defined(__APPLE__)       /* CLAC-18937 */
#pragma pack(push,1)
#else
#pragma pack(1)
#endif
/* <--- DR 68259 */

typedef    unsigned short       PclFlavor;
typedef    unsigned short       PclLength;
typedef    unsigned short       PclLength_t;   /* DR68080: Define data type
                                                   same as PclLength  */
/* CLAC-34171 --> */
#ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
typedef    char                 PclBodyType[SysMaxParcelSize];
#else
typedef    char                 PclBodyType[1];
#endif
/* <--CLAC-34171 */

                                           /*--> DR61637 */
typedef    unsigned int         PclAltLength;
/* CLAC-34171 --> */
#ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
typedef    char                 PclAltBodyType[SysExtMaxParcelSize];
#else
typedef    char                 PclAltBodyType[1];
#endif
/* <--CLAC-34171 */
                                          /*--> DR61637 */
                                          /*--> DR68615 */
typedef    char                 Pclchar;
typedef    unsigned char        PclByte;
typedef    short                PclInt16;
typedef    short *              PclInt16_Ptr;
typedef    unsigned short       PclWord;
typedef    unsigned short       PclUInt16;
typedef    unsigned short *     PclUInt16_Ptr;
typedef    int                  PclInt32;
typedef    int *                PclInt32_Ptr;
typedef    unsigned int         PclUInt32;
typedef    unsigned int *       PclUInt32_Ptr;
typedef    unsigned int         PclDouble_Word;
typedef    unsigned             Pclprotag_t;
typedef    char *               PclPtrType ;
typedef    char *               Pclstorid_t;

#ifdef WIN32
  typedef  __int64              PclInt64;
  typedef  unsigned __int64     PclUInt64;
#else
  typedef long long             PclInt64;
  typedef unsigned long long    PclUInt64;
#endif
                                       /*--> DR68615 */

/*==================*/
/* struct PclType   */
/*==================*/

struct  PclType
{
    PclFlavor           Flavor;
    PclLength           Length;
    PclBodyType         Body;
};

/*--> DR61637        */
/*===================*/
/* struct PclAltType */
/*===================*/
struct PclAltType
{
    PclFlavor           Flavor;
    PclLength           Length;
    PclAltLength        AltLength;
    PclAltBodyType      Body;
};

/*=========================*/
/* struct PclAltHeadType   */
/*=========================*/
struct PclAltHeadType
{
    PclFlavor           Flavor;
    PclLength           Length;
    PclAltLength        AltLength;
};
/*         DR61637<--*/

/*======================*/
/* struct PclHeadType   */
/*======================*/

struct  PclHeadType
{
    PclFlavor           Flavor;
    PclLength           Length;
};


/***************************************************************/
/*                        PARCEL FLAVORS                       */
/***************************************************************/

#define  PclNOFLAVOR     (PclFlavor)(0)
#define  PclREQ          (PclFlavor)(1)
#define  PclRUNSTARTUP   (PclFlavor)(2)
#define  PclDATA         (PclFlavor)(3)
#define  PclRESP         (PclFlavor)(4)
#define  PclKEEPRESP     (PclFlavor)(5)
#define  PclABORT        (PclFlavor)(6)
#define  PclCANCEL       (PclFlavor)(7)
#define  PclSUCCESS      (PclFlavor)(8)
#define  PclFAILURE      (PclFlavor)(9)
#define  PclRECORD       (PclFlavor)(10)
#define  PclENDSTATEMENT (PclFlavor)(11)
#define  PclENDREQUEST   (PclFlavor)(12)
#define  PclFMREQ        (PclFlavor)(13)
#define  PclFMRUNSTARTUP (PclFlavor)(14)
#define  PclVALUE        (PclFlavor)(15)
#define  PclNULLVALUE    (PclFlavor)(16)
#define  PclOK           (PclFlavor)(17)
#define  PclFIELD        (PclFlavor)(18)
#define  PclNULLFIELD    (PclFlavor)(19)
#define  PclTITLESTART   (PclFlavor)(20)
#define  PclTITLEEND     (PclFlavor)(21)
#define  PclFORMATSTART  (PclFlavor)(22)
#define  PclFORMATEND    (PclFlavor)(23)
#define  PclSIZESTART    (PclFlavor)(24)
#define  PclSIZEEND      (PclFlavor)(25)
#define  PclSIZE         (PclFlavor)(26)
#define  PclRECSTART     (PclFlavor)(27)
#define  PclRECEND       (PclFlavor)(28)
#define  PclPROMPT       (PclFlavor)(29)
#define  PclENDPROMPT    (PclFlavor)(30)
#define  PclREWIND       (PclFlavor)(31)
#define  PclNOP          (PclFlavor)(32)
#define  PclWITH         (PclFlavor)(33)
#define  PclPOSITION     (PclFlavor)(34)
#define  PclENDWITH      (PclFlavor)(35)
#define  PclLOGON        (PclFlavor)(36)
#define  PclLOGOFF       (PclFlavor)(37)
#define  PclRUN          (PclFlavor)(38)
#define  PclRUNRESP      (PclFlavor)(39)
#define  PclUCABORT      (PclFlavor)(40)
#define  PclHOSTSTART    (PclFlavor)(41)
#define  PclCONFIG       (PclFlavor)(42)
#define  PclCONFIGRESP   (PclFlavor)(43)
#define  PclSTATUS       (PclFlavor)(44)
#define  PclIFPSWITCH    (PclFlavor)(45)
#define  PclPOSSTART     (PclFlavor)(46)
#define  PclPOSEND       (PclFlavor)(47)
#define  PclBULKRESP     (PclFlavor)(48)
#define  PclERROR        (PclFlavor)(49)
#define  PclDATE         (PclFlavor)(50)
#define  PclROW          (PclFlavor)(51)
#define  PclHUTCREDBS    (PclFlavor)(52)
#define  PclHUTDBLK      (PclFlavor)(53)
#define  PclHUTDELTBL    (PclFlavor)(54)
#define  PclHUTINSROW    (PclFlavor)(55)
#define  PclHUTRBLK      (PclFlavor)(56)
#define  PclHUTSNDBLK    (PclFlavor)(57)
                                          /* 58 is  not in use */
#define  PclHUTRELDBCLK  (PclFlavor)(59)
#define  PclHUTNOP       (PclFlavor)(60)
#define  PclHUTBLD       (PclFlavor)(61)
#define  PclHUTBLDRSP    (PclFlavor)(62)  /* currently not used*/
#define  PclHUTGETDDT    (PclFlavor)(63)
#define  PclHUTGETDDTRSP (PclFlavor)(64)
#define  PclHUTIDx       (PclFlavor)(65)
#define  PclHUTIDxRsp    (PclFlavor)(66)
#define  PclFieldStatus  (PclFlavor)(67)
#define  PclINDICDATA    (PclFlavor)(68)
#define  PclINDICREQ     (PclFlavor)(69)
                                          /* 70 is not in use  */
#define  PclDATAINFO     (PclFlavor)(71)
#define  PclIVRUNSTARTUP (PclFlavor)(72)
#define  PclOPTIONS      (PclFlavor)(85)
#define  PclPREPINFO     (PclFlavor)(86)
#define  PclCONNECT      (PclFlavor)(88)
#define  PclLSN          (PclFlavor)(89)
#define  PclErrorCnt     (PclFlavor)(105)
#define  PclSESSINFO     (PclFlavor)(106)
#define  PclSESSINFORESP (PclFlavor)(107)
#define  PclSESSOPT      (PclFlavor)(114)
#define  PclVOTEREQUEST  (PclFlavor)(115)
#define  PclVOTETERM     (PclFlavor)(116)
#define  PclCMMT2PC      (PclFlavor)(117)
#define  PclABRT2PC      (PclFlavor)(118)
#define  PclFORGET       (PclFlavor)(119)

/*=============================*/
/*  NDM parcels only.  HN_00   */
/*=============================*/

#define  PclCURSORHOST    (PclFlavor)(120)    /* HN_00 - NDM Support */
#define  PclCURSORDBC     (PclFlavor)(121)    /* HN_00 - NDM Support */
#define  PclFLAGGER       (PclFlavor)(122)    /* HN_00 - NDM Support */

#define  PclPREPINFOX     (PclFlavor)(125)    /* DR 66590 */

/*=============================*/
/*  TDSP Parcels.  DR48735     */
/*=============================*/

#define  PclMULTITSR      (PclFlavor)(128)    /* DR48735 */
#define  PclSPOPTIONSTYPE (PclFlavor)(129)    /* DR48735 */

/*=============================*/
/*   SSO Parcels (DR52052)     */
/*=============================*/

/* Single Sign-On Related Parcels   DR52052, DR51567-rhh     */
#define PcLSSOAUTHREQ    (130)  /*  SSO Authorization Request       */
                                /* Only used in the gateway         */
#define PcLSSOAUTHRESP   (131)  /* SSO Authorization Response       */
                                /* Only used in the gateway         */
#define PcLSSOREQ        (132)  /* SSO Request                      */
#define PcLSSODOMAIN     (133)  /* SSO User Domain                  */
#define PcLSSORESP       (134)  /* SSO Response                     */
#define PcLSSOAUTHINFO   (135)  /* SSO Authentication Info          */
#define PcLUSERNAMEREQ   (136)  /* SSO User Name Request            */
#define PcLUSERNAMERESP  (137)  /* SSO Name Response                */
/* Single Sign-On Related Parcels   DR52052, DR51567-rhh     */
/* begin DR56467 */
#define PclSSOAUTHREQ    PcLSSOAUTHREQ
#define PclSSOAUTHRESP   PcLSSOAUTHRESP
#define PclSSOREQ        PcLSSOREQ
#define PclSSODOMAIN     PcLSSODOMAIN
#define PclSSORESP       PcLSSORESP
#define PclSSOAUTHINFO   PcLSSOAUTHINFO
#define PclUSERNAMEREQ   PcLUSERNAMEREQ
#define PclUSERNAMERESP  PcLUSERNAMERESP
/* end DR56467 */

/*=============================*/
/*     DR57583 - LOBs          */
/*                             */
/*=============================*/

#define PclQMD        (140)  /* MultipartData                    */
#define PclQEMD       (141)  /* EndMultipartData                 */
#define PclQMID       (142)  /* MultipartIndicData               */
#define PclQEMID      (143)  /* EndMultipartIndicData            */
#define PclSMR        (144)  /* MultipartRecord                  */
#define PclSEMR       (145)  /* EndMultipartRecord               */
#define PclXDIX       (146)  /* DataInfoX                        */
#define PclQMRS       (147)  /* MultipartRunStartup              */
#define PclQMR        (148)  /* MultipartRequest                 */
#define PclSEDM       (149)  /* ElicitDataMailbox                */
#define PclSED        (150)  /* ElicitData                       */
#define PclSEF        (151)  /* ElicitFile                       */
#define PclSEDR       (152)  /* ElicitDataReceived               */

/*=============================*/
/* DR57320 Extended Response   */
/*         Parcels             */
/*=============================*/

#define  PclBIGRESP      (PclFlavor)(153) /* DR57130               */
#define  PclBIGKEEPRESP  (PclFlavor)(154) /* DR57130               */

/*=============================*/
/* DR60695 Cursor Positioning  */
/*                             */
/*=============================*/

/* Parcel to indicate positioning may be requested */
#define  PclSETPOSITION    (PclFlavor)(157)
/* Parcel to define the first row to be returned */
#define  PclROWPOSITION     (PclFlavor)(158)
/* Parcel to define where the LOB data should be */
#define  PclOFFSETPOSITION  (PclFlavor)(159)

#define PclRESULTSUMMARY (163) /* DR58352: ResultSummary           */
#define PclERRORINFO     (164) /* DR86770: ErrorInfo               */

/*===============================================*/
/* DR61637 APH Parcels : Set MSB for APH flavors */
/*===============================================*/
#define SYSALTPCLINDICATOR             0x8000
#define PclAltREQ          (PclFlavor) (PclREQ        | SYSALTPCLINDICATOR)
#define PclAltRUNSTARTUP   (PclFlavor) (PclRUNSTARTUP | SYSALTPCLINDICATOR)
#define PclAltDATA         (PclFlavor) (PclDATA       | SYSALTPCLINDICATOR)
#define PclAltCANCEL       (PclFlavor) (PclCANCEL     | SYSALTPCLINDICATOR)
#define PclAltFMREQ        (PclFlavor) (PclFMREQ      | SYSALTPCLINDICATOR)
#define PclAltFMRUNSTARTUP (PclFlavor) (PclFMRUNSTARTUP | SYSALTPCLINDICATOR)
#define PclAltNOFLAVOR     (PclFlavor) (PclNOFLAVOR   | SYSALTPCLINDICATOR)
#define PclAltINDICDATA    (PclFlavor) (PclINDICDATA  | SYSALTPCLINDICATOR)
#define PclAltINDICREQ     (PclFlavor) (PclINDICREQ   | SYSALTPCLINDICATOR)
#define PclAltDATAINFO     (PclFlavor) (PclDATAINFO   | SYSALTPCLINDICATOR)
#define PclAltIVRUNSTARTUP (PclFlavor) (PclIVRUNSTARTUP | SYSALTPCLINDICATOR)
#define PclAltOPTIONS      (PclFlavor) (PclOPTIONS    | SYSALTPCLINDICATOR)
#define PclAltCURSORHOST   (PclFlavor) (PclCURSORHOST | SYSALTPCLINDICATOR)
#define PclAltMULTITSR     (PclFlavor) (PclMULTITSR   | SYSALTPCLINDICATOR)
#define PclAltSPOPTIONSTYPE (PclFlavor) (PclSPOPTIONSTYPE  | SYSALTPCLINDICATOR)
#define PclAltBIGRESP      (PclFlavor) (PclBIGRESP    | SYSALTPCLINDICATOR)
#define PclAltBIGKEEPRESP  (PclFlavor) (PclBIGKEEPRESP | SYSALTPCLINDICATOR)
#define PclAltRESP         (PclFlavor) (PclRESP    |  SYSALTPCLINDICATOR)
#define PclAltKEEPRESP     (PclFlavor) (PclKEEPRESP | SYSALTPCLINDICATOR)
#define PclAltQMD          (PclFlavor) (PclQMD      | SYSALTPCLINDICATOR)
#define PclAltQEMD         (PclFlavor) (PclQEMD     | SYSALTPCLINDICATOR)
#define PclAltQMID         (PclFlavor) (PclQMID     | SYSALTPCLINDICATOR)
#define PclAltQEMID        (PclFlavor) (PclQEMID    | SYSALTPCLINDICATOR)
#define PclAltXDIX         (PclFlavor) (PclXDIX     | SYSALTPCLINDICATOR)
#define PclAltQMRS         (PclFlavor) (PclQMRS     | SYSALTPCLINDICATOR)
#define PclAltQMR          (PclFlavor) (PclQMR      | SYSALTPCLINDICATOR)
#define PclAltREWIND       (PclFlavor) (PclREWIND   | SYSALTPCLINDICATOR)
/* <-- DR61637 */
/* DR 60695 -> */
#define  PclAltSETPOSITION (PclFlavor)(PclSETPOSITION | SYSALTPCLINDICATOR)
#define  PclAltROWPOSITION (PclFlavor)(PclROWPOSITION | SYSALTPCLINDICATOR)
#define  PclAltOFFSETPOSITION \
   (PclFlavor)(PclOFFSETPOSITION | SYSALTPCLINDICATOR)
/* <-- DR 60695 */
#define PclAltERRORINFO    (PclFlavor)(PclERRORINFO | SYSALTPCLINDICATOR)
/* DR 58352 --> */
#define PclAltRESULTSUMMARY (PclFlavor)(PclRESULTSUMMARY | SYSALTPCLINDICATOR)
/* <-- DR 58352 */

/* DR68842 -> */
#define PclGTWCONFIG        (165)    /* Gateway configuration parcel */
#define PclCLIENTCONFIG     (166)    /* Client configuration parcel  */
#define PclAUTHMECH         (167)    /* Gateway Auth Methods parcel  */
/* <- DR68842 */

/* DR98026 -> */
#define PclSTATEMENTINFO    (169)
#define PclSTATEMENTINFOEND (170)
#define PclAltSTATEMENTINFO \
       (PclFlavor)(PclSTATEMENTINFO | SYSALTPCLINDICATOR)
#define PclAltSTATEMENTINFOEND \
       (PclFlavor)(PclSTATEMENTINFOEND | SYSALTPCLINDICATOR)
/* <- DR98026 */

#define PclRESULTSET        (172)    /* DR102528, Dynamic Result Sets */
#define PclAltRESULTSET \
        (PclFlavor)(PclRESULTSET | SYSALTPCLINDICATOR)
#define PclRESULTSETCURPOS  (175)    /* DR109200, SPDR positioning    */
#define PclAltRESULTSETCURPOS \
        (PclFlavor)(PclRESULTSETCURPOS | SYSALTPCLINDICATOR)
#define PclELICITDATABYNAME (176)    /* DR115324, LOB Loader Pahse 2  */
#define PclAltELICITDATABYNAME \
        (PclFlavor)(PclELICITDATABYNAME | SYSALTPCLINDICATOR)
#define PclCLIENTATTRIBUTE  (189)    /* DR147092 Client Attribute     */
#define PclAltCLIENTATTRIBUTE \
        (PclFlavor)(PclCLIENTATTRIBUTE | SYSALTPCLINDICATOR)
#define PclSTMTERROR        (192)    /* DR121202 Statment Independence */
#define PclAltSTMTERROR \
        (PclFlavor)(PclSTMTERROR | SYSALTPCLINDICATOR)

/* DR145358, CLAC-29119 -> */
#define PclSESSIONSTATUS          (194)
#define PclSESSIONSTATUSRESP      (195)
#define PclCONTINUECONTEXT        (196)
#define PclAltCONTINUECONTEXT \
        (PclFlavor)(PclCONTINUECONTEXT | SYSALTPCLINDICATOR)
#define PclCONTROLDATASTART       (197)
#define PclAltCONTROLDATASTART \
        (PclFlavor)(PclCONTROLDATASTART | SYSALTPCLINDICATOR)
#define PclCONTROLDATAEND         (198)
#define PclAltCONTROLDATAEND \
        (PclFlavor)(PclCONTROLDATAEND | SYSALTPCLINDICATOR)
#define PclRECOVERABLEPROTOCOL    (200) /* GTW-DBS use only */
#define PclAltRECOVERABLEPROTOCOL \
        (PclFlavor)(PclRECOVERABLEPROTOCOL | SYSALTPCLINDICATOR)
#define PclREDRIVE                (201)
#define PclAltREDRIVE \
        (PclFlavor)(PclREDRIVE | SYSALTPCLINDICATOR)
#define PclACKREQUESTED           (202)
#define PclAltACKREQUESTED \
        (PclFlavor)(PclACKREQUESTED | SYSALTPCLINDICATOR)
#define PclREDRIVESUPPORTED       (204)
#define PclAltREDRIVESUPPORTED \
        (PclFlavor)(PclREDRIVESUPPORTED | SYSALTPCLINDICATOR)
#define PclSTATEMENTSTATUS        (205)     /* CLAC-28857: ESS               */
#define PclAltSTATEMENTSTATUS \
        (PclFlavor)(PclSTATEMENTSTATUS | SYSALTPCLINDICATOR)
#define PclRECOVERABLENPSUPPORTED (214)
#define PclAltRECOVERABLENPSUPPORTED \
        (PclFlavor)(PclRECOVERABLENPSUPPORTED | SYSALTPCLINDICATOR)
/* <- DR145358, CLAC-29119 */
/* CLAC-29222 -> */
#define PclSECURITYPOLICY         (206)
#define PclPROXYSECURITYPOLICY    (207)
#define PclSECURITYUSED           (208)
#define PclMESSAGEAUDITINFO       (209)
/* <- CLAC-29222 */

/* CLAC-31559 --> */
#define PclSLOBRESPONSE           (215)
#define PclAltSLOBRESPONSE \
        (PclFlavor)(PclSLOBRESPONSE | SYSALTPCLINDICATOR)
/* <-- CLAC-31559 */

/*  CLAC-30554 --> */
#define PclSLOBDATASTART          (220)
#define PclAltSLOBDATASTART \
        (PclFlavor)(PclSLOBDATASTART | SYSALTPCLINDICATOR)
#define PclSLOBDATA               (221)
#define PclAltSLOBDATA \
        (PclFlavor)(PclSLOBDATA | SYSALTPCLINDICATOR)
#define PclSLOBDATAEND            (222)
#define PclAltSLOBDATAEND \
        (PclFlavor)(PclSLOBDATAEND | SYSALTPCLINDICATOR)
/* <-- CLAC-30554  */
/***************************************************************/
/*                     Needed for low CLI                      */
/***************************************************************/

/*=============================================================*/
/*   Purpose  To define the string representations for all     */
/*            parcels and define flavors.  It is always fixed  */
/*            length and is padded with blanks to make up the  */
/*            length.                                          */
/*=============================================================*/

#define  PclSREQ              "REQ             "
#define  PclSRUNSTARTUP       "RUNSTARTUP      "
#define  PclSDATA             "DATA            "
#define  PclSRESP             "RESP            "
#define  PclSKEEPRESP         "KEEPRESP        "
#define  PclSABORT            "ABORT           "
#define  PclSCANCEL           "CANCEL          "
#define  PclSSUCCESS          "SUCCESS         "
#define  PclSFAILURE          "FAILURE         "
#define  PclSERROR            "ERROR           "
#define  PclSRECORD           "RECORD          "
#define  PclSENDSTATEMENT     "ENDSTATEMENT    "
#define  PclSENDREQUEST       "ENDREQUEST      "
#define  PclSFMREQ            "FMREQ           "
#define  PclSFMRUNSTARTUP     "FMRUNSTARTUP    "
#define  PclSVALUE            "VALUE           "
#define  PclSNULLVALUE        "NULLVALUE       "
#define  PclSOK               "OK              "
#define  PclSFIELD            "FIELD           "
#define  PclSNULLFIELD        "NULLFIELD       "
#define  PclSTITLESTART       "TITLESTART      "
#define  PclSTITLEEND         "TITLEEND        "
#define  PclSFORMATSTART      "FORMATSTART     "
#define  PclSFORMATEND        "FORMATEND       "
#define  PclSSIZESTART        "SIZESTART       "
#define  PclSSIZEEND          "SIZEEND         "
#define  PclSSIZE             "SIZE            "
#define  PclSRECSTART         "RECSTART        "
#define  PclSRECEND           "RECEND          "
#define  PclSPROMPT           "PROMPT          "
#define  PclSENDPROMPT        "ENDPROMPT       "
#define  PclSREWIND           "REWIND          "
#define  PclSNOP              "NOP             "
#define  PclSWITH             "WITH            "
#define  PclSPOSITION         "POSITION        "
#define  PclSENDWITH          "ENDWITH         "
#define  PclSLOGON            "LOGON           "
#define  PclSLOGOFF           "LOGOFF          "
#define  PclSRUN              "RUN             "
#define  PclSRUNRESP          "RUNRESP         "
#define  PclSUCABORT          "UCABORT         "
#define  PclSHOSTSTART        "HOSTSTART       "
#define  PclSCONFIG           "CONFIG          "
#define  PclSCONFIGRESP       "CONFIGRESP      "
#define  PclSSTATUS           "STATUS          "
#define  PclSIFPSWITCH        "ITFSWITCH       "
#define  PclSPOSSTART         "POSSTART        "
#define  PclSPOSEND           "POSEND          "
#define  PclSBULKRESP         "BULKRESP        "
#define  PclSDATE             "DATE            "
#define  PclSROW              "ROW             "
#define  PclSHUTCREDBS        "HUTCREDBS       "
#define  PclSHUTDBLK          "HUTDBLK         "
#define  PclSHUTDELTBL        "HUTDELTBL       "
#define  PclSHUTINSROW        "HUTINSROW       "
#define  PclSHUTRBLK          "HUTRBLK         "
#define  PclSHUTSNDBLK        "HUTSNDBLK       "
#define  PclSHUTRELDBCLK      "HUTRELDBCLK     "
#define  PclSHUTNOP           "HUTNOP          "
#define  PclSHUTBLD           "HUTBLD          "
#define  PclSHUTBLDRSP        "HUTBLDRSP       "  /* obsolete  */
#define  PclSHUTGETDDT        "HUTGETDDT       "
#define  PclSHUTGETDDTRSP     "HUTGETDDTRSP    "
#define  PclSHUTIDX           "HUTIDX          "
#define  PclSHUTIDXRSP        "HUTIDXRSP       "
#define  PclSFIELDSTATUS      "FIELDSTATUS     "
#define  PclSINDICDATA        "INDICDATA       "
#define  PclSINDICREQ         "INDICREQ        "
#define  PclSSESSOPT          "SESSOPT         "
#define  PclSDATAINFO         "DATAINFO        "
#define  PclSIVRUNSTARTUP     "IVRUNSTARTUP    "

/* Added the following string representations for 2PC - H0_01, JAE  */
/* HN_01 - Change to upper case. */

#define  PclSASSIGNRSP        "ASSIGNRSP       "
#define  PclSSESSINFO         "SESSINFO        "
#define  PclSSESSINFORESP     "SESSINFORESP    "
#define  PclSVOTEREQUEST      "VOTEREQUEST     "
#define  PclSVOTETERM         "VOTETERM        "
#define  PclSFORGET           "YES/DONE        "
#define  PclS2PCCMMT          "2PCCOMMIT       "
#define  PclS2PCABRT          "2PCABORT        "

/* Added the following string representations for NDM - HN_00, JAE */
/* HN_01 - Change to upper case. */

#define  PclSCURSORHOST       "CURSORHOST      "
#define  PclSCURSORDBC        "CURSORDBC       "
#define  PclSFLAGGER          "FLAGGER         "
                                       /*-->DR61637 */
#define  PclSAltREQ           "AltREQ          "
#define  PclSAltRUNSTARTUP    "AltRUNSTARTUP   "
#define  PclSAltDATA          "AltDATA         "
#define  PclSAltCANCEL        "AltCANCEL       "
#define  PclSAltFMREQ         "AltFMREQ        "
#define  PclSAltFMRUNSTARTUP  "AltFMRUNSTARTUP "
#define  PclSAltNOFLAVOR      "AltNOFLAVOR     "
#define  PclSAltINDICDATA     "AltINDICDATA    "
#define  PclSAltINDICREQ      "AltINDICREQ     "
#define  PclSAltDATAINFO      "AltDATAINFO     "
#define  PclSAltOPTIONS       "AltOPTIONS      "
#define  PclSAltMULTITSR      "AltMULTITSR     "
#define  PclSAltSPOPTIONS     "AltSPOPTIONSTYPE"
#define  PclSAltCURSORHOST    "AltCURSORHOST   "
#define  PclSAltBIGRESP        "AltBIGRESP      "
#define  PclSAltBIGKEEPRESP    "AltBIGKEEPRESP  "
#define  PclSAltRESP           "AltRESP         "
#define  PclSAltKEEPRESP       "AltKEEPRESP     "

                                     /* DR61637<--*/

/*=============================================================*/
/* TYPE     PclStmtType                                        */
/*                                                             */
/*   Purpose  To define the data type for kinds of activities  */
/*            returned in the Ok and Success parcels.          */
/*                                                             */
/*=============================================================*/

typedef     unsigned short       PclStmtType; /* DR68511 */


/*=============================================================*/
/* Purpose  To express the kind of activity within the Ok      */
/*            and Success parcel.                              */
/*                                                             */
/* Notes    In case you hadn't noticed, UPDATE is different    */
/*          from UPDATE...RETRIEVING, and a COMMENT statement  */
/*          which sets the comment string is different from a  */
/*          COMMENT statement which returns this information.  */
/*          The guiding principle is that the recipient of an  */
/*          Ok or Success parcel should be able to tell what   */
/*          parcel sequence to expect next.                    */
/*                                                             */
/*          PclStmtNull is used as a bit of a placeholder in   */
/*          data structures where a PclStmtType value may      */
/*          appear, but is not set in the particular case.     */
/*                                                             */
/*=============================================================*/

#define  PclStmtNull      (PclStmtType)( 0)
#define  PclRetStmt       (PclStmtType)( 1)
#define  PclInsStmt       (PclStmtType)( 2)
#define  PclUpdStmt       (PclStmtType)( 3) /* UPDATE          */
#define  PclUpdRetStmt    (PclStmtType)( 4) /* UPDATE ...      */
                                            /* RETRIEVING      */
#define  PclDelStmt       (PclStmtType)( 5)
#define  PclCTStmt        (PclStmtType)( 6)
#define  PclModTabStmt    (PclStmtType)( 7)
#define  PclCVStmt        (PclStmtType)( 8)
#define  PclCMStmt        (PclStmtType)( 9)
#define  PclDropTabStmt   (PclStmtType)(10)
#define  PclDropViewStmt  (PclStmtType)(11)
#define  PclDropMacStmt   (PclStmtType)(12)
#define  PclDropIndStmt   (PclStmtType)(13)
#define  PclRenTabStmt    (PclStmtType)(14)
#define  PclRenViewStmt   (PclStmtType)(15)
#define  PclRenMacStmt    (PclStmtType)(16)
#define  PclCreIndStmt    (PclStmtType)(17)
#define  PclCDStmt        (PclStmtType)(18)
#define  PclCreUserStmt   (PclStmtType)(19)
#define  PclGrantStmt     (PclStmtType)(20)
#define  PclRevokeStmt    (PclStmtType)(21)
#define  PclGiveStmt      (PclStmtType)(22)
#define  PclDropDBStmt    (PclStmtType)(23)
#define  PclModDBStmt     (PclStmtType)(24)
#define  PclDatabaseStmt  (PclStmtType)(25)
#define  PclBTStmt        (PclStmtType)(26)
#define  PclETStmt        (PclStmtType)(27)
#define  PclAbortStmt     (PclStmtType)(28)
#define  PclNullStmt      (PclStmtType)(29)
#define  PclExecStmt      (PclStmtType)(30)
#define  PclCmntSetStmt   (PclStmtType)(31) /* COMMENT set     */
                                            /*  statement      */
#define  PclCmntGetStmt   (PclStmtType)(32) /* COMMENT return- */
                                            /*  ing statement  */
#define  PclEchoStmt      (PclStmtType)(33)
#define  PclRepViewStmt   (PclStmtType)(34)
#define  PclRepMacStmt    (PclStmtType)(35)
#define  PclCheckPtStmt   (PclStmtType)(36)
#define  PclDelJrnlStmt   (PclStmtType)(37)
#define  PclRollBackStmt  (PclStmtType)(38)
#define  PclRelLockStmt   (PclStmtType)(39)
#define  PclHUTConfigStmt (PclStmtType)(40)
#define  PclVCheckPtStmt  (PclStmtType)(41)
#define  PclDumpJrnlStmt  (PclStmtType)(42)
#define  PclDumpDBStmt    (PclStmtType)(43)
#define  PclRestoreStmt   (PclStmtType)(44)
#define  PclRollForwStmt  (PclStmtType)(45)
#define  PclDelDBStmt     (PclStmtType)(46)
#define  PclClearDumpStmt (PclStmtType)(47)
#define  PclSaveDumpStmt  (PclStmtType)(48)
#define  PclShowStmt      (PclStmtType)(49)
#define  PclHelpStmt      (PclStmtType)(50)
#define  PclBeginLoadStmt (PclStmtType)(51)
#define  PclChkPtLoadStmt (PclStmtType)(52)
#define  PclEndLoadStmt   (PclStmtType)(53)
#define  PclLinStmt       (PclStmtType)(54)

#define  PclGrantLogonStmt  (PclStmtType)(55) /* DCR4505 BEGIN */
#define  PclRevokeLogonStmt (PclStmtType)(56)
#define  PclBegAccLogStmt   (PclStmtType)(57)
#define  PclEndAccLogStmt   (PclStmtType)(58)
#define  PclCollStatStmt    (PclStmtType)(59)
#define  PclDropStatStmt    (PclStmtType)(60) /* DCR 4505  END */

#define  PclSetCollationStmt (PclStmtType)(61)/* DCR 5094      */

/* Begin CLAC-29160, missing definitions from DBS pclbody.h */
#define  PclBegEditStmt	     (PclStmtType)(62)
#define  PclEditStmt         (PclStmtType)(63)
#define  PclExecEditStmt     (PclStmtType)(64)
#define  PclEndEditStmt      (PclStmtType)(65)
/* End CLAC-29160 */

#define  PclReleaseMloadStmt (PclStmtType)(66)/* DR 17876      */

/* Added the following for 2PC - H0_01, JAE                    */

#define  PclEditDelStmt     (PclStmtType)(67)
#define  PclEditInsStmt     (PclStmtType)(68)
#define  PclEditUpdStmt     (PclStmtType)(69)
#define  Pcl2PCVoteReq      (PclStmtType)(76)
#define  Pcl2PCVoteTerm     (PclStmtType)(77)
#define  Pcl2PCCmmt         (PclStmtType)(78)
#define  Pcl2PCAbrt         (PclStmtType)(79)
#define  Pcl2PCForget       (PclStmtType)(80)
#define  PclRevalidateRefStmt (PclStmtType)(89) /* CLAC-29474 */
#define  PclCommitStmt      (PclStmtType)(90)   /* HN_00 */

/* Begin CLAC-29160, missing definitions from DBS pclbody.h */
#define  PclMonVConfig       (PclStmtType)(91) /* Monitor Virtual Config    */
#define  PclMonPConfig       (PclStmtType)(92) /* Monitor Physical Config   */
#define  PclMonVSummary      (PclStmtType)(93) /* Monitor Virtual Summary   */
#define  PclMonPSummary      (PclStmtType)(94) /* Monitor Physical Summary  */
#define  PclMonVRes          (PclStmtType)(95) /* Monitor Virtual Resource  */
#define  PclMonPRes          (PclStmtType)(96) /* Monitor Physical Resource */
/* End CLAC-29160 */

/*Begin DR41685*/
#define  PclCTrigStmt        (PclStmtType)(97)
#define  PclDTrigStmt        (PclStmtType)(98)
#define  PclRenTrigStmt      (PclStmtType)(99)
#define  PclRepTrigStmt      (PclStmtType)(100)
#define  PclAltTrigStmt      (PclStmtType)(101)
/*End DR41685*/

/* CLAC-29160, missing definition from DBS pclbody.h */
#define  PclRdlStmt          (PclStmtType)(102) /* Replication statement */

/* Begin DR48735 */
#define PclDropProcStmt      (PclStmtType)(103)
#define PclCreateProcStmt    (PclStmtType)(104)
#define PclCallStmt          (PclStmtType)(105)
#define PclRenProcStmt       (PclStmtType)(106)
#define PclRepProcStmt       (PclStmtType)(107) /* DR50139       */
/* End DR48735 */

/* CLAC-29160, missing definition from DBS pclbody.h */
#define PclSetAcct           (PclStmtType)(108) /* Set Session Account */

                                               /*->DR60508       */
/* Introduced through DBS DR 51806   */
#define PclHulStmt (109)                       /* Locking logger */

/* Introduced through DBS 50391      */
#define PclMonSql  (PclStmtType)(110)         /* Monitor Session */
#define PclMonVer  (PclStmtType)(111)         /* Monitor SQL     */

/*Introduced through DBS DR 55105    */
#define PclBeginDBQLStmt  (PclStmtType)(112)  /* Begin DBQL       */
#define PclEndDBQLStmt    (PclStmtType)(113)  /* End DBQL         */

/* Introduced through DBS DR 52715   */
#define PclCreRoleStmt    (PclStmtType)(114)  /* Create Role SQL   */
#define PclDrpRoleStmt    (PclStmtType)(115)  /* Drop Role SQL     */
#define PclGrantRoleStmt  (PclStmtType)(116)  /* Grant Role SQL    */
#define PclRevokeRoleStmt (PclStmtType)(117)  /* Revoke Role SQL   */
#define PclCreProfileStmt (PclStmtType)(118)  /* Create Profile SQL*/
#define PclModProfileStmt (PclStmtType)(119)  /* Modify Profile SQL*/
#define PclDrpProfileStmt (PclStmtType)(120)  /* Drop Profile SQL  */
#define PclSetRoleStmt    (PclStmtType)(121)  /* Set Role SQL      */

                                    /*--->  DR67892 stmt types UDF */
#define PclCreUDFStmt     (PclStmtType)(122) /* Create  UDF */
#define PclRplcUDFStmt    (PclStmtType)(123) /* Replace UDF */
#define PclDropUDFStmt    (PclStmtType)(124) /* Drop    UDF */
#define PclAlterUDFStmt   (PclStmtType)(125) /* Alter   UDF */
#define PclRenUDFStmt     (PclStmtType)(126) /* Rename  UDF */
                                    /*  DR67892 stmt types UDF <---*/

#define PclMrgMixedStmt      (PclStmtType)(127)
#define PclMrgUpdStmt        (PclStmtType)(128)
#define PclMrgInsStmt        (PclStmtType)(129)
                                               /*DR60508       <---*/

                                               /* DR62240--->      */

#define PclAlterProcStmt      (PclStmtType)(130) /* ALTER PROCEDURE SQL */
#define PclTDQMEnablEStmt     (PclStmtType)(131) /* Enable          */
#define PclTDQMStatsStmt      (PclStmtType)(132) /* Statistics      */
#define PclTDQMPerfGroupsStmt (PclStmtType)(133) /* get Perf Groups */

                                               /* DR62240       <---*/

/* DR85431 --> */
/* DR64870 -> User Defined Types */
#define PclCreUDTStmt    (PclStmtType)(134) /* Create UDT        */
#define PclDropUDTStmt   (PclStmtType)(135) /* Drop UDT          */
#define PclAlterUDTStmt  (PclStmtType)(136) /* Alter UDT         */
#define PclRplcUDTStmt   (PclStmtType)(137) /* Replace UDT       */
#define PclCreUDMStmt    (PclStmtType)(138) /* Create UDM        */
#define PclAlterUDMStmt  (PclStmtType)(139) /* Alter UDM         */
#define PclRplcUDMStmt   (PclStmtType)(140) /* Replace UDM       */
#define PclCreCastStmt   (PclStmtType)(141) /* Create Cast       */
#define PclRplcCastStmt  (PclStmtType)(142) /* Replace Cast      */
#define PclDropCastStmt  (PclStmtType)(143) /* Drop Cast         */
#define PclCreOrdStmt    (PclStmtType)(144) /* Create Ordering   */
#define PclRplcOrdStmt   (PclStmtType)(145) /* Replace Ordering  */
#define PclDropOrdStmt   (PclStmtType)(146) /* Drop Ordering     */
#define PclCreXFormStmt  (PclStmtType)(147) /* Create Transform  */
#define PclRplcXFormStmt (PclStmtType)(148) /* Replace Transform */
#define PclDropXFormStmt (PclStmtType)(149) /* Drop Transform    */
/* <- DR64870 */

/* DR68323 -> V2R6 External Routine Security - AUTHORIZATION */
#define PclCreAuthStmt    (PclStmtType)(150)       /* Create */
#define PclDrpAuthStmt    (PclStmtType)(151)       /* Drop   */
/* <- DR68323 */

/* DR68596 -> V2R6 Replication Services - REPLICATION GROUP */
#define PclCreRepGrpStmt  (PclStmtType)(152)      /* Create */
#define PclAltRepGrpStmt  (PclStmtType)(153)      /* Alter  */
#define PclDrpRepGrpStmt  (PclStmtType)(154)      /* Drop   */
/* <- DR68596 */
/* <-- DR85431 */

/* DR105446 -> */
#define PclTwmDelRqstChgStmt   (PclStmtType)(155)
#define PclTwmSummaryStmt      (PclStmtType)(156)
#define PclTwmDynRuleStmt      (PclStmtType)(157)
#define PclTwmDynObjStmt       (PclStmtType)(158)
#define PclTwmWdAssignmentStmt (PclStmtType)(159)
#define PclTwmDynBuildStmt     (PclStmtType)(160)
#define PclTwmListWdStmt       (PclStmtType)(161)
/* <- DR105446 */

/* DR92423 -> */
#define PclSetSesIsoLvlStmt  (PclStmtType)(162)  /* tx-isolation       */
/* <- DR92423 */

/* DR105446 -> */
#define PclInitIdxAnalysis   (PclStmtType)(163)  /* InitIndex Analysis */
#define PclRplcAuthStmt      (PclStmtType)(164)  /* Replace Auth Stmt  */
/* <- DR105446 */

/* DR103360 -> */
#define  PclSetQBandStmt     (PclStmtType)(165)  /* Query Banding      */
/* <- DR103360 */

/* DR105446 -> */
#define  PclLogArcOnStmt     (PclStmtType)(166)  /* Online Archieve    */
#define  PclLogArcOffStmt    (PclStmtType)(167)  /* Online Archieve    */
#define  PclMonQueryBandStmt (PclStmtType)(168)  /* Monitor Query Band */
#define  PclCreCorrStmt      (PclStmtType)(169)  /* Column Correlation */
#define  PclRepCorrStmt      (PclStmtType)(170)  /* Column Correlation */
#define  PclDrpCorrStmt      (PclStmtType)(171)  /* Column Correlation */
#define  PclAltCorrStmt      (PclStmtType)(172)  /* Column Correlation */
/* <- DR105446 */

/* DR111230 --> */
#define  PclUserEventControlStmt (PclStmtType)(173) /* User Event Cntl  */
#define  PclEventStatusStmt  (PclStmtType)(174)  /* Event Status        */
#define  PclMonAWTResStmt    (PclStmtType)(175)  /* Monitor AWT Res     */
#define  PclSPDynResultSet   (PclStmtType)(176)  /* SP Result Set       */
/* <-- DR111230 */

/* DR115327 --> */
#define  PclCreRepRulSetStmt (PclStmtType)(177)  /* Create Rep. RulSet  */
#define  PclRplRepRulSetStmt (PclStmtType)(178)  /* Replace Rep. RulSet */
#define  PclDrpRepRulSetStmt (PclStmtType)(179)  /* Drop Rep. RulSet    */
/* <-- DR115327 */

/* DR115330 --> */
#define PclCreUDOStmt (PclStmtType)(180)         /* Create Operator     */
#define PclRepUDOStmt (PclStmtType)(181)         /* Replace Operator    */
#define PclRenUDOStmt (PclStmtType)(182)         /* Rename Operator     */
#define PclDrpUDOStmt (PclStmtType)(183)         /* Drop Operator       */
/* <-- DR115330 */

/* DR119336 --> */
#define  PclGrantConnThrStmt  (PclStmtType)(184) /* Grant Conn. Through */
#define  PclRevokeConnThrStmt (PclStmtType)(185) /* Revoke Conn. Through*/
#define  PclCreateGLOPSetStmt (PclStmtType)(186) /* Create GLOP Set     */
#define  PclDropGLOPSetStmt   (PclStmtType)(187) /* Drop GLOP Set       */
/* <-- DR119336 */

/* DR121443 --> */
#define  PclCreConstraint     (PclStmtType)(188) /* Create Sec. Const.  */
#define  PclAltConstraint     (PclStmtType)(189) /* Alter Sec. Const.   */
#define  PclDrpConstraint     (PclStmtType)(190) /* Drop Sec. Const.    */
/* <-- DR121443 */

/* DR145672, CLAC-30608 --> */
#define  PclCreateIndexTypeStmt  (PclStmtType)(191) /* Create Ind type  */
#define  PclDropIndexTypeStmt    (PclStmtType)(192) /* Drop Index Type  */
#define  PclReplaceIndexTypeStmt (PclStmtType)(193) /* Replace Ind Type */
#define  PclAlterIndexStmt       (PclStmtType)(194) /* Alter Index      */
#define  PclCheckWorkloadForStmt (PclStmtType)(195) /* Chk Workload For */
#define  PclNoSplExportStmt      (PclStmtType)(196) /* Fexp No Spooling */
#define  PclCheckWorkloadEndStmt (PclStmtType)(197) /* Chk Workload End */
#define  PclFlushDBQLStmt        (PclStmtType)(198) /* Flush DBQL Cache */
#define  PclTDWMExpStmt          (PclStmtType)(199) /* TDWM Exception   */
#define  PclTDWMTestStmt         (PclStmtType)(200) /* TDWM Test        */
#define  PclMonTDWMResrStmt      (PclStmtType)(201) /* Monitor TDWM Res */
#define  PclMonWDResrStmt        (PclStmtType)(202) /* Monitor WD PM    */
#define  PclRegisterObj          (PclStmtType)(203) /* Register XML     */
#define  PclSetSessCalendarStmt  (PclStmtType)(204) /* Exp Business day */
/* <-- DR145672, CLAC-30608 */
/* CLAC-29283 --> */
#define  PclMonReqStmt          (PclStmtType)(205)  /* Monitor Request  */
#define  PclMrgDelStmt          (PclStmtType)(206)  /* Merge-Into Del   */
/* <--CLAC-29283 */
/* CLAC-29422 --> */
#define  PclBeginDBQCStmt       (PclStmtType)(207)  /* Begin Query Capt */
#define  PclEndDBQCStmt         (PclStmtType)(208)  /* End Query Capt   */
/* <-- CLAC-29422 */
/* CLAC-29578 --> */
#define PclShowXMLStmt          (PclStmtType)(209)  /* Show obj in XML  */
/* <-- CLAC-29578 */
/* CLAC-29854 --> */
#define PclGtwHostGroupStmt     (PclStmtType)(210)  /* Gtw Hostgroup Pro*/
#define PclProxyConfigStmt      (PclStmtType)(211)  /* ProxyConfig      */
#define PclSecureAttributeStmt  (PclStmtType)(212)  /* SecureAttribute  */
#define PclSetSessVeryHotStmt   (PclStmtType)(213)  /* VH FSG Cache     */
/* <-- CLAC-29854 */
/* CLAC-30608 --> */
#define PclUnitySQLStmt         (PclStmtType)(214)  /* Unity SQL Stmt   */
#define PclSetSessSearchUIFDbPath (PclStmtType)(215) /* Aster Support   */
#define PclCreateZoneStmt       (PclStmtType)(216)  /* Create Zone      */
#define PclAlterZoneStmt        (PclStmtType)(217)  /* Alter Zone       */
#define PclDropZoneStmt         (PclStmtType)(218)  /* Drop Zone        */
#define PclCreateSrvStmt        (PclStmtType)(219)  /* Create Fore Srv  */
#define PclAlterSrvStmt         (PclStmtType)(220)  /* Alter Fore Srv   */
#define PclDropSrvStmt          (PclStmtType)(221)  /* Drop Foreign Srv */
#define PclBeginIsoLoadStmt     (PclStmtType)(222)  /* Begin Iso Load   */
#define PclChkPointLoadStmt     (PclStmtType)(223)  /* Checkpt Iso Load */
#define PclEndIsoLoadStmt       (PclStmtType)(224)  /* End Iso Load     */
#define PclSetSessLoadStmt      (PclStmtType)(225)  /* Set Sess Load    */
#define PclGrantZoneStmt        (PclStmtType)(226)  /* Grant Zone       */
#define PclRevokeZoneStmt       (PclStmtType)(227)  /* Revoke Zone      */
/* <-- CLAC-30608 */
/* CLAC-32605 --> */
#define PclForViewpointStmt     (PclStmtType)(228)  /* For Viewpoint    */
#define PclSetSessJSONIgnoreErrors (PclStmtType)(229)  /* Set Sess JSON */
#define PclInitiateChkStmt      (PclStmtType)(230)  /* CheckJI Act Type */
/* <-- CLAC-32605 */
/* CLAC-33337 --> */
#define PclCreateMapStmt        (PclStmtType)(231)  /* Create Map       */
#define PclSetSessUPTStmt       (PclStmtType)(232)  /* Set Session UPT  */
#define PclDropMapStmt          (PclStmtType)(233)  /* Drop Map         */
#define PclGrantMapStmt         (PclStmtType)(234)  /* Grant Map        */
#define PclRevokeMapStmt        (PclStmtType)(235)  /* Revoke Map       */
#define PclSetSessDotNotationOnError (PclStmtType)(236) /* Sess Dot No. */
#define PclCreSchemaStmt        (PclStmtType)(237)  /* Create Schema    */
#define PclDropSchemaStmt       (PclStmtType)(238)  /* Drop Schema      */
#define PclSetSessUdbStmt       (PclStmtType)(239)  /* Debug Function   */
/* <-- CLAC-33337 */
/* CLAC-34036 --> */
#define PclSetFSAttr            (PclStmtType)(240)  /* Foreign Server   */
/* <-- CLAC-34036 */
/* CLIWS-6627 --> */
#define PclSetTransformStmt     (PclStmtType)(241)  /* Set Transform Group */
/* <-- CLIWS-6627 */
/* CLIWS-6720 --> */
#define PclCreAliasStmt         (PclStmtType)(242)  /* Create Alias     */
#define PclRepAliasStmt         (PclStmtType)(243)  /* Replace Alias    */
#define PclDrpAliasStmt         (PclStmtType)(244)  /* Drop Alias       */
/* <-- CLIWS-6720 */
/* CLIWS-6953 --> */
#define PclIncRestoreAllWriteStmt (PclStmtType)(245) /* Incremental Restore */
/* <-- CLIWS-6953 */
/* CLIWS-7052 --> */
#define PclExecFuncStmt         (PclStmtType)(246)  /* Execute Analytic Func */
/* <-- CLIWS-7052 */
/* CLIWS-7014 --> */
#define PclSetSessUDFSearchPath (PclStmtType)(247)  /* Set Sess UDFSearchPath*/
/* <-- CLIWS-7014 */
/* CLIWS-7256 --> */
#define PclFncMapRetStmt        (PclStmtType)(248)  /* Select with table     */
                                                    /* creation message info */
#define PclFncMapInsSelStmt     (PclStmtType)(249)  /* Insert with table     */
                                                    /* creation message info */
/* <-- CLIWS-7256 */
/* CLIWS-7544 --> */
#define PclExecFuncStmtNoARTOut (PclStmtType)(250)  /* Execute Analytic Func */
                                                    /* No ART table Output   */
                                               /* ART: Analytic Result Table */
/* <-- CLIWS-7544 */

/* CLIWS-7934 --> */
#define PclCreateStorageStmt    (PclStmtType)(253)  /* Create Storage        */
#define PclAlterStorageStmt     (PclStmtType)(254)  /* Alter Storage         */
#define PclDropStorageStmt      (PclStmtType)(255)  /* Drop Storage          */
#define PclGrantStorageStmt     (PclStmtType)(256)  /* Grant Storage         */
#define PclRevokeStorageStmt    (PclStmtType)(257)  /* Revoke Storage        */
/* <-- CLIWS-7934 */

/* CLIWS-7750 --> */
#define PclCreateCOGMapStmt     (PclStmtType)(258)  /* Create COG Map        */
#define PclDropCOGMapStmt       (PclStmtType)(259)  /* Drop COG Map          */
#define PclCreCOGGroupStmt      (PclStmtType)(260)  /* Create COG Group      */
#define PclDropCOGGroupStmt     (PclStmtType)(261)  /* Drop COG Group        */
#define PclModCOGGroupStmt      (PclStmtType)(262)  /* Modify COG Group      */
#define PclCreCOGPrfStmt        (PclStmtType)(263)  /* Create COG Profile    */
#define PclDropCOGPrfStmt       (PclStmtType)(264)  /* Drop COG Profile      */
#define PclModCOGPrfStmt        (PclStmtType)(265)  /* Modify COG Profile    */
#define PclResumeCOGPrfStmt     (PclStmtType)(266)  /* Resume COG Profile    */
#define PclSuspendCOGPrfStmt    (PclStmtType)(267)  /* Suspend COG Profile   */
#define PclSetSessCOGGroupStmt  (PclStmtType)(268)  /* Set Session COG Group */
/* <-- CLIWS-7750 */

/* CLIWS-7870 --> */
#define PclCreateSpoolMapStmt   (PclStmtType)(269)  /* Create Spool Map      */
#define PclEnableSpoolMapStmt   (PclStmtType)(270)  /* Enable Spool Map      */
#define PclDisableSpoolMapStmt  (PclStmtType)(271)  /* Disable Spool Map     */
#define PclDropSpoolMapStmt     (PclStmtType)(272)  /* Drop Spool Map        */
/* <-- CLIWS-7870 */

/* CLIWS-7750 --> */
#define PclGrantOnCOGGrpStmt    (PclStmtType)(273)  /* Grant on COG Group    */
#define PclRevokeOnCOGGrpStmt   (PclStmtType)(274)  /* Revoke on COG Group   */
/* <-- CLIWS-7750 */

/* CLIWS-8852 --> */
#define PclCreateDataLakeStmt    (PclStmtType)(275)  /* Create DataLake      */
#define PclAlterDataLakeStmt     (PclStmtType)(276)  /* Alter DataLake       */
#define PclDropDataLakeStmt      (PclStmtType)(277)  /* Drop DataLake        */
/* <-- CLIWS-8852 */

/* CLIWS-8996 --> */
#define PclReplaceDataLakeStmt   (PclStmtType)(278)  /* Replace DataLake     */
/* <-- CLIWS-8996 */

/*----------------------------------------------------------------------*/
/* It is expected ( particularly by BTEQ ) that the names added for     */
/* activity types will mirror the spelling that the DBS uses for the    */
/* macros they define in their PclBody.h header file. DBS developers    */
/* use DR 59863 to reserve the numbers and names. The only difference   */
/* between what is used in DBS' PclBody.h and CLI's parcel.h entries    */
/* should be in case. DBS uses all uppercase and CLI uses mixed case.   */
/*                                                                      */
/* For example... If DBS is adding PCLCREUDOSTMT, it will be expected   */
/* that CLI in turn will add PclCreUDOStmt.                             */
/*----------------------------------------------------------------------*/

/*=============================*/
/*  LAN message parcels only.  */
/*=============================*/

#define  PclASSIGN        (PclFlavor)(100)
#define  PclASSIGNRSP     (PclFlavor)(101)

/*====================*/
/* STRUCT PCLSUCCESS  */
/*====================*/

struct  PclSUCCESSType
{
    PclFlavor          PclKind;          /* 8                 */
    PclLength          Length;           /* 14 to 269         */
    PclWord            StatementNo;
    Pclchar            ActivityCount[4]; /* four byte integer */
    PclWord            WarningCode;
    PclWord            FieldCount ;
    PclWord            ActivityType;
    PclWord            WarningLength;
    Pclchar            WarningMsg[255];  /* 0 to 255 bytes    */
};

struct  PclAltSUCCESSType
{
    PclFlavor          PclKind;          /* 0x8008            */
    PclLength          Length;           /* 0                 */
    PclAltLength       AltLength;        /* varies            */
    PclWord            StatementNo;
    Pclchar            ActivityCount[4]; /* four byte integer */
    PclWord            WarningCode;
    PclWord            FieldCount ;
    PclWord            ActivityType;
    PclWord            WarningLength;
    Pclchar            WarningMsg[255];  /* 0 to 255 bytes    */
};

/*====================*/
/*   STRUCT PCLOk     */
/*====================*/

struct PclOkType
{
    PclFlavor          PclKind;          /* 17                */
    PclLength          Length;           /* 14 to 269         */
    PclWord            StatementNo;
    PclWord            FieldCount ;
    PclUInt32          ActivityCount;    /* DR42286, unsigned */
    PclWord            ActivityType;
    PclWord            WarningCode;
    PclWord            WarningLength;
    Pclchar            WarningMsg[255];  /* 0 to 255 bytes    */
};

struct PclAltOkType
{
    PclFlavor          PclKind;          /* 0x8011            */
    PclLength          Length;           /* 0                 */
    PclAltLength       AltLength;        /* varies            */
    PclWord            StatementNo;
    PclWord            FieldCount ;
    PclUInt32          ActivityCount;    /* DR42286, unsigned */
    PclWord            ActivityType;
    PclWord            WarningCode;
    PclWord            WarningLength;
    Pclchar            WarningMsg[255];  /* 0 to 255 bytes    */
};

/* DR58352 --> */

struct  PclResultSummaryType
{
    PclFlavor          PclKind;          /* 163               */
    PclLength          Length;           /* varies            */
    PclWord       	   StatementNo;
    PclUInt64	 	   ActivityCount;  /* unsigned 64-bit integer */
    PclWord            FieldCount;
    PclWord            ActivityType;
    Pclchar            Mode; /* R=Record, F=Field, I=Indicator, M=Multipart */
    Pclchar            reserved[9];	/* for future use */
};

struct  PclAltResultSummaryType
{
    PclFlavor          PclKind;          /* 0x80a3            */
    PclLength          Length;           /* 0                 */
    PclAltLength       AltLength;        /* varies            */
    PclWord       	   StatementNo;
    PclUInt64	 	   ActivityCount;  /* unsigned 64-bit integer */
    PclWord            FieldCount;
    PclWord            ActivityType;
    Pclchar            Mode; /* R=Record, F=Field, I=Indicator, M=Multipart */
    Pclchar            reserved[9];	/* for future use */
};

struct ResultSummaryExtension
{
#define WARN_MESSAGE          1
   PclWord                    ExtInfoID;   /* unsigned 16-bit integer */
   PclWord                    ExtInfoLength; /* unsigned 16-bit integer */
   PclWord                    ExtInfoBody;
};

struct ResultSummaryExtensionWarning
{
   PclWord                  code;
   char                     warning_msg[1];
};

/* <-- DR58352 */

/*====================*/
/* STRUCT PCLFAILURE  */
/*====================*/

struct PclFAILUREType
{
    PclFlavor          PclKind;          /*  9                */
    PclLength          PLength;          /* 13 to 267         */
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];         /* 1 to 255 bytes    */
};

struct PclAltFAILUREType
{
    PclFlavor          PclKind;          /* 0x8009            */
    PclLength          PLength;          /* 0                 */
    PclAltLength       AltLength;        /* varies            */
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];         /* 1 to 255 bytes    */
};

/*====================*/
/*   STRUCT PCLERROR  */
/*====================*/

struct PclERRORType
{
    PclFlavor          PclKind;          /* 49                */
    PclLength          PLength;          /* 13 to 267         */
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];         /* 1 to 255 bytes    */
};

struct PclAltERRORType
{
    PclFlavor          PclKind;          /* 0x8031            */
    PclLength          PLength;          /* 0                 */
    PclAltLength       AltLength;        /* varies            */
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];         /* 1 to 255 bytes    */
};

/*====================*/
/*   STRUCT PCLRUN    */
/*====================*/

struct PclRUNType
{
    PclFlavor          PclKind;          /* 38 */
    PclLength          Length;           /* 20 */
    Pclchar            Runstring[16];
};

/*====================*/
/*   STRUCT PCLAltRUN */
/*====================*/
                     /*--> DR61637 */
struct PclAltRUNType
{
    PclFlavor       PclKind;          /* 32812 */
    PclLength       Length;           /* 0 */
    PclAltLength    AltLength;        /* 24 */
    Pclchar         Runstring[16];
};
                   /* DR61637<---*/

/*====================*/
/*   STRUCT PCLASG    */
/*====================*/

struct PclASSIGNType
{
    PclFlavor       PclKind;          /* 100 */
    PclLength       Length;           /* 97  */
    Pclchar         username[32];   /* DR52058: To be increased to
                                       92 ? */
};

/*====================*/
/* STRUCT PCLASGRESP  */
/*====================*/

struct PclASSIGNRSPType
{
    PclFlavor       PclKind;         /* 101                    */
    PclLength       Length;          /* 98                     */
    Pclchar         PublicKey[8];    /* Public key is up to an */
                                     /* 8 digit number which   */
                                     /* is the encryption part */
                                     /* of the public key.     */
    Pclchar         SesCopAddr[32];  /* SesCopAddr is the net- */
                                     /* work address to connect*/
                                     /* to for the session     */
    Pclchar         PublicKeyN[32];

                   /* PublicKeyN is the value you mod the      */
                   /* encryption value by.  Also it informs the*/
                   /* user how many bytes to encrypt at one    */
                   /* time.  N should be a value large  enough */
                   /* to encrypt 32 bits of data at a time.    */
                   /* If the value is small,  say under 255,   */
                   /* this is the initial code which  does not */
                   /* require long math.  This will be fixed   */
                   /* later.   So encrypt 32 bits at a time    */
                   /* into 64 bits values.  If it is the       */
                   /* initial code, then use a nibble at a time*/
                   /* and  encrypt into a byte (8 bits).       */

    Pclchar         RelArray[6];     /* RelArray contains the  */
                                     /* release number string  */
    Pclchar         VerArray[14];    /* VerArray contains the  */
                                     /* version number string  */
    PclInt16        HostID;          /* HostID contains logical*/
                                     /* hostid to be returned  */
                                     /* in dbcarea             */
                                     /* 87Nov03 DR11006 DAA    */
};

/*===================*/
/*  STRUCT PCLRESP   */
/*===================*/

struct PclRespType
{
    PclFlavor       PclKind;          /* 4 */
    PclLength       Length;           /* 6 */
    PclInt16        MaxMsgSize;
};
                      /*--> DR57320 */
/*===================*/
/* STRUCT PCLBIGRESP */
/*===================*/
struct PclBigRespType

{
    PclFlavor       PclKind;          /* 153 or 154 */
    PclLength       Length;           /* 8          */
    PclUInt32       MaxMsgSize;
};
                      /* DR57320 <--*/

                       /*--> DR61637 */
/*======================*/
/* STRUCT PCLALTBIGRESP */
/*======================*/
struct PclAltBigRespType
{
    PclFlavor       PclKind;          /* 153 + 0x8000 */
    PclLength       Length;           /*  0           */
    PclAltLength    AltLength;        /* 12           */
    PclUInt32       MaxMsgSize;
};

/*======================*/
/* STRUCT PCLALTRESP */
/*======================*/
struct PclAltRespType
{
    PclFlavor       PclKind;          /* 153 + 0x8000 */
    PclLength       Length;           /*  0           */
    PclAltLength    AltLength;        /* 10           */
    PclUInt16       MaxMsgSize;
};
                      /* DR61637 <--*/

/*===================*/
/*   STRUCT PCLREQ   */
/*===================*/

struct PclReqType
{
    PclFlavor       PclKind;          /* 1 */
    PclLength       Length;           /* ?? */
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar         Body[SysMaxRequestSize];
    #else
    Pclchar         Body[1];
    #endif
    /* <--CLAC-34171 */
};

                                /*-->DR61637 */
/*===================*/
/*  STRUCT PCLAltREQ */
/*===================*/

struct PclAltReqType
{
    PclFlavor       PclKind;          /* 32769 */
    PclLength       Length;           /* 0     */
    PclAltLength    AltLength;        /* ??    */
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar         Body[SysAltMaxRequestSize];
    #else
    Pclchar         Body[1];
    #endif
    /* <--CLAC-34171 */
};
                              /* DR61637<--*/

/*******************************************************************
* Parcel Structures - DR48735, TDSP                                *
*                     PclSPOptionsType                             *
*                     PclMultiTsr                                  *
*  added for TDSP support                                          *
*******************************************************************/

struct PclSPOptionsType          /* DR48735, TDSP             */
{
    PclFlavor     Flavor;        /*  PCLSPOPTIONS		      */
    PclLength     Length;        /*  6	                      */
    Pclchar       SPPrintOption; /* 'P' - PRINT	              */
                                 /* 'N' - NOPRINT             */
    Pclchar       WithSPLText;   /* 'Y' - SPL Code needs encryption */
};

                                               /*-->DR61637   */
struct PclAltSPOptionsType
{
    PclFlavor     Flavor;        /*  PclAltSPOPTIONSTYPE      */
    PclLength     Length;        /*   0                       */
    PclAltLength  AltLength;     /*  10                       */
    Pclchar       SPPrintOption; /* 'P' - PRINT               */
                                 /* 'N' - NOPRINT             */
    Pclchar       WithSPLText;   /* 'Y' - SPL Code needs encryption   */
};
                                                /* DR61637<---*/


struct PclMultiTSR	       /* DR48735, TDSP			          */
{
    PclFlavor      Flavor;      /* New Parcel Flavor          */
    PclLength      Length;      /* Length of this structure	  */
    PclWord        SeqNum;      /* Sequence No. starting from 1 within */
                                /* the series of messages     */
    PclByte        IsLast;      /* 1 - Message is last		  */
                                /* 0 - Else			          */
};

                                               /*-->DR61637   */
struct PclAltMultiTSR
{
    PclFlavor      Flavor;      /* New Parcel Flavor          */
    PclLength      Length;      /* 0                  	      */
    PclAltLength   AltLength;   /* Length of this structure   */
    PclWord        SeqNum;      /* Sequence No. starting from 1 within */
                                /* the series of messages     */
    PclByte        IsLast;      /* 1 - Message is last        */
                                /* 0 - Else                   */
};
                                               /* DR61637<---*/

/*******************************************************************/
/*  HN_00 - The following defines the NDM Parcels - JAE 05AUG94    */
/*******************************************************************/

/*==================*/
/* STRUCT FLAGTYPE  */
/*==================*/

typedef struct PclFlagType
{
    Int16       Location;            /* Warning position                */
    Int16       Reason;              /* Warning number                  */
    Int16       MsgLength;           /* Length in bytes of text message */
    Pclchar     Msg[SysMaxAnsiMsg];  /* Message text                    */
} PclFlagType;

typedef struct PclFlaggerType
{
    PclFlavor                  Flavor;
    PclLength                  Length;
    PclFlagType                Flags[1];
} PclFlaggerType;

typedef struct CliFlaggerType
{
    PclFlagType                Flags[1];
} CliFlaggerType;

/*========================*/
/* STRUCT CURSORHOSTTYPE  */
/*========================*/

typedef struct PclCursorHostType
{
    PclFlavor                  Flavor;
    PclLength                  Length;
    PclInt16                   AMP;
    PclByte                    Row[8];
    PclByte                    Request[4];
} PclCursorHostType;

                               /*  DR61637 <--*/
typedef struct PclAltCursorHostType
{
    PclFlavor                  Flavor;
    PclLength                  Length;
    PclAltLength               AltLength;
    PclInt16                   AMP;
    PclByte                    Row[8];
    PclByte                    Request[4];
} PclAltCursorHostType;
                               /*--> DR61637  */

typedef struct CliCursorHostType
{
    PclInt16                   AMP;
    PclByte                    Row[8];
    PclByte                    Request[4];
} CliCursorHostType;

/*========================*/
/* STRUCT CURSORDBCTYPE  */
/*========================*/

typedef struct PclCursorDBCType
{
    PclFlavor                  Flavor;
    PclLength                  Length;
    PclInt16                   AMP;
    PclByte                    Row[8];
} PclCursorDBCType;

typedef struct CliCursorDBCType
{
    PclInt16                   AMP;
    PclByte                    Row[8];
} CliCursorDBCType;

/**********************/
/* End OF NDM Parcels */
/**********************/


/*******************************************************************/
/*  The following defines the 2PC Parcels - JAE 23Nov92            */
/*******************************************************************/

/*==================*/
/* STRUCT SESSOPT   */
/*==================*/

struct PclSessOptListType        /* HN_00 - NDM Contract Support   */
                                 /* Added ANSI_Tran and FIPS_Flag. */
{
    Pclchar            ANSI_Tran;
    Pclchar            TPCOption;
    Pclchar            FIPS_Flag;
    Pclchar            date_form;    /* DR40417 */
    Pclchar            ESS_flag;     /* DR58352 */
    Pclchar            utilityWorkload; /* DR126915 */
    Pclchar            Redrive;      /* DR145358, CLAC-29119 */
    Pclchar            extendedLoadUsage; /* CLAC-29283 */
    Pclchar            RFU_9;
    Pclchar            RFU_10;
};


struct PclSessOptType
{
    PclFlavor                   Flavor;
    PclLength                   Length;
    struct   PclSessOptListType List;
};

/**********************/
/* STRUCT VOTEREQ     */
/**********************/

typedef char PclIDType[SysMaxName];

struct PclForeignIDType
{
    PclInt16        Length;
    PclIDType       Name;
};

struct PclVoteReqType
{
    PclFlavor                  PclKind;
    PclLength                  Length;
    struct   PclForeignIDType  Coordinator;
    struct   PclForeignIDType  RunUnitID;
};

/**********************/
/* STRUCT VOTETERM    */
/**********************/

struct PclVoteTermType
{
    PclFlavor                  PclKind;
    PclLength                  Length;
};

/**********************/
/* STRUCT CMMT2PC     */
/**********************/

struct PclCmmt2PCType
{
    PclFlavor                  PclKind;
    PclLength                  Length;
};

/**********************/
/* STRUCT ABT2PC      */
/**********************/

struct PclAbrt2PCType
{
    PclFlavor                  PclKind;
    PclLength                  Length;
};

/*  END OF 2PC PARCELS */

/*=============================================================*/
/* 87Sep18 DAA DCR3780 we did not get this in the correct      */
/* version the first time, so let's try it again with feeling..*/
/* The following definitions are required to support the new   */
/*  options parcel and connect parcel, and the LSN functions   */
/*=============================================================*/


struct PclOptListType
{
    Pclchar            RequestMode;
    Pclchar            DBCFunction;
    Pclchar            SelectData;  /*DR57583*/
    Pclchar            CCS; /* DR69228 */
    Pclchar            APH_responses; /* DR68835 */
    Pclchar            ReturnStatementInfo; /* DR98026  */
    Pclchar            transformsOff;       /* DR123516 */
    Pclchar            MaxDecimalPrecision; /* DR98024  */
    Pclchar            AGKR_option;         /* DR98025  */
    Pclchar            DynamicResultSets;   /* DR102528 */
    PclByte            SPReturnResult;      /* DR102528 */
    Pclchar            periodAsStruct;      /* DR123516 */
    PclByte            columnInfo;          /* DR116354 */
    Pclchar            TrustedSessions;     /* DR116354 */
    Pclchar            multiStatementErrors;/* DR121202 */
    Pclchar            arrayTransformsOff;  /* DR145224 */
    Pclchar            xmlResponseFormat;   /* CLAC-28875 */
    Pclchar            tasmFastFailReq;     /* CLAC-29270, 30478 */
    Pclchar            Reserved1;           /* CLAC-29115 */
    Pclchar            Reserved2;           /* CLAC-29115 */
    Pclchar            largeRowOpt;         /* CLAC-29115 */
};


struct PclOptionsType
{
    PclFlavor                 Flavor;
    PclLength                 Length;
    struct     PclOptListType OptList;
};
                            /*--> DR61637 */
struct PclAltOptionsType
{
    PclFlavor                  Flavor;
    PclLength                  Length;
    PclAltLength               AltLength;
    struct     PclOptListType  OptList;
};
                            /* DR61637 <--*/


/***************************************************************/
/*                                                             */
/*  PrepInfo parcel definition                                 */
/*                                                             */
/***************************************************************/

struct CliPrepColInfoType
{
    PclInt16       DataType;
    PclUInt16      DataLen;   /* DR101116 */
    /* CLAC-29672 -->
    PclInt16       NameLen;           ****************************************
    Pclchar        Name[NameLen];     * Name, Format and Title have variable *
    PclInt16       FormatLen;         * lengths and are listed for reference *
    Pclchar        Format[FormatLen]; * only.                                *
    PclInt16       TitleLen;          ****************************************
    Pclchar        Title[TitleLen];
    <-- CLAC-29672 */
};

/* DR66590 ---> */
struct CliPrepColInfoTypeX
{
    PclInt16       DataType;
    PclInt64       DataLen;
    /* DR89769 --> */
/*  PclInt16       DataExtType;     */
    Pclchar        ColumnCharSetCode;
    Pclchar        ColumnExtInfo;
    /* <-- DR89769 */
    /* CLAC-29672 -->
    PclInt16       NameLen;           ****************************************
    Pclchar        Name[NameLen];     * Name, Format and Title have variable *
    PclInt16       FormatLen;         * lengths and are listed for reference *
    Pclchar        Format[FormatLen]; * only.                                *
    PclInt16       TitleLen;          ****************************************
    Pclchar        Title[TitleLen];
    <-- CLAC-29672 */
};
/* <--- DR66590 */


struct CliPrepInfoType
{
    Pclchar        CostEstimate[8]; /* CostEstimate looks like a */
                                    /* 8 byte float value type?? */
                                    /* How can this be since     */
                                    /* unix floats are 4 bytes?? */
                                    /* We will need to look into */
                                    /* this : This field is RFU  */
                                    /* so the type might change  */
    PclInt16       SummaryCount;
    PclInt16       ColumnCount ;

    /* The following field is commented out to show it has variable length.  */
    /* The size can be greater than or equal to 0 up to maximum parcel size. */

    /* Pclchar     ColInfo[N];                                               */
};

/* CLAC-29367 ->

      +------------------------+
      | Flavor                 |
      +------------------------+
      | Length                 |
      +------------------------+
      | CostEstimate           |
      +------------------------+
      | SummaryCount           |
      +--+---------------------+
         | ColumnCount         |         <------------+
         +--+------------------+                      |
            | DataType         |   <--+               | SummaryCount+1
            +------------------+      | ColumnCount   | repetitions
            | DataLen          |      | repetitions   |
            +------------------+      | (may be 0)    |
            | ColumnName       |      |               |
            +------------------+      |               |
            | ColumnFormat     |      |               |
            +------------------+      |               |
            | ColumnTitle      |   <--+  <------------+
            +------------------+

      This figure shows PrepInfo parcel structure.  More information about the
      PrepInfo and PrepInfoX can be found in "Chapter 9: Parcels" in "Teradata
      Call-Level Interface Reference for Network-Attached Systems".
      Note: The first two byte of ColumnName, ColumnFormat, and ColumnTitle
            are used for size in bytes of each data.  The following is an
            example in which the first two byte is 4.

                       0       2   3   4   5
                      +-------+---+---+---+---+
                      |   4   |'c'|'i'|'t'|'y'|
                      +-------+---+---+---+---+

<- CLAC-29367 */

struct PclCONNECTType
{
    PclFlavor       Flavor;
    PclLength       Length;
    Pclchar         PartitionName[16];
    PclInt32        LSN;
    PclInt16        Function;
    PclInt16        Pad2bytes;        /* to make 3b2 happy */
};


struct CliCONNECTType
{
    Pclchar            PartitionName[16];
    PclInt32           LSN;
    PclInt16           Function;
    PclInt16           Pad2bytes;        /* to make 3b2 happy */
};


#define  ASSOCLSN  2
#define  ALLOCLSN  1
#define  NOPLSN    0

struct PclLSNType
{
    PclFlavor       Flavor;
    PclLength       Length;
    PclInt32        LSN;
};


struct CliLSNType
{
    PclInt32           LSN;
};


struct  CliDInfoType   /* Internal part of the DataInfo parcel */
{
    PclWord            SQLType;
    PclWord            SQLLen;
};


struct  CliDataInfoType  /* To define a DataInfo parcel       */
{
    PclWord              FieldCount;
    struct CliDInfoType  InfoVar[300];
};


struct  CliEndStateType  /* To define the End Statement parcel*/
{
    PclWord            StatementNo;
};


struct  CliEndWithType   /* To define the EndWith parcel      */
{
    PclWord            WithId;
};


struct  CliFailureType   /* To define the Failure parcel       */
{
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];                /* 1 to 255 bytes  */
};


struct  CliErrorType   /* To define the Error parcel           */
{
    PclWord            StatementNo;
    PclInt16           Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[SysMaxErrMsg];
};


struct  CliFieldType   /* To define the Field parcel           */
{
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            Data[SysMaxParcelSize];
    #else
    Pclchar            Data[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliFMReqType   /* DBC/SQL Field Mode Request parcel    */
{
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            ReqText[SysMaxParcelSize];
    #else
    Pclchar            ReqText[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliHostStartType   /* To define the HostStartup Parcel */
{
    PclWord            StartType;
    PclWord            Scope;
};


struct  CliIndicDataType   /* To define Indicator Data parcel  */
{
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            Data[SysMaxParcelSize];
    #else
    Pclchar            Data[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliIndicReqType    /* To define the DBC/SQL Indicator  */
{                          /* record Mode Request parcel       */

    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            ReqText[SysMaxRequestSize];
    #else
    Pclchar            ReqText[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliKeepRespType    /* To define the KeepRespond parcel */
{
    PclWord            MaxMsgSize;
};


struct  CliNullValueType   /* To define a Null Value parcel    */
{
    PclWord            FieldNum;
};


struct  CliOkType          /* To define the Ok parcel          */
{
    PclWord            StatementNo;
    PclWord            FieldCount;
    PclUInt32          ActivityCount; /* DR42286, unsigned         */
    PclWord            ActivityType;
    PclWord            WarningCode;
    PclWord            WarningLength;
    Pclchar            WarningMsg[SysMaxErrMsg];
};


struct  CliPositionType   /* To define the Position parcel     */
{
    PclWord            ColumnNo;
};


struct  CliPromptType     /* To define the Prompt parcel       */
{
    PclWord            NumFields;
};


struct  CliRecordType     /* To define the Record parcel       */
{
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            Body[SysMaxParcelSize];
    #else
    Pclchar            Body[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliReqType        /* To define a DBC/SQL Record Mode   */
{                         /* Request parcel                    */

    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            ReqText[SysMaxRequestSize];
    #else
    Pclchar            ReqText[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliRespType       /* To define the Respond parcel      */
{

    PclWord            MaxMsgSize;
};


struct  CliSizeType       /* To define the Size parcel         */
{
    PclWord            MaxFldSize;
};

struct  CliSessOptListType
{
    Pclchar            AbortLevel;
    Pclchar            Mode2PC;
    Pclchar            RFU[8];
};

struct  CliSuccessType    /* To define the Success parcel       */
{
    PclWord            StatementNo;
/* DR88784 --> */
#if (defined(I370) && _O_BYTEALIGN)  /* DR107934 */
    PclUInt32          ActivityCount;
#else
    Pclchar            ActivityCount[4]; /* should be 4 byte Int   */
                                         /* however alignment prob */
#endif
/* <-- DR88784 */
    PclWord            WarningCode;
    PclWord            FieldCount ;
    PclWord            ActivityType;
    PclWord            WarningLength;
    Pclchar            WarningMsg[SysMaxErrMsg];
};

/* DR58352 --> */

struct  CliResultSummaryType    /* To define the Result Summary parcel       */
{
    PclWord       	   StatementNo;
/* DR88784 --> */
#if (defined(I370) && _O_BYTEALIGN)  /* DR107934 */
    PclUInt64          ActivityCount;
#else
    Pclchar            ActivityCount[8]; /* should be 8 byte Int   */
                                         /* however alignment prob */
#endif
/* <-- DR88784 */
    PclWord            FieldCount;
    PclWord            ActivityType;
    Pclchar            Mode; /* R=Record, F=Field, I=Indicator, M=Multipart */
    Pclchar            reserved[9];	/* for future use */
};

/* <-- DR58352 */

struct  CliValueType    /* To define the Value parcel           */
{
    PclWord            FieldNum;
    /* CLAC-34171 --> */
    #ifdef CLIV2_STRUCT_DEF_NO_DEPRECATE
    Pclchar            Data[SysMaxParcelSize-2];
    #else
    Pclchar            Data[1];
    #endif
    /* <--CLAC-34171 */
};


struct  CliWithType     /* To define the With parcel             */
{
    PclWord            WithId;
};


struct  ConnectBody
{
    Pclchar            PartName[16];
    PclInt32           LSN;
    PclInt16           Func;
    PclInt16           Pad2byte;    /* to make 3b2 happy */
};


/*===========================================================*/
/* 87Nov03 DAA DCR3273 Error Message Inserts support.  The   */
/*  following structures/definitions have been added for     */
/*  support of error message inserts                         */
/*===========================================================*/

/*===========================================================*/
/* The InsTriad structure will appear at the end of error    */
/* and failure parcels following the InsCount field, which   */
/* indicates how many triads follow the message text         */
/*===========================================================*/

struct InsTriad
{
    Pclchar            Kind;   /* Kind is the type of insert   */
                               /* valid types are defined below*/
    Pclchar            Offset; /* Offset is the offset from the*/
                               /* beginning of the message text*/
                               /* where the inserted text begin*/
    Pclchar            Size;   /* Size is the size of the      */
                               /* inserted text.               */
};


#define  PclErrIns_VSTR   0
#define  PclErrIns_FSTR   1
#define  PclErrIns_DBID   2
#define  PclErrIns_TVMID  3
#define  PclErrIns_FLDID  4

/*=============================================================*/
/*             Following added for DCR4029  -  WBJ             */
/*=============================================================*/

struct PclConfigType
{
    PclFlavor       Flavor;
    PclLength       Length;
};

/*=============================================================*/
/*  The following structures are nested within the variable    */
/*  appendages hung on the config response parcel. A word      */
/*  about their organization is in order.                      */
/*  The Config Response Parcel will look like:                 */
/*       [ Fixed Portion - thru ProcCount                   ]  */
/*       [ The number of IFP records specified by ProcCount ]  */
/*       [ A 16 bit value indicating the number of AMP recs ]  */
/*       [ The appropriate number of AMP records (AMPCount) ]  */
/*       [ 16 bits count of entries in char set             ]  */
/*       [ The set of character code to name map entries    ]  */
/*       [ InDoubt field indicating if InDoubt sessions exist] */
/*       [ 1 byte value indicating if extension area follows]  */
/*       [ Extension area (Features, Length, Tx_Semantic Def.] */
/*=============================================================*/

/*==================*/
/*  The IFP Record  */
/*==================*/

struct IFPrec
{
    PclInt16   IFPPhyProcNo;
    PclByte    IFPState;
    PclByte    Pad;
};

/*==================*/
/*  The AMP Record  */
/*==================*/

typedef   PclInt16   AMPArray;

/*==================================*/
/*  The Character Code to Name Map  */
/*==================================*/

#define FLTMAXTRANSLATENAME 30  /* length of char set names */

struct FltCharArrayNameCodeType
{
    PclByte    CharSetCode;
    PclByte    Pad;
    PclByte    CharSetName[FLTMAXTRANSLATENAME];
};

/*==================================*/
/*  In Doubt field of ConfigRsp     */
/*==================================*/

typedef   Pclchar    InDoubt;

/*==================================*/
/*  Extension Area of ConfigRsp     */
/*==================================*/

struct  PclCfgExtendType
{
    PclInt16           Feature;
    PclInt16           Length;
};

#define PCLCfgANSI 1          /* ANSI_Feature - 1 indicates NDM release */

struct  PclCfgNDMFeatureType
{
    PclInt16           ANSI_Feature;  /* Set to PCLCfgANSI for NDM release */
    PclInt16           ANSI_Length;
    Pclchar            ANSI_Tran;     /* 'A' ANSI, 'T' Teradata */
};

/*==============================================*/
/*  Finally  -  The config response parcel!!    */
/*==============================================*/

struct PclConfigRspType
{
    PclFlavor       Flavor;
    PclLength       Length;
    PclInt32        FirstSessNo;
    PclInt32        FirstReqNo;
    PclInt16        MaxSessCnt;
    PclInt16        ProcCount;  /* count of IFPs              */
    Pclchar         VarPart[1]; /* start of variable portion  */

     /*=======================================================*/
     /* Variable Part is organized as follows :               */
     /* [ The number of IFP records specified by ProcCount ]  */
     /* [ A 16 bit value indicating the number of AMP recs ]  */
     /* [ The specified number of AMP records (AMPCount)   ]  */
     /* [ 16 bits count of entries in char set             ]  */
     /* [ The set of character code to name map entries    ]  */
     /*                                                       */
     /* The types for the IFP records, AMP records and Char   */
     /* codes are defined above.                              */
     /*                                                       */
     /*=======================================================*/
     /* NDM Changes:                                          */
     /* For the NDM Query Environment Facility, this parcel   */
     /* is returned as a response to the LAN Request.  There  */
     /* is an extension to this parcel that contains other    */
     /* information following the character set entries.      */
     /* [ 1 byte "IN DOUBT" field.                         ]  */
     /* [ 1 byte indicating if more data follows this byte.]  */
     /* [ 2 byte Feature field (1 - NDM features exist). ]    */
     /* [ 2 byte length field.                             ]  */
     /* [ 1 byte Transaction Semantics default ('A' or 'T')]  */
     /*                                                       */
};

/*============================================================*/
/*                    End DCR4029 mods                        */
/*============================================================*/
/*#ifdef WIN32 */
/* SSO: start DR 52052 */
/*   Purpose  To define the Authorization Request Parcel */

typedef struct
{
    PclFlavor 	    PclKind;    /* PCLSSOAUTHREQ */
    PclLength_t 	PclLength;
} pclssoauthreq_t;

/*  Fields  PclKind is PCLSSOAUTHREQ.

            PclLength is the length of the parcel.

    MsgType Request.

    Notes   The PclSSOAuthReq parcel is used to request which
            authentication methods are available.
*/

/*   Purpose  To define the Authorization Response Parcel */

typedef struct
{
    PclFlavor  	    PclKind;    /* PCLSSOAUTHRESP */
    PclLength_t     PclLength;  /* DR68080: PclLength_t used */
    PclByte         NumberOfMethods;
    unsigned char   Methods[255];
} pclssoauthrsp_t;

/*  Fields  PclKind is PCLSSOAUTHRESP.

            PclLength is the length of the parcel.

            NumberOfMethods is the number of methods returned
            as an array or character Methods

            Methods is an array of supported methods for this platform.

    MsgType Request.

    Notes   The PclSSOAuthReq parcel is used to request which
            authentication methods are available.
*/

/*   Purpose  To define the SSOREQ Parcel */

typedef struct
{
    PclFlavor 	    PclKind;    /* PclSSOREQ */
    PclLength_t     PclLength;  /* DR68080: PclLength_t used */
    PclByte         Method;
    PclByte         Trip;
    PclUInt16       AuthDataLen;
    Pclchar         AuthData[1];  /* DR54273 */
} pclssoreq_t;

#define SIZEOF_PCLSSOREQ_T 8 /* DR52869: unix rounds up char[1] to a sizeof 2
                                causing a problem with parcel data size */
/*  Fields  PclKind is PclSSOREQ.

            PclLength is the length of the parcel.

            Method is the method to use for authentication which
            must be one of the values in PclSSOMethods_t and must
            be one of the values returned in the AuthMethods parcel.

            Trip is incremented each time a COPSSOREQ message is sent
            in either direction.

            AuthDataLen defines the length of the authentication
            data.

            AuthData represents the opaque authentication data
*/

/*   Purpose  To define the SSOREQ Parcel */

typedef struct
{
    PclFlavor 	    PclKind;    /* PclSSORESP */
    PclLength_t 	PclLength; /* DR68080: PclLength_t used */
    PclByte         Method;
    PclByte         Code;
    PclByte         Trip;
    PclByte         MustBeZero;
    PclUInt16       AuthDataLen;
    Pclchar         AuthData[1];	/* DR54273 */
} pclssorsp_t;

/*  Fields  PclKind is PclSSORESP.

            PclLength is the length of the parcel.

            Method is the method to use for authentication which
            must be one of the values in PclSSOMethods_t and must
            be one of the values returned in the AuthMethods parcel.

            Code is one of the two values SSOLOGONOK or
            SSOLOGONCONTINUED.  SSOLOGONFAILURE is used internally
            by the Gateway

            Trip is incremented each time a COPSSOREQ message is sent
            in either direction.

            AuthDataLen defines the length of the authentication
            data.

            AuthData represents the opaque authentication data
*/

/*   Purpose  To define the SSOREQ Parcel */

typedef struct
{
    PclFlavor 	    PclKind;    /* PCLUSERNAMEREQ */
    PclLength_t 	PclLength;  /* DR68080: PclLength_t used */
} pclusernamereq_t;

/* DR116354 --> */
typedef struct
{
    PclFlavor       PclKind;    /* PCLUSERNAMEREQ */
    PclLength_t     PclLength;  /* Parcel length  */
    PclByte         EONResponse;
} pclusernamereqEON_t;
/* <-- DR116354 */

/*  Fields  PclKind is PCLUSERNAMEREQ. */

/*  Purpose     To define an enumeration value for each authentication
                method that is supported by Teradata Software
*/

#ifdef CLI_MF_GTW          /* CLIWS-7751 */
#pragma enum(4)
#endif  /* CLI_MF_GTW */

typedef enum
{
    AuthNotSpecified = 0,      /* DR68842: Used for 6.0 tdgss */
    /* AuthWinNegotiate = 1, *//* DR68842: no longer offered */
    AuthWinNTLM      = 2,
    AuthWinKerberos  = 3,
    AuthTeradata     = 8       /* DR52869 */
} PclSSOMethods_t;

#ifdef CLI_MF_GTW          /* CLIWS-7751 */
#pragma enum(reset)
#endif  /* CLI_MF_GTW */


/*  The following defines are used for translating the
//  Package Name returned by the security system to the
//  enumerated methods defined above.
*/

#define AUTHNOTSPECIFIEDSTR ""
#define AUTHWINNEGOTIATESTR "Negotiate"  /* DR68842: backwards compatibility? */
#define AUTHWINNTLMSTR      "NTLM"
#define AUTHWINKERBEROSSTR  "Kerberos"
#define AUTHTERADATASTR     "TERADATA"   /* DR52869-dhv-01 */

/* SSO: end DR 52052 */
/*#endif*/


/*============================================================*/
/*            The following was added for DR68842             */
/*============================================================*/

/* Purpose  To define the type of gateway configuration response
            parcel extension data present in gateway configuration
            response parcel.

   Fields   pclgtwextnone means there is no configuration response
            information present in the pclgtwconfigrsp_t parcel.
            For pre-V2R6 gateway this is always the definition.

            pclgtwextselfdef means that the pclgtwconfigrsp_t parcel
            contains self-defining structures. The structures always
            appear after the pclgtwconfigextend_t in the parcel. The
            self-defining structures have the following general
            format:

               Feature  :  pclgtwconfigfeature_t;
               Length   :  pclgtwconfigfeaturelen_t;
               ** Misc data fields for feature
*/

/*  Purpose     To define the self-defining gateway feature number */
/*              As defined for DBS configuration parcel.           */
typedef unsigned short pclgtwconfigfeature_t, pclcliconfigfeature_t;

/* Purpose    To define the length of the self-defining feature of a */
/*            gateway configuration response parcel.                 */
typedef unsigned short pclgtwconfigfeaturelen_t, pclcliconfigfeaturelen_t;

#ifdef CLI_MF_GTW          /* CLIWS-7751 */
#pragma enum(4)
#endif  /* CLI_MF_GTW */

typedef enum
{
    pclgtwextnone = 0,
    pclgtwextselfdef = 1
} pclgtwconfigextend_t, pclcliconfigextend_t;

#ifdef CLI_MF_GTW          /* CLIWS-7751 */
#pragma enum(reset)
#endif  /* CLI_MF_GTW */

/* pclclientconfig_t and pclgtwconfig_t are the same structures. We use dif- */
/* ferent variable names for config extensions for sanity purposes and ease  */
/* of debugging. */
typedef struct
{
    PclFlavor             PclKind;        /* PclCLIENTCONFIG */
    PclLength_t           PclLength;
    pclcliconfigextend_t  ClientConfigExt;
} pclclientconfig_t;

typedef struct
{
    PclFlavor             PclKind;        /* PclGTWCONFIG */
    PclLength_t           PclLength;
    pclgtwconfigextend_t  GtwConfigExt;
} pclgtwconfig_t;

/* Gateway self-defining element (SDE) values */
#define PCLGTWCONFIGSSO                 1 /* SSO */
#define PCLGTWCONFIGTDGSS               2 /* GSS */
#define PCLGTWCONFIGUTF                 3 /* DR94976: Unicode */
#define PCLGTWCONFIGNODEID              4 /* DR113468: Session ID */
#define PCLGTWCONFIGRECOVERABLEPROTOCOL 5 /* DR145358, CLAC-29119 -> */
#define PCLGTWCONFIGCONTROLDATA         6
#define PCLGTWCONFIGREDRIVE             7 /* <- DR145358, CLAC-29119 */
#define PCLGTWCONFIGSECURITYPOLICY      10 /* CLAC-29222 */
#define PCLGTWCONFIGNEGOMECH            12 /* CLAC-31283 */
#define PCLGTWCONFIGIDP                 15 /* CLIWS-7948 */
#define PCLGTWCONFIGPORTS               16 /* CLIWS-7780 */
#define PCLGTWCONFIGIDPSCOPE            17 /* CLIWS-8666 */

/* Client self-defining element values */
#define PCLCLICONFIGVERSION 1 /* DR101074: client config version */
#define PCLCLICONFIGTDGSS   2 /* DR96966: GSS. featurelen for this feature */
                              /* should be 0 (due to original 4013 error   */
                              /* (and JDBC).                               */
#define PCLCLICONFIGRECOVERABLEPROTOCOL 3 /* DR145358, CLAC-29119 -> */
#define PCLCLICONFIGCONTROLDATA         4
#define PCLCLICONFIGREDRIVE             5 /* <- DR145358, CLAC-29119 */
#define PCLCLICONFIGSECURITYPOLICY      8 /* CLAC-29222 */
#define PCLCLICONFIGESS                 9 /* CLAC-28857: ESS */
#define PCLCLICONFIGNEGOMECH           11 /* CLAC-31283 */
#define PCLCLICONFIGIDP                14 /* CLIWS-7948 */
#define PCLCLICONFIGTLS                15 /* CLIWS-7647 */
#define PCLCLICONFIGIDPSCOPE           16 /* CLIWS-8666 */

#define CLICONFIGVERSIONLEN 12
#define CLICONFIGTDGSSLEN   4

typedef struct cli_config_ext_version_s /* DR101074 */
{
    pclcliconfigfeature_t    feature;   /* PCLCLICONFIGVERSION */
    pclcliconfigfeaturelen_t length;    /* CLICONFIGVERSIONLEN */
    unsigned char            value[CLICONFIGVERSIONLEN];
} cli_config_ext_version_t;

typedef struct cli_config_ext_tdgss_s   /* DR96966, DR101074 */
{
    pclcliconfigfeature_t    feature;   /* PCLCLICONFIGTDGSS */
    pclcliconfigfeaturelen_t length;    /* CLICONFIGTDGSSLEN */
    unsigned char            value[CLICONFIGTDGSSLEN];
} cli_config_ext_tdgss_t;

/* DR145358, CLAC-29119 -> */
typedef struct cli_config_ext_recoverableprotocol_s
{
    pclcliconfigfeature_t    feature;   /* PCLCLICONFIGRECOVERABLEPROTOCOL */
    pclcliconfigfeaturelen_t length;    /* 0 */
} cli_config_ext_recoverableprotocol_t;

typedef struct cli_config_ext_controldata_s
{
    pclcliconfigfeature_t    feature;   /* PCLCLICONFIGCONTROLDATA */
    pclcliconfigfeaturelen_t length;    /* 0 */
} cli_config_ext_controldata_t;

typedef struct cli_config_ext_redrive_s
{
    pclcliconfigfeature_t    feature;   /* PCLCLICONFIGREDRIVE */
    pclcliconfigfeaturelen_t length;    /* 0 */
} cli_config_ext_redrive_t;
/* <- DR145358, CLAC-29119 */

typedef struct cli_config_ext_ess_s       /* CLAC-28857 */
{
    pclcliconfigfeature_t    ESS_Feature; /* PCLCLICONFIGESS */
    pclcliconfigfeaturelen_t ESS_Length;  /* 1 */
    unsigned char            ESS_flag;    /* 0 */
} cli_config_ext_ess_t;

typedef struct cli_config_ext_securitypolicy_s /* CLAC-29222 */
{
    pclcliconfigfeature_t    feature; /* PCLCLICONFIGSECURITYPOLICY */
    pclcliconfigfeaturelen_t length;  /* 1 */
    unsigned char            level;   /* 1 */
} cli_config_ext_securitypolicy_t;

typedef struct cli_config_ext_negomech_s /* CLAC-31283 */
{
    pclcliconfigfeature_t    feature; /* PCLCLICONFIGNEGOMECH */
    pclcliconfigfeaturelen_t length;  /* 1 */
    unsigned char            level;   /* 1 */
} cli_config_ext_negomech_t;

typedef struct cli_config_ext_tls_s   /* CLIWS-7647 */
{
    pclcliconfigfeature_t    feature; /* PCLCLICONFIGTLS */
    pclcliconfigfeaturelen_t length;  /* 0 */
} cli_config_ext_tls_t;

typedef struct cli_config_ext_idp_s   /* CLIWS-7948 */
{
    pclcliconfigfeature_t    feature; /* PCLCLICONFIGIDP */
    pclcliconfigfeaturelen_t length;  /* 0 */
} cli_config_ext_idp_t;


/* Purpose    To define the legal values for the SSO_Setting field of */
/*            pclgtwconfig parcel.                                    */
#define       SSO_ON    0x01
#define       SSO_OFF   0x02
#define       SSO_ONLY  0x03

/* Fields     SSO_Feature must be set to PCLGTWCONFIGSSO.*/
/*            SSO_Length must be set to 5.               */
/*            SSO_Setting is defined above.              */
typedef struct
{
    pclgtwconfigfeature_t    SSO_Feature;
    pclgtwconfigfeaturelen_t SSO_Length;
    unsigned char            SSO_Setting;
} pclgtwconfigsso_t;

/* Fields     TDGSS_Feature (PCLGTWCONFIGTDGSS, PCLGTWCONFIGUTF)          */
/*            TDGSS_Length                                                */
typedef struct
{
    pclgtwconfigfeature_t    TDGSS_Feature;
    pclgtwconfigfeaturelen_t TDGSS_Length;
}  pclgtwconfigtdgss_t;

/* DR113468 --> */
/* Fields     UID_Feature must be set to PCLGTWCONFIGNODEID */
/*            UID_Length must be set to 6.                  */
/*            UID_Setting is defined above.                 */
typedef struct
{
    pclgtwconfigfeature_t    UID_Feature;
    pclgtwconfigfeaturelen_t UID_Length;
    unsigned short           UID_Setting;
} pclgtwconfiguid_t;
/* <-- DR113468 */

/* DR145358, CLAC-29119 -> */
typedef struct
{
    pclgtwconfigfeature_t    RECOVERABLEPROTOCOL_Feature;
                                          /* PCLGTWCONFIGRECOVERABLEPROTOCOL */
    pclgtwconfigfeaturelen_t RECOVERABLEPROTOCOL_Length;  /* 4 */
} pclgtwconfigrecoverableprotocol_t;

typedef struct
{
    pclgtwconfigfeature_t    CONTROLDATA_Feature; /* PCLGTWCONFIGCONTROLDATA */
    pclgtwconfigfeaturelen_t CONTROLDATA_Length;  /* 4 */
} pclgtwconfigcontroldata_t;

typedef struct
{
    pclgtwconfigfeature_t    REDRIVE_Feature; /* PCLGTWCONFIGREDRIVE */
    pclgtwconfigfeaturelen_t REDRIVE_Length;  /* 4 */
} pclgtwconfigredrive_t;
/* <- DR145358, CLAC-29119 */

/* CLAC-29222 -> */
typedef struct
{
    pclgtwconfigfeature_t     SECURITYPOLICY_Feature;
    /* PCLGTWCONFIGSECURITYPOLICY */
    pclgtwconfigfeaturelen_t  SECURITYPOLICY_Length;  /* 5 */
    unsigned char             SECURITYPOLICY_Level;   /* 1 */
} pclgtwconfigsecuritypolicy_t;

/* CLIWS-7780: Gateway port configuration */
typedef struct
{
    pclgtwconfigfeature_t     PORTS_Feature;     /* PCLGTWCONFIGPORTS */
    pclgtwconfigfeaturelen_t  PORTS_Length;      /* 6 */
    unsigned char             PORTS_Legacy;      /* 0 - disabled or 1 - enabled */
    unsigned char             PORTS_TLS;         /* 0 - disabled or 1 - enabled */
} pclgtwconfigports_t;

/* CLIWS-7948: Gateway IdP configuration */
typedef struct
{
    pclgtwconfigfeature_t     IDP_Feature;            /* PCLGTWCONFIGIDP */
    pclgtwconfigfeaturelen_t  IDP_Length;             /* 4 + 2 + 2 + IDP_ClientID_Length + IDP_MetadataURL_Length */
    unsigned short            IDP_ClientID_Length;
    char*                     IDP_ClientID;           /* ClientID */
} pclgtwconfigidp_t;

/* CLIWS-7948: Gateway IdP metadata URL */
/* Note: pclgtwconfigidpurl_t immediately follows pclgtwconfigidp_t in the parcel */
typedef struct
{
    unsigned short IDP_MetadataURL_Length;
    char*          IDP_MetadataURL;        /* MetadataURL */
} pclgtwconfigidpurl_t;

/* CLIWS-8666: Gateway OIDC scope parameter */
typedef struct
{
    pclgtwconfigfeature_t     IDP_Feature;            /* PCLGTWCONFIGIDPSCOPE */
    pclgtwconfigfeaturelen_t  IDP_Length;             /* 4 + 2 + IDP_Scope_Length */
    unsigned short            IDP_Scope_Length;
    char*                     IDP_Scope;
} pclgtwconfigidpscope_t;

/* Security Policy parcel */
#ifdef CLI_MF_GTW          /* CLIWS-7751 */
#pragma enum(4)
#endif  /* CLI_MF_GTW */

typedef enum
{
    pclsecpolicyextnone    = 0,
    pclsecpolicyextselfdef = 1
} pclsecpolicyextend_t;

#ifdef CLI_MF_GTW          /* CLIWS-7751 */
#pragma enum(reset)
#endif  /* CLI_MF_GTW */

typedef struct
{
    PclFlavor            PclKind; /* PCLSECURITYPOLICY, PCLSECURITYUSED */
    PclLength            Length;
    PclByte              securityRequired;
    PclByte              confidentialityRequired;
    PclByte              pad1[2];
    PclUInt32            securityLevel;
    PclByte              pad2[4];
    pclsecpolicyextend_t SecPolicyExt;
} pclsecuritypolicy_t;
/* <- CLAC-29222 */

/* PclKind       - Must be PCLAUTHMECH     .                                */
/* MechOidLength - Length of the authentication method Oid.                 */
/* MechOid       - Oid of the authentication method. In the structure below */
/*             the MechOid is a string defined as MechOidLength bytes long. */
typedef struct
{
    PclFlavor            PclKind;               /* PCLAUTHMECH */
    PclLength_t          PclLength;
    pclgtwconfigextend_t MechConfigExt;
    unsigned int         MechOidLength;
    unsigned char        MechOid[1];
    /* Followed by config extensions if MechConfigExt == pclgtwextselfdef */
} pclauthmech_t;

/*============================================================*/
/*                    End of DR68842 mods                     */
/*============================================================*/

/* CLAC-31283 --> */
/* Fields     NEGOMECH_Feature must be set to PCLGTWCONFIGNEGOMECH */
/*            NEGOMECH_Length must be set to 5.                    */
/*            NEGOMECH_Level is defined above.                     */
typedef struct
{
    pclgtwconfigfeature_t    NEGOMECH_Feature; /* PCLGTWCONFIGNEGOMECH */
    pclgtwconfigfeaturelen_t NEGOMECH_Length;  /* 5 */
    unsigned char            NEGOMECH_Level;   /* 1 */
} pclgtwconfignegomech_t;
/* <-- CLAC-31283 */

/*  DR 98026 -->  */
/*=============================*/
/* StatementInfo Parcel Types  */
/*=============================*/
typedef struct
{
    PclFlavor                 Flavor;
    PclLength                 Length;
    PclByte                   MetaData;  /* see below */
}
pclstatementinfo_t, PclStatementInfo_t;

typedef struct
{
    PclFlavor                 Flavor;
    PclLength                 Length;
    PclAltLength              AltLength;
    PclByte                   MetaData;  /* see below */
}
pclextstatementinfo_t, PclExtStatementInfo_t;
/* <-- DR 98026  */


/* DR57583 LOBs */

/*  DataInfoX request/response parcel */

struct FieldType
{
    PclUInt16 data_type;       /* DR68105 */
    union
    {
        /* non-numeric */
        Pclchar length_nonnumeric[8];

        /* numeric */
        struct _numeric
        {
            PclUInt32 num_digits;
            PclUInt32 num_fractional;
        } s;
    } u;
};

struct  PclXDIXType
{
    PclFlavor            PclKind;
    PclLength            Length;
    PclUInt32            FieldCount;
    struct FieldType     FieldEntries[1];  /* variable number of entries */
};

/* DR61637 APH version of Datainfox parcel */

struct PclAltXDIXTType
{
    PclFlavor            PclKind;
    PclLength            Length;
    PclAltLength         AltLength;
    PclUInt32            FieldCount;
    struct FieldType     FieldEntries[1];  /* variable number of entries */
};

struct PclSEDType
{
    PclFlavor            PclKind;
    PclLength            Length;
    PclUInt32            token;
};

/* DR85431 --> */

struct CliSEFType
{
    Pclchar              SourceLanguage;
    Pclchar              FileType;
    Pclchar              FileSupplied;
    Pclchar              pad;
    PclUInt16            FileLength;
    Pclchar              FileStart[1];   /* variable size */
};

struct PclSEFType
{
    PclFlavor            PclKind;
    PclLength            Length;
    struct CliSEFType    EFType;
};
/* <-- DR85431 */

/*=============================*/
/* DR60695 Cursor Positioning  */
/*       Parcel Types          */
/*=============================*/


struct PclSetPositionType

{
    PclFlavor       Flavor;          /* 157*/
    PclLength       Length;           /* 4  */
};

struct PclRowPositionType
{
    PclFlavor         Flavor;          /* 158 */
    PclLength         Length;           /*  16 */
    PclUInt16         statmnt_no;
    PclUInt16         dummy;
    PclUInt64         rownum;           /*unsigned 64 bit integer*/

};

struct PclOffsetPositionType
{
    PclFlavor          Flavor;          /* 159 */
    PclLength          Length;           /*  16 */
    PclUInt16          statmnt_no;
    PclUInt16          dummy;
    PclUInt64          offsetaddress;    /*unsigned 64 bit integer*/
};

struct PclAltSetPositionType

{
    PclFlavor       Flavor;       /* 0x8000 + 157	  */
    PclLength       Length;       /* 0    	      */
    PclAltLength    AltLength;    /* 8              */
};

struct PclAltRowPositionType
{
    PclFlavor         Flavor;       /* 0x8000 + 158	  */
    PclLength         Length;       /* 0    	      */
    PclAltLength      AltLength;    /* 20             */
    PclUInt16         statmnt_no;
    PclUInt16         dummy;
    PclUInt64         rownum;           /*unsigned 64 bit integer*/
};

struct PclAltOffsetPositionType
{
    PclFlavor          Flavor;       /* 0x8000 + 159	  */
    PclLength          Length;       /* 0    	      */
    PclAltLength       AltLength;    /* 20             */
    PclUInt16          statmnt_no;
    PclUInt16          dummy;
    PclUInt64          offsetaddress;    /*unsigned 64 bit integer*/
};

/*  <-- DR 68259  */

/* DR 68835 --> */

struct PclErrorInfoType
{
    PclFlavor          Flavor;       /* 164	  */
    PclLength          Length;       /* variable */
/* The following section may repeat as many times as will fit
   based on the parcel length (Length)                        */
    PclUInt16          info_type;
    PclUInt16          info_length;
    Pclchar            error_info[1];  /* variable-length field */
};

struct PclAltErrorInfoType
{
    PclFlavor          Flavor;       /* 164	  */
    PclLength          Length;       /* 0 */
    PclAltLength       AltLength;    /* variable */
/* The following section may repeat as many times as will fit
   based on the parcel length (AltLength)                     */
    PclUInt16          info_type;
    PclUInt16          info_length;
    Pclchar            error_info[1];  /* variable-length field */
};

typedef struct
{
    PclFlavor 	       PclKind;    /* PclUSERNAMERESP */
    PclLength_t        PclLength;
    unsigned char      ActualUserName[1]; /* variable length field */
} pclusernameresp_t; /* DR101287 */

#define PCLBUFFERSIZE 1 /* actual buffer size in case of >64KB message overflow.
        The info, i.e., the actual data is a 4byte unsigned integer value     */

/* <-- DR 68835 */

/* DR98026, Statment Info Feature -> */
/*--------------------------------------------------------------------*
*                                                                     *
*        STATEMENT-INFORMATION RESPONSE,                              *
*                            END-INFORMATION (PARAMETER,              *
*                                             QUERY,                  *
*                                             SUMMARY,                *
*                                             IDENTITY-COLUMN,        *
*                                             STORED-PROCEDURE-OUTPUT,*
*                                             STORED-PROCEDURE-ANSWER,*
*                                             ESTIMATED-PROCESSING)   *
*                                                                     *
*--------------------------------------------------------------------*/

struct TRDSATE
  {
  PclByte pbtieih[6];     /* INFORMATION HEADER                  */
#define PBTIEL 6          /* LENGTH OF INFORMATION               */
  };

#define PBTIEIH                         pbtieih


/*--------------------------------------------------------------------*
*                                                                     *
*        STATEMENT-INFORMATION RESPONSE,                              *
*                            FULL-LAYOUT (PARAMETER,                  *
*                                         QUERY,                      *
*                                         SUMMARY,                    *
*                                         IDENTITY-COLUMN,            *
*                                         STORED-PROCEDURE-OUTPUT,    *
*                                         STORED-PROCEDURE-ANSWER)    *
*                                                                     *
*                            IF PBROFN=P/S/B                          *
*                                                                     *
*--------------------------------------------------------------------*/

struct TRDSATF
  {
  struct TRDSATE info_h;     /* INFORMATION HEADER                   */
  PclUInt16 pbtifdbl;        /* DATABASE-NAME LENGTH                 */
  Pclchar pbtifdb[1];        /* DATABASE-NAME (var-length)           */
  PclUInt16 pbtiftbl;        /* TABLE/PROCEDURE-NAME LENGTH          */
  Pclchar pbtiftb[1];        /* TABLE/PROCEDURE-NAME (var-length)    */
  PclUInt16 pbtifcnl;        /* COLUMN/UDF/UDM/SP-PARM-NAME LENGTH   */
  Pclchar pbtifcn[1];        /* COLUMN/UDF/UDM/SP-PARM-NAME (var-length) */
  PclUInt16 pbtifcp;         /* COLUMN POSITION IN TABLE             */
  PclUInt16 pbtifanl;        /* AS-NAME LENGTH                       */
  Pclchar pbtifan[1];        /* AS-NAME (var-length)                 */
  PclUInt16 pbtiftl;         /* TITLE LENGTH                         */
  Pclchar pbtift[1];         /* TITLE (var-length)                   */
  PclUInt16 pbtiffl;         /* FORMAT LENGTH                        */
  Pclchar pbtiff[1];         /* FORMAT (var-length)                  */
  PclUInt16 pbtifdvl;        /* DEFAULT-VALUE LENGTH                 */
  Pclchar pbtifdv[1];        /* DEFAULT-VALUE (var-length)           */
  Pclchar pbtific;           /* IDENTITY-COLUMN                      */
#define PBTIFICY 'Y'         /* -YES                                 */
#define PBTIFICN 'N'         /* -NO                                  */
#define PBTIFICU 'U'         /* -UNKNOWN                             */
  Pclchar pbtifdw;           /* DEFINITEABLY WRITABLE                */
#define PBTIFDWY 'Y'         /* -YES                                 */
#define PBTIFDWN 'N'         /* -NO                                  */
  Pclchar pbtifnl;           /* NULLABLE                             */
#define PBTIFNLY 'Y'         /* -YES                                 */
#define PBTIFNLN 'N'         /* -NO                                  */
#define PBTIFNLU 'U'         /* -UNKNOWN                             */
  Pclchar pbtifmn;           /* MAY BE NULL                          */
#define PBTIFMNY 'Y'         /* -YES                                 */
#define PBTIFMNN 'N'         /* -NO                                  */
#define PBTIFMNU 'U'         /* -UNKNOWN                             */
  Pclchar pbtifsr;           /* SEARCHABLE                           */
#define PBTIFSRY 'Y'         /* -YES                                 */
#define PBTIFSRN 'N'         /* -NO                                  */
#define PBTIFSRU 'U'         /* -UNKNOWN                             */
  Pclchar pbtifwr;           /* WRITABLE                             */
#define PBTIFWRY 'Y'         /* -YES                                 */
#define PBTIFWRN 'N'         /* -NO                                  */
  PclUInt16 pbtifdt;         /* DATA TYPE                            */
#define PBTIFDTN 0           /* -NOT AVAILABLE                       */
  PclUInt16 pbtifut;         /* UDT INDICATOR                        */
#define PBTIFUTU 0           /* -UNKNOWN                             */
#define PBTIFUTS 1           /* -STRUCTURED UDT                      */
#define PBTIFUTD 2           /* -DISTINCT UDT                        */
#define PBTIFUTI 3           /* -INTERNAL UDT                        */
  PclUInt16 pbtiftyl;        /* TYPE NAME LENGTH                     */
  Pclchar pbtifty[1];        /* TYPE NAME (var-length)               */
  PclUInt16 pbtifmil;        /* DATATYPEMISCINFO LENGTH              */
  Pclchar pbtifmi[1];        /* DATATYPEMISCINFO (var-length)        */
  PclUInt64 pbtifmdl;        /* MAXIMUM DATA LENGTH IN BYTES         */
  PclUInt16 pbtifnd;         /* TOTAL NUMBER OF DIGITS               */
  PclUInt16 pbtifnid;        /* NUMBER OF INTERVAL DIGITS            */
  PclUInt16 pbtifnfd;        /* NUMBER OF FRACTIONAL DIGITS          */
  Pclchar pbtifct;           /* CHARACTER SET TYPE                   */
#define PBTIFCTN 0           /* -NOT CHARACTER DATA                  */
#define PBTIFCTL 1           /* -LATIN                               */
#define PBTIFCTU 2           /* -UNICODE                             */
#define PBTIFCTS 3           /* -KANJISJIS                           */
#define PBTIFCTG 4           /* -GRAPHIC                             */
#define PBTIFCTK 5           /* -KANJI1                              */
  PclUInt64 pbtifmnc;        /* MAXIMUM NUMBER OF CHARACTERS         */
  Pclchar pbtifcs;           /* CASE SENSITIVE                       */
#define PBTIFCSY 'Y'         /* -YES                                 */
#define PBTIFCSN 'N'         /* -NO                                  */
#define PBTIFCSU 'U'         /* -UNKNOWN                             */
  Pclchar pbtifsn;           /* SIGNED                               */
#define PBTIFSNY 'Y'         /* -YES                                 */
#define PBTIFSNN 'N'         /* -NO                                  */
#define PBTIFSNU 'U'         /* -UNKNOWN                             */
  Pclchar pbtifk;            /* KEY                                  */
#define PBTIFKY 'Y'          /* -YES                                 */
#define PBTIFKN 'N'          /* -NO                                  */
#define PBTIFKU 'U'          /* -UNKNOWN                             */
  Pclchar pbtifu;            /* UNIQUE                               */
#define PBTIFUY 'Y'          /* -YES                                 */
#define PBTIFUN 'N'          /* -NO                                  */
#define PBTIFUU 'U'          /* -UNKNOWN                             */
  Pclchar pbtife;            /* EXPRESSION                           */
#define PBTIFEY 'Y'          /* -YES                                 */
#define PBTIFEN 'N'          /* -NO                                  */
#define PBTIFEU 'U'          /* -UNKNOWN                             */
  Pclchar pbtifso;           /* SORTABLE                             */
#define PBTIFSOY 'Y'         /* -YES                                 */
#define PBTIFSON 'N'         /* -NO                                  */
#define PBTIFSOU 'U'         /* -UNKNOWN                             */
/* DR106619 --> */
  Pclchar pbtifpt;           /* PARAMETER TYPE                       */
#define PBTIFPTI 'I'         /* -INPUT                               */
#define PBTIFPTB 'B'         /* -INOUT (BOTH)                        */
#define PBTIFPTO 'O'         /* -OUTPUT                              */
#define PBTIFPTU 'U'         /* -UNKNOWN                             */
/* <-- DR106619 */
  };

#define PBTIFAN                         pbtifan[1]
#define PBTIFANL                        pbtifanl
#define PBTIFCN                         pbtifcn[1]
#define PBTIFCNL                        pbtifcnl
#define PBTIFCP                         pbtifcp
#define PBTIFCS                         pbtifcs
#define PBTIFCT                         pbtifct
#define PBTIFDB                         pbtifdb[1]
#define PBTIFDBL                        pbtifdbl
#define PBTIFDT                         pbtifdt
#define PBTIFDV                         pbtifdv[1]
#define PBTIFDVL                        pbtifdvl
#define PBTIFDW                         pbtifdw
#define PBTIFE                          pbtife
#define PBTIFF                          pbtiff[1]
#define PBTIFFL                         pbtiffl
#define PBTIFIC                         pbtific
#define PBTIFIH                         pbtifih
#define PBTIFK                          pbtifk
#define PBTIFMDL                        pbtifmdl
#define PBTIFMI                         pbtifmi[1]
#define PBTIFMIL                        pbtifmil
#define PBTIFMN                         pbtifmn
#define PBTIFMNC                        pbtifmnc
#define PBTIFND                         pbtifnd
#define PBTIFNFD                        pbtifnfd
#define PBTIFNID                        pbtifnid
#define PBTIFNL                         pbtifnl
#define PBTIFPT                         pbtifpt  /*DR106619*/
#define PBTIFSN                         pbtifsn
#define PBTIFSO                         pbtifso
#define PBTIFSR                         pbtifsr
#define PBTIFT                          pbtift[1]
#define PBTIFTB                         pbtiftb[1]
#define PBTIFTBL                        pbtiftbl
#define PBTIFTL                         pbtiftl
#define PBTIFTY                         pbtifty[1]
#define PBTIFTYL                        pbtiftyl
#define PBTIFU                          pbtifu
#define PBTIFUT                         pbtifut
#define PBTIFWR                         pbtifwr


/*--------------------------------------------------------------------*
*                                                                     *
*        STATEMENT-INFORMATION RESPONSE,                              *
*                            LIMITED-LAYOUT (QUERY,                   *
*                                            SUMMARY,                 *
*                                            IDENTITY-COLUMN,         *
*                                            STORED-PROCEDURE-OUTPUT, *
*                                            STORED-PROCEDURE-ANSWER) *
*                                                                     *
*                            IF PBROFN=E                              *
*                                                                     *
*--------------------------------------------------------------------*/

struct TRDSATL
  {
  struct TRDSATE info_h;         /* INFORMATION HEADER               */
  PclUInt16 pbtildt;             /* DATA TYPE                        */
#define PBTILDTN 0               /* -NOT AVAILABLE                   */
  PclUInt64 pbtilmdl;            /* MAXIMUM DATA LENGTH IN BYTES     */
  PclUInt16 pbtilnd;             /* TOTAL NUMBER OF DIGITS           */
  PclUInt16 pbtilnid;            /* NUMBER OF INTERVAL DIGITS        */
  PclUInt16 pbtilnfd;            /* NUMBER OF FRACTIONAL DIGITS      */

#define PBTILL 22                /* LENGTH OF INFORMATION            */
  };

#define PBTILDT                         pbtildt
#define PBTILIH                         pbtilih
#define PBTILMDL                        pbtilmdl
#define PBTILND                         pbtilnd
#define PBTILNFD                        pbtilnfd
#define PBTILNID                        pbtilnid



/*--------------------------------------------------------------------*
*                                                                     *
*        STATEMENT-INFORMATION RESPONSE,                              *
*                            STATISTIC-LAYOUT (ESTIMATED-PROCESSING)  *
*                                                                     *
*--------------------------------------------------------------------*/

struct TRDSATS
  {
  struct TRDSATE info_h;         /* INFORMATION HEADER               */
  PclUInt64 pbtisee;             /* EXECUTION ESTIMATE (MILLISECS)   */

#define PBTISL 14                /* LENGTH OF INFORMATION            */
  };

#define PBTISEE                         pbtisee
#define PBTISIH                         pbtisih


/*====================================================================*
*                                                                     *
*        STATEMENT-INFORMATION RESPONSE (WHEN RORSI SPECIFIED)        *
*                                                                     *
*====================================================================*/
#define TRDSPFTI 169             /* FLAVOR                           */

struct TRDSPBTI
  {
/*                                                                    */
/*                           INFORMATION HEADER                       */
/*                                                                    */
  PclUInt16 pbtilout;             /* INFORMATION LAYOUT               */
#define PBTILFL 1                 /* -FULL LAYOUT                     */
#define PBTILLL 2                 /* -LIMITED LAYOUT                  */
#define PBTILSL 3                 /* -STATISTIC LAYOUT                */
#define PBTILEI 4                 /* -END INFORMATION                 */
  PclUInt16 pbtiid;               /* INFORMATION ID                   */
#define PBTIIP 1                  /* -PARAMETER                       */
#define PBTIIQ 2                  /* -QUERY                           */
#define PBTIIS 3                  /* -SUMMARY                         */
#define PBTIIIC 4                 /* -IDENTITY-COLUMN                 */
#define PBTIISPO 5                /* -STORED-PROCEDURE-OUTPUT         */
#define PBTIISPR 6                /* -STORED-PROCEDURE-RESULTSET      */
#define PBTIIEP 7                 /* -ESTIMATED-PROCESSING            */
  PclUInt16 pbtilen;              /* LENGTH OF INFORMATION DATA       */

#define PBTIBL 6                  /* LENGTH OF INFO-HEADER            */
  };

#define PBTIID                          pbtiid
#define PBTILEN                         pbtilen
#define PBTILOUT                        pbtilout

/* <- DR98026, Statement Info Feature */

/* DR102528, Dynamic Result Sets --> */
struct PclResultSetBdy
{
    PclUInt64 ResultSet_row;

    PclWord ResultSet_DB_len;
    Pclchar ResultSet_DB_name[1]; /* variable length of this object */
                                  /* specified in preceding field   */
    PclWord ResultSet_SP_len;
    Pclchar ResultSet_SP_name[1]; /* variable length of this object */
                                  /* specified in preceding field   */
};
/* <-- DR102528, Dynamic Result Sets */

/* DR109200, Stored Procedure Dynamic Result Sets Cursor Positioning -> */
struct PclResultSetPositionType
{
    PclFlavor         Flavor;       /* 173            */
    PclLength         Length;       /* 14             */
    PclUInt16         statmnt_no;
    PclUInt64         rownum;       /* unsigned 64-bit integer */
};

struct PclAltResultSetPositionType
{
    PclFlavor         Flavor;       /* 0x8000 + 173   */
    PclLength         Length;       /* 0              */
    PclAltLength      AltLength;    /* 18             */
    PclUInt16         statmnt_no;
    PclUInt64         rownum;       /* unsigned 64-bit integer */
};
/* <- DR109200, SPDR Cursor Positioning */

/* DR115324, LOB Loader Phase 2 --> */
struct PclElicitDataByNameType
{
    PclFlavor         Flavor;         /* 176                             */
    PclLength         Length;
    PclUInt16         filespeclen;    /* length of the filespec in bytes */
    Pclchar           filespec[1024]; /* name of file in client charset  */
};

struct PclAltElicitDataByNameType
{
    PclFlavor         Flavor;         /* 0x8000 + 176                    */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;
    PclUInt16         filespeclen;    /* length of the filespec in bytes */
    Pclchar           filespec[1024]; /* name of file in client charset  */
};
/* <-- DR115324, LOB Loader Phase 2 */

/* DR145358, CLAC-29119 -> */
#define PCLSESINFOEXTENDOPENRESPONSE 7 /* Open Responses Extension */
#define CONTCXTBUFFER_SIZE           256

typedef struct PclSessInfoReqEntryType
 {
    PclUInt32           RequestNo;
    PclByte             RequestState;
 } PclSessInfoReqEntryType;

/*      RequestState has the following values:
            Active{1)  request is active on the database
            Redrive(2)  request is being redriven by the database
            Complete(3)  request is complete and the response is available */

typedef struct PclSessinfoOpenRespType
 {
    PclUInt16           OpenRespNum;
    PclUInt16           OpenRespLen;
    PclByte             RedriveOrNewRequestOK;
    PclUInt16           EntryLength;
    PclSessInfoReqEntryType     Request[1];
 } PclSessinfoOpenRespType;

/*        RedriveOrNewRequestOK is binary 1 if the client can submit a new
          request or redrive an outstanding request without informing the
          application of the database or network failure.

          EntryLength is the number of bytes for the following Request array.
          If EntryLength is zero, then there are no entries in the array.

          Request is an array of request state information. */

typedef struct PclSessionStatusRespType
{
    PclFlavor         Flavor;	     /* 195                              */
    PclLength	      Length;	     /*                                  */
} PclSessionStatusRespType;

/* ControlDataLength in a control data parcel contains the length of all */
/*     parcels in the entire control data section, including the         */
/*     ControlDataStart and ControlDataEnd parcels                       */
typedef struct PclControlDataType
{
    PclFlavor         Flavor;         /* 197, 198                        */
    PclLength         Length;         /* 8                               */
    PclUInt32         ControlDataLength;
} PclControlDataType;

typedef struct PclAltControlDataType
{
    PclFlavor         Flavor;         /* 0x8000 + 197, 198               */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;      /* 12                              */
    PclUInt32         ControlDataLength;
} PclAltControlDataType;

typedef struct PclRecoverableProtocolType
{
    PclFlavor         Flavor;         /* 200                             */
    PclLength         Length;         /* 4                               */
} PclRecoverableProtocolType;

typedef struct PclAltRecoverableProtocolType
{
    PclFlavor         Flavor;         /* 0x8000 + 200                    */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;      /* 8                               */
} PclAltRecoverableProtocolType;

typedef struct PclRedriveType
{
    PclFlavor         Flavor;         /* 201                             */
    PclLength         Length;         /* 5                               */
    Pclchar           RedriveStatus;
} PclRedriveType;

typedef struct PclAltRedriveType
{
    PclFlavor         Flavor;         /* 0x8000 + 201                    */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;      /* 9                               */
    Pclchar           RedriveStatus;
} PclAltRedriveType;

typedef struct PclRedriveSupportedType
{
    PclFlavor         Flavor;         /* 204                             */
    PclLength         Length;         /* 5                               */
    Pclchar           RedriveSupportedStatus;
} PclRedriveSupportedType;

typedef struct PclAltRedriveSupportedType
{
    PclFlavor         Flavor;         /* 0x8000 + 204                    */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;      /* 9                               */
    Pclchar           RedriveSupportedStatus;
} PclAltRedriveSupportedType;

typedef struct PclRecoverableNPSupportedType
{
    PclFlavor         Flavor;         /* 214                             */
    PclLength         Length;         /* 5                               */
    Pclchar           RecoverableNPSupportedStatus;
} PclRecoverableNPSupportedType;

typedef struct PclAltRecoverableNPSupportedType
{
    PclFlavor         Flavor;         /* 0x8000 + 214                    */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;      /* 9                               */
    Pclchar           RecoverableNPSupportedStatus;
} PclAltRecoverableNPSupportedType;

typedef struct PclAcknowledgementRequestedType
{
    PclFlavor         Flavor;         /* 202                             */
    PclLength         Length;         /* 4                               */
} PclAcknowledgementRequestedType;

typedef struct PclAltAcknowledgementRequestedType
{
    PclFlavor         Flavor;         /* 0x8000 + 202                    */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;      /* 8                               */
} PclAltAcknowledgementRequestedType;

#define MAXCONTROLDATALEN sizeof(PclAltControlDataType) + \
    CONTCXTBUFFER_SIZE + \
    sizeof(PclAltRecoverableProtocolType) + \
    sizeof(PclAltRedriveType) + \
    sizeof(PclAltAcknowledgementRequestedType) + \
    sizeof(PclAltControlDataType)
/* <- DR145358, CLAC-29119 */

/* CLAC-28857 ESS -> */
struct PclStatementStatusType
{
    PclFlavor         Flavor;         /* PclSTATEMENTSTATUS              */
    PclLength         Length;
    Pclchar           StatementStatus;
    Pclchar           ResponseMode;
    Pclchar           Reserved_2[2];
    PclUInt32         StatementNo;
    PclUInt16         Code;
    PclUInt16         ActivityType;
    PclUInt64         ActivityCount;
    PclUInt32         FieldCount;
    Pclchar           Reserved_4[4];
};

struct PclAltStatementStatusType
{
    PclFlavor         Flavor;         /* PclAltSTATEMENTSTATUS           */
    PclLength         Length;         /* 0                               */
    PclAltLength      AltLength;
    Pclchar           StatementStatus;
    Pclchar           ResponseMode;
    Pclchar           Reserved_2[2];
    PclUInt32         StatementNo;
    PclUInt16         Code;
    PclUInt16         ActivityType;
    PclUInt64         ActivityCount;
    PclUInt32         FieldCount;
    Pclchar           Reserved_4[4];
};

struct CliStatementStatusType         /* same as above, less parcel header */
{
    Pclchar           StatementStatus;
    Pclchar           ResponseMode;
    Pclchar           Reserved_2[2];
    PclUInt32         StatementNo;
    PclUInt16         Code;
    PclUInt16         ActivityType;
    PclUInt64         ActivityCount;
    PclUInt32         FieldCount;
    Pclchar           Reserved_4[4];
};

/* Possible values for the CliStatementStatusType.StatementStatus field */
/*   and the existing parcels they corresponds to                       */
#define CLI_ESS_SUCCESS    0  /* PclSUCCESS and PclOK */
#define CLI_ESS_ERROR      1  /* PclERROR             */
#define CLI_ESS_FAILURE    2  /* PclFAILURE           */
#define CLI_ESS_STMTERROR  3  /* PclSTMTERROR         */

/* Possible values for the CliStatementStatusType.ResponseMode field */
#define CLI_ESS_RESP_FIELD_MODE           1
#define CLI_ESS_RESP_RECORD_MODE          2
#define CLI_ESS_RESP_INDIC_MODE           3
#define CLI_ESS_RESP_MULTIPART_INDIC_MODE 4

typedef struct CliEssExt_s
{
    PclUInt16         ExtInfoID;     /* ESS Extension Info ID             */
    PclUInt32         ExtInfoLength; /* length of the extension body only */
    /* followed by a possibly empty variable length extension body */
} CliEssExt_t, *CliEssExt_p;

#define CLI_ESS_EXT_WARNINGMSG 1
typedef struct CliEssExtWarningMsg_s
{
    PclUInt16         WarningCode;
    PclUInt16         ComponentID;
    PclUInt32         WarningLength; /* length of WarningMsg */
    /* if WarningLength > 0, followed by a variable length WarningMsg */
} CliEssExtWarningMsg_t, *CliEssExtWarningMsg_p;

#define CLI_ESS_EXT_ERRORFAILURE 2 /* CLAC-30059 */
typedef struct CliEssExtErrorFailure_s
{
    PclUInt16         ErrorFailureCode;
    PclUInt16         ComponentID;
    PclUInt32         ErrorFailureLength; /* length of ErrorFailureMsg */
    /* if ErrorFailureLength > 0, followed by a var len ErrorFailureMsg */
} CliEssExtErrorFailure_t, *CliEssExtErrorFailure_p;

#define CLI_ESS_EXT_CASCADEDERROR 3 /* CLAC-30059 */
typedef struct CliEssExtCascadedError_s
{
    PclUInt16         CascadingErrorCode;
    PclUInt16         ComponentID;
    PclUInt32         CascadingErrorLength; /* length of CascadingErrorMsg */
    /* if CascadingErrorLength > 0, followed by a var len CascadingErrorMsg */
} CliEssExtCascadedError_t, *CliEssExtCascadedError_p;

#define CLI_ESS_EXT_SYNTAXERRORLOCATION 4 /* CLAC-30059 */
typedef struct CliEssExtSyntaxErrorLocation_s
{
    PclUInt32         SyntaxErrorCharOffset;
} CliEssExtSyntaxErrorLocation_t, *CliEssExtSyntaxErrorLocation_p;

#define CLI_ESS_EXT_MINRESPBUFFERSIZE 5 /* CLAC-30059 */
typedef struct CliEssExtMinRespBufferSize_s
{
    PclUInt32         MinRespBufferSize;
} CliEssExtMinRespBufferSize_t, *CliEssExtMinRespBufferSize_p;

#define CLI_ESS_EXT_MERGEACTIVITYCOUNTS 10
typedef struct CliEssExtMergeActivityCounts_s
{
    PclUInt64         InsertCount;
    PclUInt64         UpdateCount;
    PclUInt64         DeleteCount; /* CLAC-29283 */
} CliEssExtMergeActivityCounts_t, *CliEssExtMergeActivityCounts_p;

#define CLI_ESS_EXT_MULTILOADACTIVITYCOUNTS 13
/* this ESS self-defining extension contains variable length fields
   and is only here for reference:
struct CliEssExtMultiLoadActivityCounts
{
    PclUInt64         InsertCount;
    PclUInt64         UpdateCount;
    PclUInt64         DeleteCount;
    PclUInt32         TableDBNameLength;
    Pclchar           TableDBName[TableDBNameLength];
    PclUInt32         TableNameLength;
    Pclchar           TableName[TableNameLength];
}; */

#define CLI_ESS_EXT_MULTILOADRESTARTINFO 28
typedef struct CliEssExtMultiLoadRestartInfo_s
{
    PclUInt16         RestartCode;
    PclUInt16         AMPDownCode;
} CliEssExtMultiLoadRestartInfo_t, *CliEssExtMultiLoadRestartInfo_p;
/* <- CLAC-28857 ESS */

/* CLAC-30555 MLOADX -> */
#define CLI_ESS_EXT_ARRAYINSERTACTIVITYCOUNTS 31
typedef struct CliEssExtArrayInsertActivityCounts_s
{
    PclUInt64         InsertCount;
    PclUInt64         ErrorCount;
} CliEssExtArrayInsertActivityCounts_t, *CliEssExtArrayInsertActivityCounts_p;
/* <- CLAC-30555 MLOADX */

/* CLAC-29115 1MB row -> */
#define CLI_ESS_EXT_MAXLARGEROWSRECSIZEINFO 32
typedef struct CliEssExtMaxLargeRowsRecSizeInfo_s
{
    PclUInt32         MaxLargeRowSize;
    PclUInt64         LargeRowCount;
} CliEssExtMaxLargeRowsRecSizeInfo_t, *CliEssExtMaxLargeRowsRecSizeInfo_p;
/* <- CLAC-29115 1MB row */

/* CLAC-30059 -> */
struct PclSTMTERRORType
{
    PclFlavor          PclKind;          /* PclSTMTERROR      */
    PclLength          PLength;          /* 12 to 267         */
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];         /* 1 to 255 bytes    */
};

struct PclAltSTMTERRORType
{
    PclFlavor          PclKind;          /* PclAltSTMTERROR   */
    PclLength          PLength;          /* 0                 */
    PclAltLength       AltLength;        /* 16 to 271         */
    PclWord            StatementNo;
    PclWord            Info;
    PclWord            Code;
    PclWord            Length;
    Pclchar            Msg[255];         /* 1 to 255 bytes    */
};
/* <- CLAC-30059 */

/* CLAC-31559 --> */
/*===========================*/
/* Small LOB (SLOB) Parcels  */
/*===========================*/
struct PclSLOBResponse
{
    PclFlavor                 Flavor;    /* 215 */
    PclLength                 Length;
    PclUInt64                 MaxSingleLOBBytes;
    PclUInt64                 MaxTotalLOBBytesPerRow;
};
struct PclAltSLOBResponse
{
    PclFlavor                 Flavor;    /* 0x8000 + 215 */
    PclLength                 Length;
    PclAltLength              AltLength;
    PclUInt64                 MaxSingleLOBBytes;
    PclUInt64                 MaxTotalLOBBytesPerRow;
};
/* <-- CLAC-31559 */

/*  CLAC-30554 -->           */
/*===========================*/
/* Small LOB (SLOB) Parcels  */
/*===========================*/
struct PclSLOBDataStart
{
    PclFlavor                 Flavor;    /* 220 */
    PclLength                 Length;
/*  CLAC-31559 -->  */
    PclUInt32                 MetadataItemNo;
/*  Optional field, not supported by CLI      */
/*  PclUInt32                 ArrayElementNo; */
/*  <-- CLAC-31559  */
};

struct PclAltSLOBDataStart
{
    PclFlavor                 Flavor;   /* 0x8000 + 220 */
    PclLength                 Length;
    PclAltLength              AltLength;
/*  CLAC-31559 -->  */
    PclUInt32                 MetadataItemNo;
/*  Optional field, not supported by CLI      */
/*  PclUInt32                 ArrayElementNo; */
/*  <-- CLAC-31559  */
};
/* <-- CLAC-30554  */

/* CLAC-30758 --> */
struct CliSlobDataStartType
{
  PclUInt32     MetadataItemNo;
};

struct CliSlobDataType
{
  PclUInt64     SlobLength;
  Pclchar       SlobData[1];   /* variable length */
};
/* <-- CLAC-30758 */

/* CLAC-29671 --> */
/*===========================*/
/* Client Attributes         */
/*===========================*/
#define PCLCLIATT_HOSTNAME                     7
#define PCLCLIATT_PROCTHRID                    8
#define PCLCLIATT_SYSUSERID                    9
#define PCLCLIATT_PROGNAME                    10
#define PCLCLIATT_OSNAME                      11
#define PCLCLIATT_JOBNAME                     13    /* CLMF-9820 */
#define PCLCLIATT_JOBID                       21    /* CLMF-9820 */
#define PCLCLIATT_CLIV2RELID                  24
#define PCLCLIATT_JOBDATA                     27
#define PCLCLIATT_CLIENTATTRIBUTESEX          30    /* CLIWS-8509 */
#define PCLCLIATT_CLIENT_IPADDR_BY_CLIENT     31
#define PCLCLIATT_CLIENT_PORT_BY_CLIENT       32
#define PCLCLIATT_SERVER_IPADDR_BY_CLIENT     33
#define PCLCLIATT_SERVER_PORT_BY_CLIENT       34
#define PCLCLIATT_COP_SUFFIXED_HOST_NAME      45
#define PCLCLIATT_CLIENT_CONFIDENTIALITY_TYPE 58    /* CLIWS-7192 */
#define PCLCLIATT_END                      32767

/* maximum length for PCLCLIATT_JOBDATA */
#define PCLCLIATT_JOBDATA_MAXLEN          128
/* <-- CLAC-29671*/

/* CLIWS-8509 maximum length for PCLCLIATT_CLIENTATTRIBUTESEX */
/* See https://docs.teradata.com/r/Teradata-VantageTM-Data-Dictionary/June-2022/Views-Reference/SessionInfoV-X */
#define PCLCLIATT_CLIENTATTRIBUTESEX_MAXLEN 512

/* DR 68259 ---> */
/* DR84603: Add pack(1) for Win32*/
#if defined(WIN32) || defined(__APPLE__) || defined(AIX) /* CLAC-18937, CLIWS-7896 */
#pragma pack(pop)
#elif defined(__MVS__)     /* DR106757 */
#pragma pack(reset)        /* DR106757 */
#else
#pragma pack()
#endif
/* <--- DR 68259 */

/**************************************************************/
/*                                                            */
/*                    END OF PARCEL.H                         */
/*                                                            */
/**************************************************************/
#endif



================================================
FILE: src/util/binary_reader.hpp
================================================
#pragma once

#include "duckdb/common/exception.hpp"
#include <type_traits>
#include <cstdint>

namespace duckdb {

class BinaryReader {
public:
	BinaryReader(const char *ptr, size_t len) : ptr(ptr), end(ptr + len), beg(ptr) {
	}

	template <class T>
	T Read() {
		static_assert(std::is_trivial<T>::value, "Type must be trivial");

		CheckSize(sizeof(T));

		T result;
		memcpy(&result, ptr, sizeof(T));
		ptr += sizeof(T);

		return result;
	}

	const char *ReadBytes(size_t size) {
		CheckSize(size);

		const char *result = ptr;
		ptr += size;

		return result;
	}

	void ReadInto(char *dst, size_t size) {
		CheckSize(size);

		memcpy(dst, ptr, size);
		ptr += size;
	}

	void Skip(size_t size) {
		CheckSize(size);
		ptr += size;
	}

	void Reset() {
		ptr = beg;
	}

private:
	void CheckSize(size_t size) const {
		if (ptr + size > end) {
			throw InvalidInputException("BinaryReader: read out of bounds");
		}
	}

	const char *ptr;
	const char *end;
	const char *beg;
};

} // namespace duckdb


================================================
FILE: test/sql/teradata/basic.test
================================================
require teradata

query I
SELECT 1 + 2
----
3


================================================
FILE: test/sql/teradata/ctas.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if already exists
statement ok
DROP TABLE IF EXISTS td.test_table;

# Create a test table
statement error
CREATE TABLE td.test_table AS SELECT i FROM range(5) r(i);
----
Binder Error: CREATE TABLE AS is not supported for Teradata tables. Please create the table first and then insert into it.



================================================
FILE: test/sql/teradata/filter_pushdown.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if already exists
statement ok
DROP TABLE IF EXISTS td.filter_pushdown_test;

statement ok
DROP TABLE IF EXISTS td.filter_pushdown_blob;

# Create a test table
statement ok
CREATE TABLE td.filter_pushdown_test (i INT);

statement ok
INSERT INTO td.filter_pushdown_test SELECT i FROM range(5) r(i);

# Greater than
query I
SELECT * FROM td.filter_pushdown_test WHERE i > 2 ORDER BY ALL;
----
3
4

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i>2;
----
physical_plan	<REGEX>:.*Filters: i>2.*

# Less than
query I
SELECT * FROM td.filter_pushdown_test WHERE i < 2 ORDER BY ALL;
----
0
1

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i<2;
----
physical_plan	<REGEX>:.*Filters: i<2.*

# Equal
query I
SELECT * FROM td.filter_pushdown_test WHERE i = 2 ORDER BY ALL;
----
2

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i=2;
----
physical_plan	<REGEX>:.*Filters: i=2.*

# Not equal
query I
SELECT * FROM td.filter_pushdown_test WHERE i <> 2 ORDER BY ALL;
----
0
1
3
4

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i<>2;
----
physical_plan	<REGEX>:.*Filters: i!=2.*

# AND
query I
SELECT * FROM td.filter_pushdown_test WHERE i > 1 AND i < 4 ORDER BY ALL;
----
2
3

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i>1 AND i<4;
----
physical_plan	<REGEX>:.*Filters: i>1 AND i<4.*

# OR
query I
SELECT * FROM td.filter_pushdown_test WHERE i < 1 OR i > 3 ORDER BY ALL;
----
0
4

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i<1 OR i>3;
----
physical_plan	<REGEX>:.*i<1 OR i>3.*

# IN
query I
SELECT * FROM td.filter_pushdown_test WHERE i IN (0, 2, 4) ORDER BY ALL;
----
0
2
4

query II
EXPLAIN SELECT * FROM td.filter_pushdown_test WHERE i IN (0, 2, 4);
----
physical_plan	<REGEX>:.*i IN \(0, 2, 4\).*


# Blob literal
statement ok
DROP TABLE IF EXISTS td.filter_pushdown_blob;

statement ok
CREATE TABLE td.filter_pushdown_blob (a INT, b BLOB);

statement ok
INSERT INTO td.filter_pushdown_blob VALUES
	(0, 'abc'::BLOB),
	(1, '000102030405060708090A0B0C0D0E0F'::BLOB);

query II
SELECT * FROM td.filter_pushdown_blob WHERE b = '000102030405060708090A0B0C0D0E0F' ORDER BY ALL;
----
1	000102030405060708090A0B0C0D0E0F

query I
SELECT a FROM td.filter_pushdown_blob WHERE b = '000102030405060708090A0B0C0D0E0F' ORDER BY ALL;
----
1

query I
SELECT b FROM td.filter_pushdown_blob WHERE a = 0 ORDER BY ALL;
----
abc


# clean up
statement ok
DROP TABLE IF EXISTS td.filter_pushdown_blob;

statement ok
DROP TABLE IF EXISTS td.filter_pushdown_test;


================================================
FILE: test/sql/teradata/index.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if already exists
statement ok
DROP TABLE IF EXISTS td.test_table;

# Create a test table

statement ok
CREATE TABLE td.test_table(id INT, name VARCHAR);

statement ok
INSERT INTO td.test_table VALUES (0, 'zero');

statement ok
INSERT INTO td.test_table VALUES (1, 'one');

query II
SELECT * FROM td.test_table ORDER BY ALL;
----
0	zero
1	one

statement ok
CREATE UNIQUE INDEX test_table_idx ON td.test_table(id);

query IIIII
SELECT database_name, table_name, index_name, is_unique, is_primary FROM duckdb_indexes();
----
td	test_table	test_table_idx	1	0

statement error
INSERT INTO td.test_table VALUES (1, 'two');
----
Constraint Error: Secondary index uniqueness violation in

# clean up
statement ok
DROP TABLE IF EXISTS td.test_table;



================================================
FILE: test/sql/teradata/primary_key.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if already exists
statement ok
DROP TABLE IF EXISTS td.test_table;

# Create a test table

statement ok
CREATE TABLE td.test_table(id INT PRIMARY KEY, name VARCHAR);

statement ok
INSERT INTO td.test_table VALUES (0, 'zero');

statement ok
INSERT INTO td.test_table VALUES (1, 'one');

query II
SELECT * FROM td.test_table ORDER BY ALL;
----
0	zero
1	one

statement error
INSERT INTO td.test_table VALUES (1, 'two');
----
Constraint Error: Duplicate unique prime key error in


# clean up
statement ok
DROP TABLE IF EXISTS td.test_table;



================================================
FILE: test/sql/teradata/td_connect.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

statement ok
drop table if exists td.t1;

statement ok
create table td.t1 (a int, b int);

statement ok
insert into td.t1 values (1, 2), (3, 4);

query II rowsort
select * from td.t1;
----
1	2
3	4

statement ok
drop table td.t1;


================================================
FILE: test/sql/teradata/td_query.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Select with the teradata_query function
query I
select * from teradata_query('td', 'select 1 + 2');
----
3

# Use the table macro created in the schema
query I
select * from td.query('select 1 + 2');
----
3



================================================
FILE: test/sql/teradata/teradata_delete.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if already exists
statement ok
DROP TABLE IF EXISTS td.test_table;

statement ok
CREATE TABLE td.test_table (i INTEGER);

statement ok
INSERT INTO td.test_table VALUES (1), (2), (3);

# Delete rows from the table
statement ok
DELETE FROM td.test_table WHERE i = 2;

# Verify the deletion
query I
SELECT * FROM td.test_table ORDER BY i;
----
1
3

statement ok
DROP TABLE IF EXISTS td.test_table;


================================================
FILE: test/sql/teradata/teradata_secret.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
# This test case only works for the dbc user, unfortunately
require-env TD_DB dbc

# Should not work
statement error
ATTACH '' as td (TYPE TERADATA)
----
Invalid Input Error: Teradata ATTACH must contain a 'user', either in the logon string, passed as options or in defined in a secret

statement error
ATTACH '' as td (TYPE TERADATA, SECRET my_secret)
----
Invalid Input Error: No Teradata secret found with name: my_secret

statement error
CREATE SECRET my_secret (
	TYPE TERADATA,
	HOST '127.0.0.1',
	DATABASE '${TD_DB}',
	PASSWORD 'dbc'
);
----
Invalid Input Error: Teradata secret must contain 'HOST', 'USER', 'DATABASE', and 'PASSWORD' parameters

statement ok
CREATE SECRET my_secret (
	TYPE TERADATA,
	HOST '127.0.0.1',
	DATABASE '${TD_DB}',
	USER 'dbc',
	PASSWORD 'dbc'
);

# Should work
statement ok
ATTACH '' as td (TYPE TERADATA, SECRET my_secret)


================================================
FILE: test/sql/teradata/teradata_update.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
attach '${TD_LOGON}' as td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if already exists
statement ok
DROP TABLE IF EXISTS td.test_table;

# Dont use primary index, so we can have multiple rows with the same value
statement ok
SET teradata_use_primary_index = false;

statement ok
CREATE TABLE td.test_table (i INTEGER);

statement ok
INSERT INTO td.test_table VALUES (1), (2), (3);

# Update the rows in the table
statement ok
UPDATE td.test_table SET i = i + 1 WHERE i = 2;

# Verify the deletion
query I
SELECT * FROM td.test_table ORDER BY i;
----
1
3
3

statement ok
DROP TABLE IF EXISTS td.test_table;


================================================
FILE: test/sql/teradata/types/binary.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"'
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');

# Cleanup in case test failed
statement ok
DROP TABLE IF EXISTS td.blob_table;

# DuckDB only supports a single BLOB type, mapping to VARBYTE(64000) in Teradata
statement ok
CREATE TABLE td.blob_table(b BLOB)

# Insert some data
statement ok
INSERT INTO td.blob_table VALUES ('aaa'::BLOB), ('bbb'::BLOB), (NULL), ('ccc'::BLOB);

query I
SELECT * FROM td.blob_table ORDER BY b;
----
aaa
bbb
ccc
NULL

statement ok
DROP TABLE IF EXISTS td.blob_table;


================================================
FILE: test/sql/teradata/types/character.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');

# Cleanup in case test failed
statement ok
DROP TABLE IF EXISTS td.varchar_table;

# DuckDB only supports a single VARCHAR type, mapping to VARCHAR(64000) in Teradata
statement ok
CREATE TABLE td.varchar_table(v VARCHAR)

# Insert some data
statement ok
INSERT INTO td.varchar_table VALUES ('aaa'), ('bbb'), (NULL), ('ccc');

query I
SELECT * FROM td.varchar_table ORDER BY v;
----
aaa
bbb
ccc
NULL

statement ok
DROP TABLE IF EXISTS td.varchar_table;


================================================
FILE: test/sql/teradata/types/date.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');


# Drop table if it exists
statement ok
DROP TABLE IF EXISTS td.test_date;

# Create test table
statement ok
CREATE TABLE td.test_date (
  date DATE
);

# Insert test data
statement ok
INSERT INTO td.test_date (date) VALUES
('2000-01-01'),
('2009-10-10');

# Select data from test table
query I
SELECT date FROM td.test_date ORDER BY ALL;
----
2000-01-01
2009-10-10

# clean up
statement ok
DROP TABLE IF EXISTS td.test_date;



================================================
FILE: test/sql/teradata/types/decimal.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');


# Cleanup in case test failed
statement ok
DROP TABLE IF EXISTS td.decimal_table;

# Create table with decimal column
statement ok
CREATE TABLE td.decimal_table (d DECIMAL(10, 2));

# Insert some data
statement ok
INSERT INTO td.decimal_table VALUES (1.23), (4.56), (7.89);

# Select data
query I
SELECT d FROM td.decimal_table ORDER BY ALL;
----
1.23
4.56
7.89

query I
SELECT * FROM teradata_query('td', 'SEL d FROM decimal_table ORDER BY d');
----
1.23
4.56
7.89

# Cleanup
statement ok
DROP TABLE IF EXISTS td.decimal_table;

# Now try with decimals in each size category (1-2, 3-4, 5-9, 10-18, 19-38)
# Cleanup in case test failed
statement ok
DROP TABLE IF EXISTS td.decimal_table;

# Create table with decimal column
statement ok
CREATE TABLE td.decimal_table (
	d1 DECIMAL(2, 1),
	d2 DECIMAL(4, 2),
	d3 DECIMAL(5, 2),
	d4 DECIMAL(10, 2),
	d5 DECIMAL(20, 2));

# Insert some data
statement ok
INSERT INTO td.decimal_table VALUES
	(1.2, 4.56, 7.89, 10.11, 12.13),
	(4.1, 16.17, 18.19, 20.21, 22.23),
	(4.2, 26.27, 28.29, 30.31, 32.33);

# Select data
query IIIII
SELECT d1, d2, d3, d4, d5 FROM td.decimal_table ORDER BY ALL;
----
1.2	4.56	7.89	10.11	12.13
4.1	16.17	18.19	20.21	22.23
4.2	26.27	28.29	30.31	32.33

statement ok
DROP TABLE IF EXISTS td.decimal_table;



================================================
FILE: test/sql/teradata/types/float.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');

# Cleanup in case test failed
statement ok
DROP TABLE IF EXISTS td.float_table;

# Teradata only supports a single floating point type. FLOAT, REAL and DOUBLE PRECISION are all aliases of DOUBLE
statement ok
CREATE TABLE td.float_table (d DOUBLE);

# Insert some data
statement ok
INSERT INTO td.float_table VALUES (1.0), (2.0), (3.0);

# Select data
query I
SELECT * FROM td.float_table ORDER BY d;
----
1.0
2.0
3.0

# Cleanup
statement ok
DROP TABLE IF EXISTS td.float_table;

statement ok
DROP TABLE IF EXISTS td.real_table;

# Lets also check if we can scan a table with aliases of the floating types
statement ok
CALL teradata_execute('td', 'CREATE TABLE real_table (f FLOAT, r REAL, dp DOUBLE PRECISION)');

# Clear the cache so that we can detect the new table
statement ok
CALL teradata_clear_cache()

# Describe the table. DuckDB should map all these types to DOUBLE
query II
SELECT column_name, column_type FROM (DESCRIBE td.real_table);
----
f	DOUBLE
r	DOUBLE
dp	DOUBLE

# Insert into the table
statement ok
INSERT INTO td.real_table VALUES (1.0, 2.0, 3.0);

# Select data
query III
SELECT * FROM td.real_table ORDER BY f;
----
1.0	2.0	3.0

statement ok
DROP TABLE td.real_table;


================================================
FILE: test/sql/teradata/types/integer.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');

# Cleanup in case test failed
statement ok
DROP TABLE IF EXISTS td.integer_table;

# These types are supported
# TINYINT, SMALLINT, INTEGER, BIGINT
statement ok
CREATE TABLE td.integer_table(i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT)

# Insert into integer table
statement ok
INSERT INTO td.integer_table VALUES (1, 2, 3, 4);

# Select from integer table
query IIII
SELECT * FROM td.integer_table;
----
1	2	3	4

statement ok
DROP TABLE IF EXISTS td.integer_table;

# Unsigned types and hugeints are not supported
# UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT
statement error
CREATE TABLE td.unsupported_type(u8 UTINYINT);
----
Not implemented Error: Cannot convert DuckDB type 'UTINYINT' to Teradata type

statement error
CREATE TABLE td.unsupported_type(u16 USMALLINT);
----
Not implemented Error: Cannot convert DuckDB type 'USMALLINT' to Teradata type

statement error
CREATE TABLE td.unsupported_type(u32 UINTEGER);
----
Not implemented Error: Cannot convert DuckDB type 'UINTEGER' to Teradata type

statement error
CREATE TABLE td.unsupported_type(u64 UBIGINT);
----
Not implemented Error: Cannot convert DuckDB type 'UBIGINT' to Teradata type

statement error
CREATE TABLE td.unsupported_type(i128 HUGEINT);
----
Not implemented Error: Cannot convert DuckDB type 'HUGEINT' to Teradata type

statement error
CREATE TABLE td.unsupported_type(u128 UHUGEINT);
----
Not implemented Error: Cannot convert DuckDB type 'UHUGEINT' to Teradata type



================================================
FILE: test/sql/teradata/types/time.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');


# Drop table if it exists
statement ok
DROP TABLE IF EXISTS td.test_time

# Create test table
statement ok
CREATE TABLE td.test_time (
  t TIME
);

# Insert test data
statement ok
INSERT INTO td.test_time (t) VALUES (TIME '10:14:59')
# Select data from test table
query I
SELECT t FROM td.test_time ORDER BY ALL;
----
10:14:59

# clean up
statement ok
DROP TABLE IF EXISTS td.test_time;



================================================
FILE: test/sql/teradata/types/timestamp.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');


# Drop table if it exists
statement ok
DROP TABLE IF EXISTS td.test_timestamp;

# Create test table
statement ok
CREATE TABLE td.test_timestamp (t TIMESTAMP, ts TIMESTAMP_S, tsm TIMESTAMP_MS);

# Insert test data
statement ok
INSERT INTO td.test_timestamp VALUES (
    TIMESTAMP '2000-08-25 10:14:59.123456',
    TIMESTAMP_S '2000-08-25  10:14:59',
    TIMESTAMP_MS '2000-08-25  10:14:59.123'
)

# Select data from test table
query III
SELECT * FROM td.test_timestamp ORDER BY ALL;
----
2000-08-25 10:14:59.123456	2000-08-25 10:14:59	2000-08-25 10:14:59.123

# clean up
statement ok
DROP TABLE IF EXISTS td.test_timestamp;



================================================
FILE: test/sql/teradata/types/timestamptz.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');


# Drop table if it exists
statement ok
DROP TABLE IF EXISTS td.test_timestamp;

# Create test table
statement ok
CREATE TABLE td.test_timestamp (
  t TIMESTAMPTZ
);

#(TIMESTAMPTZ '2000-01-01 11:37:58.123456+08:00'),
# Insert test data
statement ok
INSERT INTO td.test_timestamp (t) VALUES
    (TIMESTAMPTZ '2000-01-01 11:37:58-08:00')

# Select data from test table
query I
SELECT t FROM td.test_timestamp ORDER BY ALL;
----
2000-01-01 19:37:58+00

# clean up
statement ok
DROP TABLE IF EXISTS td.test_timestamp;


================================================
FILE: test/sql/teradata/types/timetz.test
================================================
require teradata

# Pass teradata logon string as environment variable
# e.g. 'export TD_LOGON="127.0.0.1/dbc,dbc"`
require-env TD_LOGON

# Pass database name to use when creating test tables
# e.g. 'export TD_DB="duckdb_testdb"'
require-env TD_DB

# Attach teradata database
statement ok
ATTACH '${TD_LOGON}' AS td (TYPE TERADATA, DATABASE '${TD_DB}');

# Drop table if it exists
statement ok
DROP TABLE IF EXISTS td.test_timestamp;

# Create test table
statement ok
CREATE TABLE td.test_timestamp (
  t TIMETZ
);

# Insert test data
statement ok
INSERT INTO td.test_timestamp (t) VALUES (TIMETZ '11:37:58.123456+08:00'), (TIMETZ '11:37:58-08:00')
# Select data from test table
query I
SELECT t FROM td.test_timestamp ORDER BY ALL;
----
11:37:58+00
11:37:58.123456+00

# clean up
statement ok
DROP TABLE IF EXISTS td.test_timestamp;



================================================
FILE: .github/workflows/Linux.yml
================================================
name: Integration Tests
on: [push, pull_request,repository_dispatch]
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}-${{ github.head_ref || '' }}-${{ github.base_ref || '' }}-${{ github.ref != 'refs/heads/main' || github.sha }}
  cancel-in-progress: true
defaults:
  run:
    shell: bash

jobs:
  linux-tests-teradata:
    name: Run tests on Linux
    runs-on: ubuntu-latest
    strategy:
      matrix:
        # Add commits/tags to build against other DuckDB versions
        duckdb_version: [ 'v1.3.2' ]
        arch: ['linux_amd64']
        vcpkg_version: [ '2023.04.15' ]
        include:
          - arch: 'linux_amd64'
            vcpkg_triplet: 'x64-linux'

    env:
      VCPKG_TARGET_TRIPLET: ${{ matrix.vcpkg_triplet }}
      GEN: Ninja
      VCPKG_TOOLCHAIN_PATH: ${{ github.workspace }}/vcpkg/scripts/buildsystems/vcpkg.cmake

    steps:
      - name: Install required ubuntu packages
        run: |
          sudo apt-get update -y -qq
          sudo apt-get install -y -qq build-essential cmake ninja-build ccache python3 wget

      - name: Install Teradata Tools and Utilities
        run: |
          wget https://test-bucket-ceiveran.s3.eu-west-1.amazonaws.com/td_ubuntu_20.tar.gz
          tar -xvf td_ubuntu_20.tar.gz
          cd TeradataToolsAndUtilitiesBase && sudo ./setup.sh a
          cd ..

      - uses: actions/checkout@v3
        with:
          fetch-depth: 0
          submodules: 'true'

      - name: Checkout DuckDB to version
        if: ${{ matrix.duckdb_version != '<submodule_version>'}}
        run: |
          cd duckdb
          git checkout ${{ matrix.duckdb_version }}

      - name: Setup Ccache
        uses: hendrikmuhs/ccache-action@main
        with:
          key: ${{ github.job }}
          save: ${{ github.ref == 'refs/heads/main' || github.repository != 'duckdb/duckdb-teradata-connector' }}

      - name: Setup vcpkg
        uses: lukka/run-vcpkg@v11.1
        with:
          vcpkgGitCommitId: a42af01b72c28a8e1d7b48107b33e4f286a55ef6

      - name: Build extension
        env:
          GEN: ninja
          STATIC_LIBCPP: 1
        run: |
          make release

      - name: Test extension
        env:
          TD_LOGON: ${{ secrets.TD_LOGON }}
          TD_DB: ${{ vars.TD_DB }}

        run: |
          make test_release


================================================
FILE: .github/workflows/MainDistributionPipeline.yml
================================================
#
# This workflow calls the main distribution pipeline from DuckDB to build, test and (optionally) release the extension
#
name: Main Extension Distribution Pipeline
on:
  pull_request:
    branches:
      - main
    paths-ignore:
      - '**/README.md'
      - 'doc/**'
  push:
    branches:
      - main
    paths-ignore:
      - '**/README.md'
      - 'doc/**'
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}-${{ github.head_ref || '' }}-${{ github.base_ref || '' }}-${{ github.ref != 'refs/heads/main' || github.sha }}
  cancel-in-progress: true

jobs:
  duckdb-latest-build:
    name: Build extension binaries
    uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main
    with:
      duckdb_version: v1.3.2
      extension_name: teradata
      ci_tools_version: main
      vcpkg_commit: ce613c41372b23b1f51333815feb3edd87ef8a8b
      # Only build for "linux_amd64", "osx_arm64", "osx_amd64", "windows_amd64"
      exclude_archs: linux_arm64;linux_amd64_musl;windows_amd64_mingw;wasm_mvp;wasm_eh;wasm_threads

  duckdb-latest-deploy:
    name: Deploy extension binaries
    needs: duckdb-latest-build
    uses: duckdb/extension-ci-tools/.github/workflows/_extension_deploy.yml@main
    secrets: inherit
    with:
      duckdb_version: v1.3.2
      ci_tools_version: main
      extension_name: teradata
      deploy_latest: ${{ startsWith(github.ref, 'refs/heads/v') || github.ref == 'refs/heads/main' }}
      exclude_archs: linux_arm64;linux_amd64_musl;windows_amd64_mingw;wasm_mvp;wasm_eh;wasm_threads
      # Only build for "linux_amd64", "osx_arm64", "osx_amd64", "windows_amd64"


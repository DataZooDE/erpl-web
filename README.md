# ERPL Web

**ERPL Web** is a DuckDB extension that allows you to make HTTP requests from within DuckDB SQL queries. 

This is an experimental extension for DuckDB that adds basic support for making HTTP requests from within SQL queries. The extension is built on top of the [httplib](https://github.com/yhirose/cpp-httplib), which is already included in the DuckDB source code.

## Installation
The installation process is the same as for any other DuckDB extension. As we provide a thrid party extension, you have to start DuckDB with the `-unsigned` flag to load the extension. After that the following commands can be used to install and load the extension:

```sql
SET custom_extension_repository = 'http://get.erpl.io';
INSTALL 'erpl_web';
LOAD 'erpl_web'; 
``` 

## Usage
To run a HTTP `GET` request, use the `http_get` function. For example:
```sql
SELECT content FROM http_get('http://httpbun.com/ip');
```

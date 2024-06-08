<a name="top"></a>

# ERPL Web

**ERPL Web** is a DuckDB extension that allows you to make HTTP requests from within DuckDB SQL queries. 

This is an experimental extension for DuckDB that adds basic support for making HTTP requests from within SQL queries. The extension is built on top of the [httplib](https://github.com/yhirose/cpp-httplib), which is already included in the DuckDB source code.

## ➜ Installation
The installation process is the same as for any other DuckDB extension. As we provide a thrid party extension, you have to start DuckDB with the `-unsigned` flag to load the extension. After that the following commands can be used to install and load the extension:

```sql
INSTALL 'erpl_web' FROM 'http://get.erpl.io';
LOAD 'erpl_web'; 
``` 
[Back to Top](#top)

## ⚙ Example Usage
To run a HTTP `GET` request, use the `http_get` function. For example:
```sql
SELECT content FROM http_get('http://httpbun.com/ip');
```

You get back a table valued result with the following fields:

- `method` (`VARCHAR`): The [HTTP method](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) used in the request.
- `status` (`INTEGER`): The [HTTP status code](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) of the response.
- `url` (`VARCHAR`): The URL of the request.
- `headers` (`MAP(VARCHAR, VARCHAR)`): The headers of the response.
  - `key` (`VARCHAR`): The header key.
  - `value` (`VARCHAR`): The header value.
- `content_type` (`VARCHAR`): The [content type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type) of the response.
- `content` (`VARCHAR`): Finally and most important, the content of the response.

We have now a few examples, showing how combining the `http_get` with DuckDBs powerful [JSON functions](https://duckdb.org/docs/extensions/json.html) can be used to extract data from APIs.

### A first request
```sql
SELECT method, status, content_type FROM http_get('https://httpbun.com/get');
```

```text
┌─────────┬────────┬──────────────────┐
│ method  │ status │   content_type   │
│ varchar │ int32  │     varchar      │
├─────────┼────────┼──────────────────┤
│ GET     │    200 │ application/json │
└─────────┴────────┴──────────────────┘
```

### Extracting JSON data from response
When it comes now the more interesting example of extracting content from an API, we use [httpbin service](https://httpbin.org/). It returns typically the follwoing resusults

```text
HTTP/1.1 200 OK
Access-Control-Allow-Credentials: true
Access-Control-Allow-Origin: *
Connection: keep-alive
Content-Length: 416
Content-Type: application/json
Date: Sat, 08 Jun 2024 14:32:32 GMT
Server: gunicorn/19.9.0

{
    "args": {
        "foo": "bar"
    },
    "data": "",
    "files": {},
    "form": {},
    "headers": {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "httpbin.org",
        "User-Agent": "HTTPie/3.2.2",
        "X-Amzn-Trace-Id": "Root=1-66646b80-6fea073c77b8279a6183c2e9"
    },
    "json": null,
    "method": "GET",
    "origin": "95.90.195.65",
    "url": "https://httpbin.org/anything?foo=bar"
}
```

The following query issues a `GET` request to the `httpbin` service and extracts the `args.foo` property, url and method from the JSON response. The
details of the JSON extraction can be found in the [DuckDB documentation](https://duckdb.org/docs/extensions/json.html#json-extraction-functions).

```sql
SELECT status, 
	   content::JSON->'args'->>'foo' as args,
	   content::JSON->>'url' as url,
	   content::JSON->>'method' as method
FROM http_get('https://httpbin.org/anything?foo=bar');
```
This results into the following output:
```text
┌────────┬─────────┬──────────────────────────────────────┬─────────┐
│ status │  args   │                 url                  │ method  │
│ int32  │ varchar │               varchar                │ varchar │
├────────┼─────────┼──────────────────────────────────────┼─────────┤
│    200 │ bar     │ https://httpbin.org/anything?foo=bar │ GET     │
└────────┴─────────┴──────────────────────────────────────┴─────────┘
```

[Back to Top](#top)


## 💻 HTTP Function Reference
For simple HTTP interaction the extension currently provides the following two functions for making HTTP requests, which just retrieve the content of the response:

| Function | Description |
|:---|:---|
| `http_get(url:VARCHAR)`  | Do an Http [`GET`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/GET) request, and retrieve the data |
| `http_head(url:VARCHAR)` | Do an HTTP [`HEAD`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/HEAD) request. |

We further provide the following functions, supporting mutating HTTP verbs:

| Function | Description |
|:---|:---|
| `http_post(url:VARCHAR, body::JSON)`    | Do an HTTP [`POST`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/POST) request. The request body is of [DuckDB's JSON type](https://duckdb.org/docs/extensions/json.html). |
| `http_post(url:VARCHAR, body::VARCHAR, content_type::VARCHAR)`    | Do an HTTP [`POST`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/POST) request. The function takes two arguments: The request `body` as well as the `content_type`. |
| `http_put(url:VARCHAR, body::JSON)`     | Do an HTTP [`PUT`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/PUT) request. The request body is of [DuckDB's JSON type](https://duckdb.org/docs/extensions/json.html).|
| `http_put(url:VARCHAR, body::VARCHAR, content_type::VARCHAR)`     | Do an HTTP [`PUT`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/PUT) request. The function takes two arguments: The request `body` as well as the `content_type`. |
| `http_patch(url:VARCHAR, body::JSON)`   | Do an HTTP [`PATCH`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/PATCH) request. The request body is of [DuckDB's JSON type](https://duckdb.org/docs/extensions/json.html).|
| `http_patch(url:VARCHAR, body::VARCHAR, content_type::VARCHAR)`   | Do an HTTP [`PATCH`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/PATCH) request. The function takes two arguments: The request `body` as well as the `content_type`. |
| `http_delete(url:VARCHAR, body::JSON)`  | Do an HTTP [`DELETE`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/DELETE) request. The request body is of [DuckDB's JSON type](https://duckdb.org/docs/extensions/json.html).|
| `http_delete(url:VARCHAR, body::VARCHAR, content_type::VARCHAR)`  | Do an HTTP [`DELETE`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/DELETE) request. The function takes two arguments: The request `body` as well as the `content_type`. |

This functions take the following named parameters parameters:

| Name | Description | Type | Default |
|:--|:-----|:-|:-|
| `headers` | A [DuckDB map](https://duckdb.org/docs/sql/data_types/map.html#creating-maps) containing key-value-pairs of [HTTP headers](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers). | `MAP(VARCHAR, VARCHAR)` | `{}` |
| `content_type` | The value of the [HTTP content type header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type) (in case of a mutating query). | `BOOL` | `false` |
| `accept` | The value of the [HTTP accept header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept). | `VARCHAR` | `'application/json'` |
| `auth` | The authentication value which is put in the header. For `auth_type == 'BASIC'` pass in a `username:password` pair. For other auth types enter the respective `token`| `VARCHAR` | `NULL` |
| `auth_type` | An enum of different HTTP auth types with the possible values `{'BASIC', 'DIGEST', 'BEARER'}`. | `ENUM` | `BASIC` |
| `timeout` | The request timeout in milliseconds. | `BIGINT` | `60000` |

[Back to Top](#top)

## Tracking

### Overview
Our extension automatically collects basic usage data to enhance its performance and gain insights into user engagement. We employ [Posthog](https://posthog.com/) for data analysis, transmitting information securely to the European Posthog instance at [https://eu.posthog.com](https://eu.posthog.com) via HTTPS.

### Data Collected
Each transmitted request includes the following information:
- Extension Version
- DuckDB Version
- Operating System
- System Architecture
- MAC Address of the Primary Network Interface

### Event Tracking
Data is transmitted under these circumstances:
- **Extension Load**: No extra data is sent beyond the initial usage information.
- **Function Invocation**: The name of the invoked function is sent. *Note: Function inputs/outputs are not transmitted.*
- **Error Occurrence**: The error message is transmitted.

### User Configuration Options
Users can control tracking through these settings:

1. **Enable/Disable Tracking**:
   ```sql
   SET erpl_telemetry_enabled = TRUE; -- Enabled by default; set to FALSE to disable tracking
   ```
   
2. **Posthog API Key Configuration** (usually unchanged):
   ```sql
   SET erpl_telemetry_key = 'phc_XXX'; -- Pre-set to our key; customizable to your own key
   ```

This approach ensures transparency about data collection while offering users control over their privacy settings.

[Back to Top](#top)

## License
The ERPL extension is licensed under the [Business Source License (BSL) Version 1.1](./LICENSE.md). The BSL is a source-available license that gives you the following permissions:

### Allowed:
1. **Copy, Modify, and Create Derivative Works**: You have the right to copy the software, modify it, and create derivative works based on it.
2. **Redistribute and Non-Production Use**: Redistribution and non-production use of the software is permitted.
3. **Limited Production Use**: You can make production use of the software, but with limitations. Specifically, the software cannot be offered to third parties on a hosted or embedded basis.
4. **Change License Rights**: After the Change Date (five years from the first publication of the Licensed Work), you gain rights under the terms of the Change License (MPL 2.0). This implies broader permissions after this date.

### Not Allowed:
1. **Offering to Third Parties on Hosted/Embedded Basis**: The Additional Use Grant restricts using the software in a manner that it is offered to third parties on a hosted or embedded basis.
2. **Violation of Current License Requirements**: If your use does not comply with the requirements of the BSL, you must either purchase a commercial license or refrain from using the software.
3. **Trademark Usage**: You don't have rights to any trademark or logo of Licensor or its affiliates, except as expressly required by the License.

### Additional Points:
- **Separate Application for Each Version**: The license applies individually to each version of the Licensed Work. The Change Date may vary for each version.
- **Display of License**: You must conspicuously display this License on each original or modified copy of the Licensed Work.
- **Third-Party Receipt**: If you receive the Licensed Work from a third party, the terms and conditions of this License still apply.
- **Automatic Termination on Violation**: Any use of the Licensed Work in violation of this License automatically terminates your rights under this License for all versions of the Licensed Work.
- **Disclaimer of Warranties**: The Licensed Work is provided "as is" without any warranties, including but not limited to the warranties of merchantability, fitness for a particular purpose, non-infringement, and title.

This summary is based on the provided license text and should be used as a guideline. For legal advice or clarification on specific points, consulting a legal professional is recommended, especially for commercial or complex use cases.

[Back to Top](#top)
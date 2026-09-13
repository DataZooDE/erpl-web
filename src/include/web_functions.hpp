#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "datazoo/oauth2/http_client.hpp"

using namespace duckdb;

namespace erpl_web {


class HttpBindData : public TableFunctionData
{
public:
    HttpBindData(std::shared_ptr<HttpRequest> request, std::shared_ptr<HttpAuthParams> auth_params, const HttpParams &http_params);

    std::vector<std::string> GetResultNames();
    std::vector<LogicalType> GetResultTypes();
    bool HasMoreResults() const;
    // Issues the request and writes the single response row. Does NOT latch: the caller
    // decides whether the result may be produced again (a read verb re-issues per
    // execution, a mutating verb must not).
    unsigned int SendRequest(DataChunk &output) const;
    // SendRequest plus the bind-data latch, for the mutating verbs.
    unsigned int FetchNextResult(DataChunk &output) const; 

private :
    std::shared_ptr<HttpRequest> request;
    std::shared_ptr<HttpAuthParams> auth_params;
    std::shared_ptr<bool> done;
    HttpParams http_params;
};

LogicalType CreateHttpHeaderType();
TableFunctionSet CreateHttpGetFunction();
TableFunctionSet CreateHttpPostFunction();
TableFunctionSet CreateHttpPutFunction();
TableFunctionSet CreateHttpPatchFunction();
TableFunctionSet CreateHttpDeleteFunction();
TableFunctionSet CreateHttpHeadFunction();

} // namespace erpl_web
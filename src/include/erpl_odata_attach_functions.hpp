#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "erpl_odata_client.hpp"
#include "erpl_odata_edm.hpp"
#include "erpl_odata_predicate_pushdown_helper.hpp"

using namespace duckdb;

namespace erpl_web {

class ODataAttachBindData : public TableFunctionData
{
public:
    static duckdb::unique_ptr<ODataAttachBindData> FromUrl(const std::string& url, std::shared_ptr<HttpAuthParams> auth_params);

public:
    ODataAttachBindData(std::shared_ptr<ODataServiceClient> odata_client);

    bool IsFinished() const;
    void SetFinished();

    std::vector<std::string> IgnorePatterns() const;
    void IgnorePatterns(const std::vector<std::string> &ignore);

    bool Overwrite() const;
    void SetOverwrite(bool overwrite);

    std::vector<ODataEntitySetReference> EntitySets();

    static bool MatchPattern(const std::string &str, const std::string &ignore_pattern);
    static bool MatchPattern(const std::string &str, const std::vector<std::string> &ignore_patterns);

private:
    bool finished = false;
    bool overwrite = false;
    std::shared_ptr<ODataServiceClient> odata_client;
    std::vector<std::string> ignore_patterns;
};

TableFunctionSet CreateODataAttachFunction();

} // namespace erpl_web
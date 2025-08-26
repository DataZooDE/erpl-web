#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include <vector>
#include <string>

namespace erpl_web 
{   
    class ArgBuilder {
    public:
        ArgBuilder();
        ArgBuilder& Add(const std::string& name, const duckdb::Value& value);
        ArgBuilder& Add(const std::string& name, duckdb::Value& value);
        ArgBuilder& Add(const std::string& name, const ArgBuilder& builder);
        ArgBuilder& Add(const std::string& name, const duckdb::vector<duckdb::Value>& values);
        ArgBuilder& Add(const std::string& name, const std::vector<duckdb::Value>& std_vector);
        ArgBuilder& Add(const std::string& name, const std::initializer_list<duckdb::Value>& values);
        duckdb::Value Build() const;
        std::vector<duckdb::Value> BuildArgList() const;
    private:
        duckdb::child_list_t<duckdb::Value> _args;
    };

    class ValueHelper {
    public:
        ValueHelper(duckdb::Value& value);
        ValueHelper(duckdb::Value& value, const std::vector<std::string>& root_path);
        ValueHelper(duckdb::Value& value, const std::string& root_path);
        
        duckdb::Value operator[](const std::string& name);
        std::vector<std::string> GetPathWithRoot(const std::string& path) const;
        static duckdb::Value GetValueForPath(duckdb::Value& value, const std::vector<std::string>& tokens);
        static duckdb::Value CreateMutatedValue(duckdb::Value& old_value, duckdb::Value& new_value, const std::string& path);
        static duckdb::Value CreateMutatedValue(duckdb::Value& old_value, duckdb::Value& new_value, const std::vector<std::string>& tokens);
        static duckdb::Value AddToList(duckdb::Value& current_list, duckdb::Value& new_value);
        static duckdb::Value RemoveFromList(duckdb::Value& current_list, duckdb::Value& remove_value);
        static duckdb::Value RemoveFromList(duckdb::Value& current_list, unsigned int index_to_remove);
        static std::vector<std::string> ParseJsonPointer(const std::string& path);
        static bool IsX(const duckdb::Value& value);
        static bool IsX(duckdb::Value& value);
        duckdb::Value& Get();
        void Print() const;
        void Print(const std::string& path) const;
    private:
        duckdb::Value& _value;
        std::vector<std::string> _root_path;
    };

    using named_parameter_map_t = duckdb::named_parameter_map_t;
    bool HasParam(const named_parameter_map_t& named_params, const std::string& name);
    duckdb::Value ConvertBoolArgument(const named_parameter_map_t& named_params, const std::string& name, const bool default_value);

    // Utility: extract list of strings from a DuckDB Value of LIST(VARCHAR)
    std::vector<std::string> GetStringList(const duckdb::Value &val);
    
} // namespace erpl_web
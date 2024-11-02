#include "erpl_odata_content.hpp"

namespace erpl_web {


// ----------------------------------------------------------------------

ODataJsonContentMixin::ODataJsonContentMixin(const std::string& content)
{
    doc = std::shared_ptr<yyjson_doc>(yyjson_read(content.c_str(), content.size(), 0), yyjson_doc_free);
}

bool ODataJsonContentMixin::IsJsonContentType(const std::string& content_type)
{
    return content_type.find("application/json") != std::string::npos;
}

void ODataJsonContentMixin::ThrowTypeError(yyjson_val *json_value, const std::string &expected)
{
    auto actual = yyjson_get_type_desc(json_value);
    std::stringstream ss;
    ss << "Expected JSON type '" << expected << "', but got type: '" << actual << "'";
    throw duckdb::ParserException(ss.str());
}

void ODataJsonContentMixin::PrettyPrint()
{
    auto flags = YYJSON_WRITE_PRETTY | YYJSON_WRITE_ESCAPE_UNICODE;
    auto json_doc_str = yyjson_write(doc.get(), flags, nullptr);
    std::cout << std::endl << json_doc_str << std::endl << std::endl;
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonValue(yyjson_val *json_value, const duckdb::LogicalType &duck_type)
{
    if (yyjson_is_null(json_value)) {
        return duckdb::Value().DefaultCastAs(duck_type);
    }

    switch (duck_type.id()) 
    {
        case duckdb::LogicalTypeId::BOOLEAN:
            return DeserializeJsonBool(json_value);
        case duckdb::LogicalTypeId::TINYINT:
            return DeserializeJsonSignedInt8(json_value);
        case duckdb::LogicalTypeId::UTINYINT:
            return DeserializeJsonUnsignedInt8(json_value);
        case duckdb::LogicalTypeId::SMALLINT:
            return DeserializeJsonSignedInt16(json_value);
        case duckdb::LogicalTypeId::USMALLINT:
            return DeserializeJsonUnsignedInt16(json_value);
        case duckdb::LogicalTypeId::INTEGER:
            return DeserializeJsonSignedInt32(json_value);
        case duckdb::LogicalTypeId::UINTEGER:
            return DeserializeJsonUnsignedInt32(json_value);
        case duckdb::LogicalTypeId::BIGINT:
            return DeserializeJsonSignedInt64(json_value);
        case duckdb::LogicalTypeId::UBIGINT:
            return DeserializeJsonUnsignedInt64(json_value);
        case duckdb::LogicalTypeId::FLOAT:
            return DeserializeJsonFloat(json_value);
        case duckdb::LogicalTypeId::DOUBLE:
            return DeserializeJsonDouble(json_value);
        case duckdb::LogicalTypeId::VARCHAR:
            return DeserializeJsonString(json_value);
        case duckdb::LogicalTypeId::ENUM:
            return DeserializeJsonEnum(json_value, duck_type);
        case duckdb::LogicalTypeId::LIST:
            return DeserializeJsonArray(json_value, duck_type);
        case duckdb::LogicalTypeId::STRUCT:
            return DeserializeJsonObject(json_value, duck_type);
        default:
            throw duckdb::InvalidTypeException(duck_type, "Unsupported OData -> DuckDB type");
    }
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonBool(yyjson_val *json_value)
{
    if (!yyjson_is_bool(json_value)) {
        ThrowTypeError(json_value, "bool");
    }

    return duckdb::Value(yyjson_get_bool(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt8(yyjson_val *json_value)
{
    if (!yyjson_is_int(json_value)) {
        ThrowTypeError(json_value, "int8_t");
    }

    return duckdb::Value::TINYINT(yyjson_get_sint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt8(yyjson_val *json_value)
{
    if (!yyjson_is_uint(json_value)) {
        ThrowTypeError(json_value, "uint8_t");
    }

    return duckdb::Value::UTINYINT(yyjson_get_uint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt16(yyjson_val *json_value)
{
    if (!yyjson_is_int(json_value)) {
        ThrowTypeError(json_value, "int16_t");
    }

    return duckdb::Value::SMALLINT(yyjson_get_sint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt16(yyjson_val *json_value)
{
    if (!yyjson_is_uint(json_value)) {
        ThrowTypeError(json_value, "uint16_t");
    }

    return duckdb::Value::USMALLINT(yyjson_get_uint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt32(yyjson_val *json_value)
{
    if (!yyjson_is_int(json_value)) {
        ThrowTypeError(json_value, "int32_t");
    }

    return duckdb::Value::INTEGER(yyjson_get_sint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt32(yyjson_val *json_value)
{
    if (!yyjson_is_uint(json_value)) {
        ThrowTypeError(json_value, "uint32_t");
    }

    return duckdb::Value::UINTEGER(yyjson_get_uint(json_value));
}   

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt64(yyjson_val *json_value)
{
    if (!yyjson_is_int(json_value)) {
        ThrowTypeError(json_value, "int64_t");
    }

    return duckdb::Value::BIGINT(yyjson_get_sint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt64(yyjson_val *json_value)
{
    if (!yyjson_is_uint(json_value)) {
        ThrowTypeError(json_value, "uint64_t");
    }

    return duckdb::Value::UBIGINT(yyjson_get_uint(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonFloat(yyjson_val *json_value)
{
    if (!yyjson_is_real(json_value)) {
        ThrowTypeError(json_value, "float");
    }

    return duckdb::Value::FLOAT(yyjson_get_real(json_value));
}   

duckdb::Value ODataJsonContentMixin::DeserializeJsonDouble(yyjson_val *json_value)
{
    if (!yyjson_is_real(json_value)) {
        ThrowTypeError(json_value, "double");
    }

    return duckdb::Value::DOUBLE(yyjson_get_real(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonString(yyjson_val *json_value)
{
    if (!yyjson_is_str(json_value)) {
        ThrowTypeError(json_value, "string");
    }

    return duckdb::Value(yyjson_get_str(json_value));
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonEnum(yyjson_val *json_value, const duckdb::LogicalType &duck_type)
{
    if (!yyjson_is_str(json_value)) {
        ThrowTypeError(json_value, "string");
    }

    std::vector<std::string> enum_values;
    for (idx_t i = 0; i < duckdb::EnumType::GetSize(duck_type); i++) {
        auto enum_str = duckdb::EnumType::GetString(duck_type, i).GetString();
        enum_values.push_back(enum_str);
    }
    
    auto json_str = yyjson_get_str(json_value);
    auto enum_index = std::find(enum_values.begin(), enum_values.end(), json_str);
    if (enum_index == enum_values.end()) {
        ThrowTypeError(json_value, "enum");
    }

    return duckdb::Value::ENUM(enum_index - enum_values.begin(), duck_type);
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonArray(yyjson_val *json_value, const duckdb::LogicalType &list_type)
{
    if (!yyjson_is_arr(json_value)) {
        ThrowTypeError(json_value, "array");
    }
    
    duckdb::vector<duckdb::Value> child_values;
    child_values.reserve(yyjson_arr_size(json_value));

    auto child_type = duckdb::ListType::GetChildType(list_type);

    size_t i, max;
    yyjson_val *json_child_val;
    yyjson_arr_foreach(json_value, i, max, json_child_val) {
        auto duck_value = DeserializeJsonValue(json_child_val, child_type);
        child_values.push_back(duck_value);
    }

    return duckdb::Value::LIST(child_type, child_values);
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonObject(yyjson_val *json_value, const duckdb::LogicalType &duck_type)
{
    if (!yyjson_is_obj(json_value)) {
        ThrowTypeError(json_value, "object");
    }

    duckdb::child_list_t<duckdb::Value> child_values;
    child_values.reserve(yyjson_obj_size(json_value));

    auto child_types = duckdb::StructType::GetChildTypes(duck_type);
    for (const auto& child : child_types) {
        auto child_name = child.first;
        auto child_type = child.second;

        auto json_child_val = yyjson_obj_get(json_value, child_name.c_str());
        auto duck_value = DeserializeJsonValue(json_child_val, child_type);
        child_values.push_back(std::make_pair(child_name, duck_value));
    }

    return duckdb::Value::STRUCT(child_values);
}

std::string ODataJsonContentMixin::MetadataContextUrl()
{
    auto root = yyjson_doc_get_root(doc.get());
    auto context = yyjson_obj_get(root, "@odata.context");
    if (!context) {
        throw std::runtime_error("No @odata.context found in OData response, cannot get EDMX metadata.");
    }

    auto context_url = yyjson_get_str(context);

    return std::string(context_url);
}

std::optional<std::string> ODataJsonContentMixin::NextUrl()
{
    auto root = yyjson_doc_get_root(doc.get());
    auto next_link = yyjson_obj_get(root, "@odata.nextLink");
    if (!next_link) {
        return std::nullopt;
    }

    auto next_url = yyjson_get_str(next_link);
    return std::string(next_url);
}

std::string ODataJsonContentMixin::GetStringProperty(yyjson_val *json_value, const std::string &property_name) const
{
    auto json_property = yyjson_obj_get(json_value, property_name.c_str());
    if (!json_property) {
        throw std::runtime_error("No " + property_name + "-element found in OData response.");
    }

    return std::string(yyjson_get_str(json_property));
}

// ----------------------------------------------------------------------

ODataEntitySetJsonContent::ODataEntitySetJsonContent(const std::string& content)
    : ODataJsonContentMixin(content)
{ }

std::string ODataEntitySetJsonContent::MetadataContextUrl()
{
    return ODataJsonContentMixin::MetadataContextUrl();
}

std::optional<std::string> ODataEntitySetJsonContent::NextUrl()
{
    return ODataJsonContentMixin::NextUrl();
}

std::vector<std::vector<duckdb::Value>> ODataEntitySetJsonContent::ToRows(std::vector<std::string> &column_names, 
                                                                          std::vector<duckdb::LogicalType> &column_types)
{
    auto root = yyjson_doc_get_root(doc.get());
    auto json_values = yyjson_obj_get(root, "value");
    if (!json_values) {
        throw std::runtime_error("No value-element found in OData response, cannot get rows.");
    }

    if (!yyjson_is_arr(json_values)) {
        throw std::runtime_error("value-element in OData response is not an array, cannot get rows.");
    }

    auto duck_rows = std::vector<std::vector<duckdb::Value>>();
    duck_rows.reserve(yyjson_arr_size(json_values));

    std::vector<std::string> column_type_names;
    for (const auto &type : column_types) {
        column_type_names.push_back(type.ToString());
    }

    size_t i_row, max_row;
    yyjson_val *json_row;
    yyjson_arr_foreach(json_values, i_row, max_row, json_row) 
    {
        auto duck_row = std::vector<duckdb::Value>();
        duck_row.reserve(column_names.size());

        for (size_t i_col = 0; i_col < column_names.size(); i_col++) {
            auto json_value = yyjson_obj_get(json_row, column_names[i_col].c_str());
            auto duck_value = DeserializeJsonValue(json_value, column_types[i_col]);
            duck_row.push_back(duck_value);
        }

        duck_rows.push_back(duck_row);
    }

    return duck_rows;
}

void ODataEntitySetJsonContent::PrettyPrint()
{
    ODataJsonContentMixin::PrettyPrint();
}

// ----------------------------------------------------------------------

ODataServiceJsonContent::ODataServiceJsonContent(const std::string& content)
    : ODataJsonContentMixin(content)
{ }

std::string ODataServiceJsonContent::MetadataContextUrl()
{
    return ODataJsonContentMixin::MetadataContextUrl();
}

void ODataServiceJsonContent::PrettyPrint()
{
    ODataJsonContentMixin::PrettyPrint();
}

std::vector<ODataEntitySetReference> ODataServiceJsonContent::EntitySets()
{
    auto root = yyjson_doc_get_root(doc.get());
    auto json_values = yyjson_obj_get(root, "value");
    if (!json_values) {
        throw std::runtime_error("No value-element found in OData response, cannot get entity sets for service.");
    }

    if (!yyjson_is_arr(json_values)) {
        throw std::runtime_error("value-element in OData response is not an array, cannot get entity sets for service.");
    }

    auto ret = std::vector<ODataEntitySetReference>();
    ret.reserve(yyjson_arr_size(json_values));

    size_t i_row, max_row;
    yyjson_val *json_row;
    yyjson_arr_foreach(json_values, i_row, max_row, json_row) 
    {
        auto kind_property = yyjson_obj_get(json_row, "kind");
        auto kind = std::string("EntitySet"); // by default we assume the reference is an entity set
        if (kind_property != nullptr) {
            kind = GetStringProperty(json_row, "kind");
        }
        
        if (kind != "EntitySet") {
            continue;
        }

        ret.push_back(ODataEntitySetReference { 
            GetStringProperty(json_row, "name"), 
            GetStringProperty(json_row, "url") 
        });
    }

    return ret;
}

} // namespace erpl_web
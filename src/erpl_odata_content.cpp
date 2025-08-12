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

ODataVersion ODataJsonContentMixin::DetectODataVersion(const std::string& content)
{
    std::cout << "[DEBUG] [DETECT_VERSION] Starting OData version detection" << std::endl;
    
    // Parse the JSON content to detect OData version
    auto doc = std::shared_ptr<yyjson_doc>(yyjson_read(content.c_str(), content.size(), 0), yyjson_doc_free);
    if (!doc) {
        std::cout << "[DEBUG] [DETECT_VERSION] Failed to parse JSON, defaulting to V4" << std::endl;
        // If we can't parse JSON, default to v4
        return ODataVersion::V4;
    }
    
    auto root = yyjson_doc_get_root(doc.get());
    if (!root || !yyjson_is_obj(root)) {
        std::cout << "[DEBUG] [DETECT_VERSION] Root is not an object, defaulting to V4" << std::endl;
        return ODataVersion::V4;
    }
    
    // Simple and reliable version detection based on top-level elements
    // OData v4: {"value": [...]}
    // OData v2: {"d": [...]}
    
    auto value_element = yyjson_obj_get(root, "value");
    if (value_element && yyjson_is_arr(value_element)) {
        std::cout << "[DEBUG] [DETECT_VERSION] Found 'value' array, detecting as V4" << std::endl;
        return ODataVersion::V4;
    }
    
    auto d_element = yyjson_obj_get(root, "d");
    if (d_element && yyjson_is_arr(d_element)) {
        std::cout << "[DEBUG] [DETECT_VERSION] Found 'd' array, detecting as V2" << std::endl;
        return ODataVersion::V2;
    }
    
    // Check for other v4 indicators
    auto context = yyjson_obj_get(root, "@odata.context");
    if (context && yyjson_is_str(context)) {
        std::cout << "[DEBUG] [DETECT_VERSION] Found '@odata.context', detecting as V4" << std::endl;
        return ODataVersion::V4;
    }
    
    // Check for other v2 indicators
    if (d_element && yyjson_is_obj(d_element)) {
        // Check if d contains results array (typical for v2 collections)
        auto results = yyjson_obj_get(d_element, "results");
        if (results && yyjson_is_arr(results)) {
            std::cout << "[DEBUG] [DETECT_VERSION] Found 'd' object with 'results' array, detecting as V2" << std::endl;
            return ODataVersion::V2;
        }
        
        // Check if d contains __metadata (typical for v2 single entities)
        auto metadata = yyjson_obj_get(d_element, "__metadata");
        if (metadata && yyjson_is_obj(metadata)) {
            std::cout << "[DEBUG] [DETECT_VERSION] Found 'd' object with '__metadata', detecting as V2" << std::endl;
            return ODataVersion::V2;
        }
        
        // If we have a 'd' wrapper but can't determine the structure, assume V2
        std::cout << "[DEBUG] [DETECT_VERSION] Found 'd' wrapper, assuming V2" << std::endl;
        return ODataVersion::V2;
    }
    
    std::cout << "[DEBUG] [DETECT_VERSION] No clear indicators found, defaulting to V4" << std::endl;
    // Default to v4 if we can't determine
    return ODataVersion::V4;
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
        std::cout << "[DEBUG] [DESERIALIZE] JSON value is null, returning default" << std::endl;
        return duckdb::Value().DefaultCastAs(duck_type);
    }

    std::cout << "[DEBUG] [DESERIALIZE] Deserializing JSON value to type: " << duck_type.ToString() << std::endl;

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
            std::cout << "[DEBUG] [DESERIALIZE] Deserializing LIST type" << std::endl;
            return DeserializeJsonArray(json_value, duck_type);
        case duckdb::LogicalTypeId::STRUCT:
            std::cout << "[DEBUG] [DESERIALIZE] Deserializing STRUCT type" << std::endl;
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
        if (yyjson_is_null(json_child_val)) {
            // Handle null values in arrays
            std::cout << "[DEBUG] [DESERIALIZE_ARRAY] Found null value at index " << i << ", using default" << std::endl;
            auto duck_value = duckdb::Value().DefaultCastAs(child_type);
            child_values.push_back(duck_value);
        } else {
            auto duck_value = DeserializeJsonValue(json_child_val, child_type);
            child_values.push_back(duck_value);
        }
    }

    return duckdb::Value::LIST(child_type, child_values);
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonObject(yyjson_val *json_value, const duckdb::LogicalType &duck_type)
{
    if (!yyjson_is_obj(json_value)) {
        ThrowTypeError(json_value, "object");
    }

    duckdb::child_list_t<duckdb::Value> child_values;
    child_values.reserve(duckdb::StructType::GetChildTypes(duck_type).size());

    auto child_types = duckdb::StructType::GetChildTypes(duck_type);
    for (const auto& child : child_types) {
        auto child_name = child.first;
        auto child_type = child.second;

        auto json_child_val = yyjson_obj_get(json_value, child_name.c_str());
        if (!json_child_val) {
            // Field doesn't exist in JSON, use default value
            std::cout << "[DEBUG] [DESERIALIZE_OBJECT] Field " << child_name << " not found in JSON, using default" << std::endl;
            auto duck_value = duckdb::Value().DefaultCastAs(child_type);
            child_values.push_back(std::make_pair(child_name, duck_value));
        } else {
            // Field exists, deserialize it
            std::cout << "[DEBUG] [DESERIALIZE_OBJECT] Deserializing field " << child_name << std::endl;
            auto duck_value = DeserializeJsonValue(json_child_val, child_type);
            child_values.push_back(std::make_pair(child_name, duck_value));
        }
    }

    return duckdb::Value::STRUCT(child_values);
}

std::string ODataJsonContentMixin::MetadataContextUrl()
{
    auto root = yyjson_doc_get_root(doc.get());
    return GetMetadataContextUrl(root);
}

std::optional<std::string> ODataJsonContentMixin::NextUrl()
{
    auto root = yyjson_doc_get_root(doc.get());
    return GetNextUrl(root);
}

std::string ODataJsonContentMixin::GetStringProperty(yyjson_val *json_value, const std::string &property_name) const
{
    auto json_property = yyjson_obj_get(json_value, property_name.c_str());
    if (!json_property) {
        throw std::runtime_error("No " + property_name + "-element found in OData response.");
    }

    return std::string(yyjson_get_str(json_property));
}

// JSON path evaluation for complex expressions like AddressInfo[1].City."Name"
yyjson_val* ODataJsonContentMixin::EvaluateJsonPath(yyjson_val* root, const std::string& path) {
    if (!root || path.empty()) {
        return nullptr;
    }

    auto path_parts = ParseJsonPath(path);
    yyjson_val* current = root;

    for (const auto& part : path_parts) {
        if (!current) {
            return nullptr;
        }

        if (part.empty()) {
            continue;
        }

        // Check if this is an array index
        if (part[0] == '[' && part[part.length() - 1] == ']') {
            if (!yyjson_is_arr(current)) {
                return nullptr;
            }
            
            try {
                size_t index = std::stoul(part.substr(1, part.length() - 2));
                current = yyjson_arr_get(current, index);
            } catch (const std::exception&) {
                return nullptr;
            }
        }
        // Check if this is a quoted property name
        else if (part[0] == '"' && part[part.length() - 1] == '"') {
            if (!yyjson_is_obj(current)) {
                return nullptr;
            }
            
            std::string property_name = part.substr(1, part.length() - 2);
            current = yyjson_obj_get(current, property_name.c_str());
        }
        // Regular property name
        else {
            if (!yyjson_is_obj(current)) {
                return nullptr;
            }
            
            current = yyjson_obj_get(current, part.c_str());
        }
    }

    return current;
}

std::vector<std::string> ODataJsonContentMixin::ParseJsonPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string current_part;
    bool in_quotes = false;
    bool in_brackets = false;
    
    for (size_t i = 0; i < path.length(); i++) {
        char c = path[i];
        
        if (c == '"' && (i == 0 || path[i-1] != '\\')) {
            in_quotes = !in_quotes;
            if (in_quotes) {
                // Start of quoted string
                if (!current_part.empty()) {
                    parts.push_back(current_part);
                    current_part.clear();
                }
                current_part += c;
            } else {
                // End of quoted string
                current_part += c;
                parts.push_back(current_part);
                current_part.clear();
            }
        }
        else if (c == '[' && !in_quotes) {
            in_brackets = true;
            if (!current_part.empty()) {
                parts.push_back(current_part);
                current_part.clear();
            }
            current_part += c;
        }
        else if (c == ']' && !in_quotes) {
            in_brackets = false;
            current_part += c;
            parts.push_back(current_part);
            current_part.clear();
        }
        else if (c == '.' && !in_quotes && !in_brackets) {
            if (!current_part.empty()) {
                parts.push_back(current_part);
                current_part.clear();
            }
        }
        else {
            current_part += c;
        }
    }
    
    if (!current_part.empty()) {
        parts.push_back(current_part);
    }
    
    return parts;
}

// Version-aware JSON parsing methods
yyjson_val* ODataJsonContentMixin::GetValueArray(yyjson_val* root) {
    std::cout << "[DEBUG] [GET_VALUE_ARRAY] OData version: " << (odata_version == ODataVersion::V2 ? "V2" : "V4") << std::endl;
    
    if (odata_version == ODataVersion::V2) {
        std::cout << "[DEBUG] [GET_VALUE_ARRAY] Processing OData v2 structure" << std::endl;
        
        // OData v2: {"d": [...]} or {"d": {"results": [...]}}
        auto d_wrapper = yyjson_obj_get(root, "d");
        if (!d_wrapper) {
            std::cout << "[DEBUG] [GET_VALUE_ARRAY] No 'd' wrapper found in OData v2 response" << std::endl;
            throw std::runtime_error("No 'd' wrapper found in OData v2 response.");
        }
        
        // Check if d is directly an array (common case)
        if (yyjson_is_arr(d_wrapper)) {
            std::cout << "[DEBUG] [GET_VALUE_ARRAY] Found 'd' as direct array with " << yyjson_arr_size(d_wrapper) << " items" << std::endl;
            return d_wrapper;
        }
        
        // Check if d contains a "results" array (traditional v2 format)
        if (yyjson_is_obj(d_wrapper)) {
            auto results = yyjson_obj_get(d_wrapper, "results");
            if (results && yyjson_is_arr(results)) {
                std::cout << "[DEBUG] [GET_VALUE_ARRAY] Found 'd' object with 'results' array containing " << yyjson_arr_size(results) << " items" << std::endl;
                return results;
            }
        }
        
        std::cout << "[DEBUG] [GET_VALUE_ARRAY] 'd' element is neither an array nor contains 'results' array" << std::endl;
        throw std::runtime_error("'d' element in OData v2 response is not an array or doesn't contain a 'results' array.");
    } else {
        std::cout << "[DEBUG] [GET_VALUE_ARRAY] Processing OData v4 structure" << std::endl;
        // OData v4: {"value": [...]}
        auto value_array = yyjson_obj_get(root, "value");
        if (!value_array) {
            std::cout << "[DEBUG] [GET_VALUE_ARRAY] No 'value' element found in OData v4 response" << std::endl;
            throw std::runtime_error("No 'value' element found in OData v4 response.");
        }
        
        if (!yyjson_is_arr(value_array)) {
            std::cout << "[DEBUG] [GET_VALUE_ARRAY] 'value' element in OData v4 response is not an array" << std::endl;
            throw std::runtime_error("'value' element in OData v4 response is not an array.");
        }
        
        std::cout << "[DEBUG] [GET_VALUE_ARRAY] Successfully found v4 value array with " << yyjson_arr_size(value_array) << " items" << std::endl;
        return value_array;
    }
}

std::string ODataJsonContentMixin::GetMetadataContextUrl(yyjson_val* root) {
    if (odata_version == ODataVersion::V2) {
        // OData v2: Check for context in the root or d wrapper
        auto context = yyjson_obj_get(root, "@odata.context");
        if (!context) {
            auto d_wrapper = yyjson_obj_get(root, "d");
            if (d_wrapper) {
                context = yyjson_obj_get(d_wrapper, "@odata.context");
            }
        }
        
        if (!context) {
            throw std::runtime_error("No @odata.context found in OData v2 response, cannot get EDMX metadata.");
        }
        
        return std::string(yyjson_get_str(context));
    } else {
        // OData v4: {"@odata.context": "..."}
        auto context = yyjson_obj_get(root, "@odata.context");
        if (!context) {
            throw std::runtime_error("No @odata.context found in OData v4 response, cannot get EDMX metadata.");
        }
        
        return std::string(yyjson_get_str(context));
    }
}

std::optional<std::string> ODataJsonContentMixin::GetNextUrl(yyjson_val* root) {
    if (odata_version == ODataVersion::V2) {
        // OData v2: Check for __next in the d wrapper
        auto d_wrapper = yyjson_obj_get(root, "d");
        if (d_wrapper) {
            auto next_link = yyjson_obj_get(d_wrapper, "__next");
            if (next_link) {
                return std::string(yyjson_get_str(next_link));
            }
        }
        return std::nullopt;
    } else {
        // OData v4: {"@odata.nextLink": "..."}
        auto next_link = yyjson_obj_get(root, "@odata.nextLink");
        if (!next_link) {
            return std::nullopt;
        }
        
        return std::string(yyjson_get_str(next_link));
    }
}

// ----------------------------------------------------------------------

ODataEntitySetJsonContent::ODataEntitySetJsonContent(const std::string& content)
    : ODataJsonContentMixin(content)
{ 
    // Auto-detect and set OData version
    auto detected_version = DetectODataVersion(content);
    SetODataVersion(detected_version);
}

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
    std::cout << "[DEBUG] [ODATA_TO_ROWS] Starting ToRows with " << column_names.size() << " columns" << std::endl;
    for (size_t i = 0; i < column_names.size(); i++) {
        std::cout << "[DEBUG] [ODATA_TO_ROWS] Column " << i << ": " << column_names[i] 
                  << " (type: " << column_types[i].ToString() << ")" << std::endl;
    }

    auto root = yyjson_doc_get_root(doc.get());
    auto json_values = GetValueArray(root);
    if (!json_values) {
        throw std::runtime_error("No value array found in OData response, cannot get rows.");
    }

    std::cout << "[DEBUG] [ODATA_TO_ROWS] Found " << yyjson_arr_size(json_values) << " rows in JSON response" << std::endl;

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
        std::cout << "[DEBUG] [ODATA_TO_ROWS] Processing row " << i_row << std::endl;
        
        // Debug: print the JSON row content
        if (i_row == 0) { // Only print first row to avoid spam
            std::cout << "[DEBUG] [ODATA_TO_ROWS] First row JSON keys: ";
            size_t key_idx, key_max;
            yyjson_val *key, *child_val;
            yyjson_obj_foreach(json_row, key_idx, key_max, key, child_val) {
                std::cout << unsafe_yyjson_get_str(key) << " ";
            }
            std::cout << std::endl;
        }
        
        auto duck_row = std::vector<duckdb::Value>();
        duck_row.reserve(column_names.size());

        for (size_t i_col = 0; i_col < column_names.size(); i_col++) {
            auto column_name = column_names[i_col];
            auto column_type = column_types[i_col];
            
            std::cout << "[DEBUG] [ODATA_TO_ROWS] Processing column " << i_col << ": " << column_name 
                      << " (type: " << column_type.ToString() << ")" << std::endl;
            
            // Use JSON path evaluator for complex expressions like AddressInfo[1].City."Name"
            auto json_value = EvaluateJsonPath(json_row, column_name);
            if (!json_value) {
                std::cout << "[DEBUG] [ODATA_TO_ROWS] Column " << column_name << " not found in JSON, using null" << std::endl;
                auto duck_value = duckdb::Value().DefaultCastAs(column_type);
                duck_row.push_back(duck_value);
                continue;
            }
            
            try {
                auto duck_value = DeserializeJsonValue(json_value, column_type);
                std::cout << "[DEBUG] [ODATA_TO_ROWS] Successfully deserialized " << column_name 
                          << " to: " << duck_value.ToString() << std::endl;
                duck_row.push_back(duck_value);
            } catch (const std::exception& e) {
                std::cout << "[DEBUG] [ODATA_TO_ROWS] Failed to deserialize " << column_name 
                          << ": " << e.what() << std::endl;
                // Use a default value instead of failing the entire row
                auto duck_value = duckdb::Value().DefaultCastAs(column_type);
                duck_row.push_back(duck_value);
            }
        }

        duck_rows.push_back(duck_row);
        std::cout << "[DEBUG] [ODATA_TO_ROWS] Successfully processed row " << i_row << std::endl;
    }

    std::cout << "[DEBUG] [ODATA_TO_ROWS] Total rows processed: " << duck_rows.size() << std::endl;
    return duck_rows;
}

void ODataEntitySetJsonContent::PrettyPrint()
{
    ODataJsonContentMixin::PrettyPrint();
}

// ----------------------------------------------------------------------

ODataServiceJsonContent::ODataServiceJsonContent(const std::string& content)
    : ODataJsonContentMixin(content)
{ 
    // Auto-detect and set OData version
    auto detected_version = DetectODataVersion(content);
    SetODataVersion(detected_version);
}

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
    auto json_values = GetValueArray(root);
    if (!json_values) {
        throw std::runtime_error("No value array found in OData response, cannot get entity sets for service.");
    }

    auto ret = std::vector<ODataEntitySetReference>();
    ret.reserve(yyjson_arr_size(json_values));

    size_t i_row, max_row;
    yyjson_val *json_row;
    yyjson_arr_foreach(json_values, i_row, max_row, json_row) 
    {
        auto kind = GetStringProperty(json_row, "kind");
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
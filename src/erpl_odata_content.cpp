#include "erpl_odata_content.hpp"
#include "erpl_tracing.hpp"

#include <cpptrace/cpptrace.hpp>

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
    ERPL_TRACE_DEBUG("DETECT_VERSION", "Starting OData version detection");
    
    if (content.empty()) {
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Empty content, defaulting to V4");
        return ODataVersion::V4;
    }
    
    // Parse the JSON content to detect OData version
    auto doc = std::shared_ptr<yyjson_doc>(yyjson_read(content.c_str(), content.size(), 0), yyjson_doc_free);
    if (!doc) {
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Failed to parse JSON, defaulting to V4");
        // If we can't parse JSON, default to v4
        return ODataVersion::V4;
    }
    
    auto root = yyjson_doc_get_root(doc.get());
    if (!root || !yyjson_is_obj(root)) {
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Root is not an object, defaulting to V4");
        return ODataVersion::V4;
    }
    
    // Simple and reliable version detection based on top-level elements
    // OData v4: {"value": [...]}
    // OData v2: {"d": [...]}
    
    auto value_element = yyjson_obj_get(root, "value");
    if (value_element && yyjson_is_arr(value_element)) {
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Found 'value' array, detecting as V4");
        return ODataVersion::V4;
    }
    
    auto d_element = yyjson_obj_get(root, "d");
    if (d_element && yyjson_is_arr(d_element)) {
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Found 'd' array, detecting as V2");
        return ODataVersion::V2;
    }
    
    // Check for other v4 indicators
    auto context = yyjson_obj_get(root, "@odata.context");
    if (context && yyjson_is_str(context)) {
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Found '@odata.context', detecting as V4");
        return ODataVersion::V4;
    }
    
    // Check for other v2 indicators
    if (d_element && yyjson_is_obj(d_element)) {
        // Check if d contains results array (typical for v2 collections)
        auto results = yyjson_obj_get(d_element, "results");
        if (results && yyjson_is_arr(results)) {
            ERPL_TRACE_DEBUG("DETECT_VERSION", "Found 'd' object with 'results' array, detecting as V2");
            return ODataVersion::V2;
        }
        
        // Check if d contains __metadata (typical for v2 single entities)
        auto metadata = yyjson_obj_get(d_element, "__metadata");
        if (metadata && yyjson_is_obj(metadata)) {
            ERPL_TRACE_DEBUG("DETECT_VERSION", "Found 'd' object with '__metadata', detecting as V2");
            return ODataVersion::V2;
        }
        
        // If we have a 'd' wrapper but can't determine the structure, assume V2
        ERPL_TRACE_DEBUG("DETECT_VERSION", "Found 'd' wrapper, assuming V2");
        return ODataVersion::V2;
    }
    
    ERPL_TRACE_DEBUG("DETECT_VERSION", "No clear indicators found, defaulting to V4");
    // Default to v4 if we can't determine
    return ODataVersion::V4;
}

void ODataJsonContentMixin::ThrowTypeError(yyjson_val* json_value, const std::string& expected)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    auto actual = yyjson_get_type_desc(json_value);
    std::ostringstream ss;
    ss << "Expected JSON type '" << expected << "', but got type: '" << actual << "'";
    throw duckdb::ParserException(ss.str());
}

void ODataJsonContentMixin::PrettyPrint()
{
    if (!doc) {
        ERPL_TRACE_DEBUG("ODATA_CONTENT", "No document to pretty print");
        return;
    }
    
    auto flags = YYJSON_WRITE_PRETTY | YYJSON_WRITE_ESCAPE_UNICODE;
    auto json_doc_str = yyjson_write(doc.get(), flags, nullptr);
    if (json_doc_str) {
        ERPL_TRACE_DEBUG("ODATA_CONTENT", "Pretty print: " + std::string(json_doc_str));
        free(json_doc_str);
    } else {
        ERPL_TRACE_ERROR("ODATA_CONTENT", "Failed to generate pretty print");
    }
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonValue(yyjson_val* json_value, const duckdb::LogicalType& duck_type)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Debug: Log the expected type and actual JSON type
    auto json_type_desc = yyjson_get_type_desc(json_value);
    ERPL_TRACE_DEBUG("ODATA_CONTENT", "Deserializing JSON value: expected type=" + duck_type.ToString() + ", actual JSON type=" + std::string(json_type_desc));
    
    try {
        switch (duck_type.id()) {
            case duckdb::LogicalTypeId::DATE: {
                // Handle JSON null values by returning SQL NULL
                if (yyjson_is_null(json_value)) {
                    return duckdb::Value(); // Return SQL NULL value
                }
                
                // Expect ISO-8601 date string (YYYY-MM-DD) or epoch days as int/real
                if (yyjson_is_str(json_value)) {
                    std::string s = yyjson_get_str(json_value);
                    // Use DuckDB's string cast
                    return duckdb::Value(s).DefaultCastAs(duckdb::LogicalType(duckdb::LogicalTypeId::DATE));
                } else if (yyjson_is_int(json_value)) {
                    // Interpret as days since epoch
                    int64_t days = yyjson_get_int(json_value);
                    return duckdb::Value::DATE(duckdb::date_t(days));
                } else if (yyjson_is_real(json_value)) {
                    // Interpret as days since epoch (with fractional part)
                    double days = yyjson_get_real(json_value);
                    int64_t whole_days = static_cast<int64_t>(days);
                    return duckdb::Value::DATE(duckdb::date_t(whole_days));
                }
                ThrowTypeError(json_value, "date (string 'YYYY-MM-DD' or integer/real days)");
                return duckdb::Value::DATE(duckdb::date_t(0));
            }
            case duckdb::LogicalTypeId::TIMESTAMP: {
                // Handle JSON null values by returning SQL NULL
                if (yyjson_is_null(json_value)) {
                    return duckdb::Value(); // Return SQL NULL value
                }
                
                // Expect ISO-8601 datetime string or epoch seconds as int
                if (yyjson_is_str(json_value)) {
                    std::string s = yyjson_get_str(json_value);
                    // Use DuckDB's string cast
                    return duckdb::Value(s).DefaultCastAs(duckdb::LogicalType(duckdb::LogicalTypeId::TIMESTAMP));
                } else if (yyjson_is_int(json_value)) {
                    // Interpret as seconds since epoch
                    int64_t seconds = yyjson_get_int(json_value);
                    return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(seconds));
                } else if (yyjson_is_real(json_value)) {
                    // Interpret as seconds since epoch (with fractional seconds)
                    double seconds = yyjson_get_real(json_value);
                    int64_t whole_seconds = static_cast<int64_t>(seconds);
                    return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(whole_seconds));
                }
                ThrowTypeError(json_value, "timestamp (string ISO-8601 or integer/real seconds)");
                return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(0));
            }
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
                throw duckdb::ParserException("Unsupported DuckDB type: " + duck_type.ToString());
        }
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("ODATA_CONTENT", std::string("Failed to deserialize JSON value: ") + e.what());
        throw;
    }
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonBool(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_bool(json_value)) {
        return duckdb::Value::BOOLEAN(yyjson_get_bool(json_value));
    } else if (yyjson_is_str(json_value)) {
        std::string str_val = yyjson_get_str(json_value);
        if (str_val == "true" || str_val == "1") {
            return duckdb::Value::BOOLEAN(true);
        } else if (str_val == "0") {
            return duckdb::Value::BOOLEAN(false);
        }
    }
    
    ThrowTypeError(json_value, "boolean");
    return duckdb::Value::BOOLEAN(false); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt8(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= INT8_MIN && val <= INT8_MAX) {
            return duckdb::Value::TINYINT(static_cast<int8_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            int8_t val = static_cast<int8_t>(std::stoi(yyjson_get_str(json_value)));
            return duckdb::Value::TINYINT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "signed int8");
    return duckdb::Value::TINYINT(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt8(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= 0 && val <= UINT8_MAX) {
            return duckdb::Value::UTINYINT(static_cast<uint8_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            uint8_t val = static_cast<uint8_t>(std::stoul(yyjson_get_str(json_value)));
            return duckdb::Value::UTINYINT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "unsigned int8");
    return duckdb::Value::UTINYINT(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt16(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= INT16_MIN && val <= INT16_MAX) {
            return duckdb::Value::SMALLINT(static_cast<int16_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            int16_t val = static_cast<int16_t>(std::stoi(yyjson_get_str(json_value)));
            return duckdb::Value::SMALLINT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "signed int16");
    return duckdb::Value::SMALLINT(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt16(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= 0 && val <= UINT16_MAX) {
            return duckdb::Value::USMALLINT(static_cast<uint16_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            uint16_t val = static_cast<uint16_t>(std::stoul(yyjson_get_str(json_value)));
            return duckdb::Value::USMALLINT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "unsigned int16");
    return duckdb::Value::USMALLINT(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt32(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= INT32_MIN && val <= INT32_MAX) {
            return duckdb::Value::INTEGER(static_cast<int32_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            int32_t val = static_cast<int32_t>(std::stoi(yyjson_get_str(json_value)));
            return duckdb::Value::INTEGER(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "signed int32");
    return duckdb::Value::INTEGER(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt32(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= 0 && val <= UINT32_MAX) {
            return duckdb::Value::UINTEGER(static_cast<uint32_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            uint32_t val = static_cast<uint32_t>(std::stoul(yyjson_get_str(json_value)));
            return duckdb::Value::UINTEGER(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "unsigned int32");
    return duckdb::Value::UINTEGER(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonSignedInt64(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        return duckdb::Value::BIGINT(yyjson_get_int(json_value));
    } else if (yyjson_is_str(json_value)) {
        try {
            int64_t val = std::stoll(yyjson_get_str(json_value));
            return duckdb::Value::BIGINT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "signed int64");
    return duckdb::Value::BIGINT(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonUnsignedInt64(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_int(json_value)) {
        int64_t val = yyjson_get_int(json_value);
        if (val >= 0) {
            return duckdb::Value::UBIGINT(static_cast<uint64_t>(val));
        }
    } else if (yyjson_is_str(json_value)) {
        try {
            uint64_t val = std::stoull(yyjson_get_str(json_value));
            return duckdb::Value::UBIGINT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "unsigned int64");
    return duckdb::Value::UBIGINT(0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonFloat(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_real(json_value)) {
        return duckdb::Value::FLOAT(static_cast<float>(yyjson_get_real(json_value)));
    } else if (yyjson_is_int(json_value)) {
        return duckdb::Value::FLOAT(static_cast<float>(yyjson_get_int(json_value)));
    } else if (yyjson_is_str(json_value)) {
        try {
            float val = std::stof(yyjson_get_str(json_value));
            return duckdb::Value::FLOAT(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "float");
    return duckdb::Value::FLOAT(0.0f); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonDouble(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_real(json_value)) {
        return duckdb::Value::DOUBLE(yyjson_get_real(json_value));
    } else if (yyjson_is_int(json_value)) {
        return duckdb::Value::DOUBLE(static_cast<double>(yyjson_get_int(json_value)));
    } else if (yyjson_is_str(json_value)) {
        try {
            double val = std::stod(yyjson_get_str(json_value));
            return duckdb::Value::DOUBLE(val);
        } catch (const std::exception&) {
            // Fall through to error
        }
    }
    
    ThrowTypeError(json_value, "double");
    return duckdb::Value::DOUBLE(0.0); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonString(yyjson_val* json_value)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    if (yyjson_is_str(json_value)) {
        const char* str_ptr = yyjson_get_str(json_value);
        size_t str_len = yyjson_get_len(json_value);
        
        if (!str_ptr) {
            ERPL_TRACE_DEBUG("ODATA_CONTENT", "String pointer is null, returning empty string");
            return duckdb::Value("");
        }
        
        // Create a safe copy of the string
        std::string safe_string(str_ptr, str_len);
        
        try {
            return duckdb::Value(safe_string);
        } catch (const std::exception& e) {
            ERPL_TRACE_ERROR("ODATA_CONTENT", "Failed to create VARCHAR value: " + std::string(e.what()));
            return duckdb::Value("");
        }
    } else if (yyjson_is_int(json_value)) {
        return duckdb::Value(std::to_string(yyjson_get_int(json_value)));
    } else if (yyjson_is_real(json_value)) {
        return duckdb::Value(std::to_string(yyjson_get_real(json_value)));
    } else if (yyjson_is_bool(json_value)) {
        return duckdb::Value(yyjson_get_bool(json_value) ? "true" : "false");
    } else if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return proper null value instead of empty string
    }
    
    ThrowTypeError(json_value, "string");
    return duckdb::Value(""); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonEnum(yyjson_val* json_value, const duckdb::LogicalType& duck_type)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (yyjson_is_str(json_value)) {
        std::string enum_value = yyjson_get_str(json_value);
        try {
            // Find the enum index for the given value
            uint64_t enum_index = 0;
            for (idx_t i = 0; i < duckdb::EnumType::GetSize(duck_type); i++) {
                if (duckdb::EnumType::GetString(duck_type, i).GetString() == enum_value) {
                    enum_index = i;
                    break;
                }
            }
            return duckdb::Value::ENUM(enum_index, duck_type);
        } catch (const std::exception& e) {
            ERPL_TRACE_ERROR("ODATA_CONTENT", "Failed to create ENUM value: " + std::string(e.what()));
            return duckdb::Value(enum_value);
        }
    }
    
    ThrowTypeError(json_value, "enum");
    return duckdb::Value(""); // Unreachable
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonArray(yyjson_val* json_value, const duckdb::LogicalType& duck_type)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (!yyjson_is_arr(json_value)) {
        ThrowTypeError(json_value, "array");
        // Use the proper constructor for empty lists with explicit type
        auto child_type = duckdb::ListType::GetChildType(duck_type);
        return duckdb::Value::LIST(child_type, duckdb::vector<duckdb::Value>());
    }
    
    auto child_type = duckdb::ListType::GetChildType(duck_type);
    duckdb::vector<duckdb::Value> list_values;
    
    size_t idx, max;
    yyjson_val* json_child_val;
    yyjson_arr_foreach(json_value, idx, max, json_child_val) {
        try {
            auto child_value = DeserializeJsonValue(json_child_val, child_type);
            list_values.push_back(child_value);
        } catch (const std::exception& e) {
            ERPL_TRACE_ERROR("ODATA_CONTENT", "Failed to deserialize array element " + std::to_string(idx) + ": " + std::string(e.what()));
            // Continue with other elements
        }
    }
    
    // Use the proper constructor that handles both empty and non-empty lists
    if (list_values.empty()) {
        return duckdb::Value::LIST(child_type, list_values);
    } else {
        return duckdb::Value::LIST(list_values);
    }
}

duckdb::Value ODataJsonContentMixin::DeserializeJsonObject(yyjson_val* json_value, const duckdb::LogicalType& duck_type)
{
    if (!json_value) {
        throw duckdb::ParserException("JSON value is null");
    }
    
    // Handle JSON null values by returning SQL NULL
    if (yyjson_is_null(json_value)) {
        return duckdb::Value(); // Return SQL NULL value
    }
    
    if (!yyjson_is_obj(json_value)) {
        ThrowTypeError(json_value, "object");
        return duckdb::Value::STRUCT({}); // Unreachable
    }
    
    auto child_types = duckdb::StructType::GetChildTypes(duck_type);
    duckdb::child_list_t<duckdb::Value> struct_values;
    
    size_t idx, max;
    yyjson_val* json_key, *json_val;
    yyjson_obj_foreach(json_value, idx, max, json_key, json_val) {
        if (yyjson_is_str(json_key)) {
            std::string key = yyjson_get_str(json_key);
            
            // Find matching child type
            duckdb::LogicalType child_type = duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR); // Default
            for (const auto& child_type_pair : child_types) {
                if (child_type_pair.first == key) {
                    child_type = child_type_pair.second;
                    break;
                }
            }
            
            try {
                auto value = DeserializeJsonValue(json_val, child_type);
                struct_values.emplace_back(key, value);
            } catch (const std::exception& e) {
                ERPL_TRACE_ERROR("ODATA_CONTENT", "Failed to deserialize object field '" + key + "': " + std::string(e.what()));
                // Continue with other fields
            }
        }
    }
    
    return duckdb::Value::STRUCT(struct_values);
}

std::string ODataJsonContentMixin::MetadataContextUrl()
{
    if (!doc) {
        return "";
    }
    
    auto root = yyjson_doc_get_root(doc.get());
    if (!root || !yyjson_is_obj(root)) {
        return "";
    }
    
    return GetMetadataContextUrl(root);
}

std::optional<std::string> ODataJsonContentMixin::NextUrl()
{
    if (!doc) {
        return std::nullopt;
    }
    
    auto root = yyjson_doc_get_root(doc.get());
    if (!root || !yyjson_is_obj(root)) {
        return std::nullopt;
    }
    
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
    ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "OData version: " + std::string(odata_version == ODataVersion::V2 ? "V2" : "V4"));
    
    if (odata_version == ODataVersion::V2) {
        ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "Processing OData v2 structure");
        
        // OData v2: {"d": [...]} or {"d": {"results": [...]}}
        auto d_wrapper = yyjson_obj_get(root, "d");
        if (!d_wrapper) {
            ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "No 'd' wrapper found in OData v2 response");
            throw std::runtime_error("No 'd' wrapper found in OData v2 response.");
        }
        
        // Check if d is directly an array (common case)
        if (yyjson_is_arr(d_wrapper)) {
            ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "Found 'd' as direct array with " + std::to_string(yyjson_arr_size(d_wrapper)) + " items");
            return d_wrapper;
        }
        
        // Check if d contains a "results" array (traditional v2 format)
        if (yyjson_is_obj(d_wrapper)) {
            auto results = yyjson_obj_get(d_wrapper, "results");
            if (results && yyjson_is_arr(results)) {
                ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "Found 'd' object with 'results' array containing " + std::to_string(yyjson_arr_size(results)) + " items");
                return results;
            }
        }
        
        ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "'d' element is neither an array nor contains 'results' array");
        throw std::runtime_error("'d' element in OData v2 response is not an array or doesn't contain a 'results' array.");
    } else {
        ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "Processing OData v4 structure");
        // OData v4: {"value": [...]}
        auto value_array = yyjson_obj_get(root, "value");
        if (!value_array) {
            ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "No 'value' element found in OData v4 response");
            throw std::runtime_error("No 'value' element found in OData v4 response.");
        }
        
        if (!yyjson_is_arr(value_array)) {
            ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "'value' element in OData v4 response is not an array");
            throw std::runtime_error("'value' element in OData v4 response is not an array.");
        }
        
        ERPL_TRACE_DEBUG("GET_VALUE_ARRAY", "Successfully found v4 value array with " + std::to_string(yyjson_arr_size(value_array)) + " items");
        return value_array;
    }
}

std::string ODataJsonContentMixin::GetMetadataContextUrl(yyjson_val* root) {
    if (!root) {
        return "";
    }
    
    auto context = yyjson_obj_get(root, "@odata.context");
    if (context && yyjson_is_str(context)) {
        return yyjson_get_str(context);
    }
    
    return "";
}

std::optional<std::string> ODataJsonContentMixin::GetNextUrl(yyjson_val* root) {
    if (!root) {
        return std::nullopt;
    }
    
    auto next_link = yyjson_obj_get(root, "@odata.nextLink");
    if (next_link && yyjson_is_str(next_link)) {
        return yyjson_get_str(next_link);
    }
    
    // Check for v2 next link
    auto next_link_v2 = yyjson_obj_get(root, "__next");
    if (next_link_v2 && yyjson_is_str(next_link_v2)) {
        return yyjson_get_str(next_link_v2);
    }
    
    return std::nullopt;
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
    ERPL_TRACE_DEBUG("ODATA_TO_ROWS", duckdb::StringUtil::Format("Starting ToRows with %d columns", column_names.size()));

    auto root = yyjson_doc_get_root(doc.get());
    auto json_values = GetValueArray(root);
    if (!json_values) {
        throw std::runtime_error("No value array found in OData response, cannot get rows.");
    }

    ERPL_TRACE_DEBUG("ODATA_TO_ROWS", duckdb::StringUtil::Format("Found %d rows in JSON response", yyjson_arr_size(json_values)));

    auto duck_rows = std::vector<std::vector<duckdb::Value>>();
    duck_rows.reserve(yyjson_arr_size(json_values));

    size_t i_row, max_row;
    yyjson_val *json_row;
    yyjson_arr_foreach(json_values, i_row, max_row, json_row) 
    {
        auto duck_row = std::vector<duckdb::Value>();
        duck_row.reserve(column_names.size());

        for (size_t i_col = 0; i_col < column_names.size(); i_col++) {
            auto column_name = column_names[i_col];
            auto column_type = column_types[i_col];
            
            // Simple property lookup - no complex JSON path evaluation needed
            auto json_value = yyjson_obj_get(json_row, column_name.c_str());
            if (!json_value) {
                // Column not found, use null value
                auto duck_value = duckdb::Value().DefaultCastAs(column_type);
                duck_row.push_back(duck_value);
                continue;
            }
            
            try {
                auto duck_value = DeserializeJsonValue(json_value, column_type);
                duck_row.push_back(duck_value);
            } catch (const std::exception& e) {
                ERPL_TRACE_ERROR("ODATA_TO_ROWS", duckdb::StringUtil::Format("Failed to deserialize %s: %s", column_name, e.what()));
                // Use a default value instead of failing the entire row
                auto duck_value = duckdb::Value().DefaultCastAs(column_type);
                duck_row.push_back(duck_value);
            }
        }

        duck_rows.push_back(duck_row);
    }

    ERPL_TRACE_DEBUG("ODATA_TO_ROWS", duckdb::StringUtil::Format("Total rows processed: %d", duck_rows.size()));
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
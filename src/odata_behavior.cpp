#include "odata_behavior.hpp"
#include "odata_content.hpp"
#include <optional>

namespace erpl_web {

ODataVersion ODataVersionDetector::detectFromJson(const std::string &content) const {
    // Delegate to existing mixin detection to preserve behavior
    return ODataJsonContentMixin::DetectODataVersion(content);
}

ODataVersion ODataVersionDetector::detectFromEdmx(const std::string &edmx_xml) const {
    // Parse just enough to get version; re-use existing Edmx::FromXml
    auto edmx = Edmx::FromXml(edmx_xml);
    return edmx.GetVersion();
}

static std::optional<std::string> extract_next_v4(yyjson_val *root) {
    if (!root) return std::nullopt;
    auto next_link = yyjson_obj_get(root, "@odata.nextLink");
    if (next_link && yyjson_is_str(next_link)) {
        return yyjson_get_str(next_link);
    }
    return std::nullopt;
}

static std::optional<std::string> extract_next_v2(yyjson_val *root) {
    if (!root) return std::nullopt;
    auto next_link = yyjson_obj_get(root, "__next");
    if (next_link && yyjson_is_str(next_link)) {
        return yyjson_get_str(next_link);
    }
    return std::nullopt;
}

static std::string extract_context_v4(yyjson_val *root) {
    if (!root) return "";
    auto ctx = yyjson_obj_get(root, "@odata.context");
    if (ctx && yyjson_is_str(ctx)) {
        return yyjson_get_str(ctx);
    }
    return "";
}

static yyjson_val* locate_collection_v4(yyjson_val *root) {
    if (!root) return nullptr;
    auto value = yyjson_obj_get(root, "value");
    if (value && yyjson_is_arr(value)) return value;
    return nullptr;
}

static yyjson_val* locate_collection_v2(yyjson_val *root) {
    if (!root) return nullptr;
    auto d = yyjson_obj_get(root, "d");
    if (!d) return nullptr;
    if (yyjson_is_arr(d)) return d;
    if (yyjson_is_obj(d)) {
        auto results = yyjson_obj_get(d, "results");
        if (results && yyjson_is_arr(results)) return results;
    }
    return nullptr;
}

const ODataProfile& ODataBehaviorRegistry::v2() {
    static ODataProfile p{
        /*locate_collection*/ locate_collection_v2,
        /*extract_next*/     extract_next_v2,
        /*extract_context*/  extract_context_v4 // v2 often lacks context; keep empty
    };
    return p;
}

const ODataProfile& ODataBehaviorRegistry::v4() {
    static ODataProfile p{
        /*locate_collection*/ locate_collection_v4,
        /*extract_next*/     extract_next_v4,
        /*extract_context*/  extract_context_v4
    };
    return p;
}

const ODataProfile& ODataBehaviorRegistry::forVersion(ODataVersion v) {
    return (v == ODataVersion::V2) ? v2() : v4();
}

} // namespace erpl_web



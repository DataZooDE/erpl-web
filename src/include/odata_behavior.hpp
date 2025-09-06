#pragma once

#include "yyjson.hpp"
#include "odata_edm.hpp"
#include <functional>
#include <optional>
#include <string>

using namespace duckdb_yyjson;

namespace erpl_web {

// Lightweight detector wrapper to centralize version detection
class ODataVersionDetector {
public:
    ODataVersion detectFromJson(const std::string &content) const;
    ODataVersion detectFromEdmx(const std::string &edmx_xml) const;
};

// Profile encapsulates version-specific behaviors; favor composition
struct ODataProfile {
    std::function<yyjson_val*(yyjson_val*)> locate_collection;
    std::function<std::optional<std::string>(yyjson_val*)> extract_next;
    std::function<std::string(yyjson_val*)> extract_context;
};

class ODataBehaviorRegistry {
public:
    static const ODataProfile& v2();
    static const ODataProfile& v4();
    static const ODataProfile& forVersion(ODataVersion v);
};

} // namespace erpl_web



#include "url_parser.hpp"
#include <algorithm>
#include <cctype>

namespace erpl_web {

// Regex patterns for URL parsing
const std::regex UrlParser::SAC_URL_PATTERN(
    "^(https?)://([a-z0-9-]+)\\.([a-z0-9]+)\\.sapanalytics\\.cloud(/.*)?$");

const std::regex UrlParser::DATASPHERE_URL_PATTERN(
    "^(https?)://([a-z0-9-]+)\\.([a-z0-9]+)\\.hcs\\.cloud\\.sap(/.*)?$");

const std::regex UrlParser::OAUTH_PATTERN(
    "^(https?)://([a-z0-9-]+)\\.([a-z0-9]+)\\.(sapanalytics\\.cloud|hcs\\.cloud\\.sap)/oauth/");

const std::regex UrlParser::ODATA_PATTERN(
    "^(https?)://.*(/api/v1/odata/|/dwaas-core/api/v1/|/api/v1/dwc/)");

const std::regex UrlParser::GENERIC_URL_PATTERN(
    "^(https?)://([^/:]+)(:[0-9]+)?(/.*)?$");

std::optional<UrlParser> UrlParser::Parse(const std::string& url) {
    std::smatch matches;
    UrlParser parser;

    // Try SAC pattern first
    if (std::regex_match(url, matches, SAC_URL_PATTERN)) {
        parser.components_["scheme"] = matches[1];
        parser.components_["tenant"] = matches[2];
        parser.components_["region"] = matches[3];
        parser.components_["domain"] = "sapanalytics.cloud";
        if (matches.size() > 4 && matches[4].matched) {
            parser.components_["path"] = matches[4];
        }
        return parser;
    }

    // Try Datasphere pattern
    if (std::regex_match(url, matches, DATASPHERE_URL_PATTERN)) {
        parser.components_["scheme"] = matches[1];
        parser.components_["tenant"] = matches[2];
        parser.components_["datacenter"] = matches[3];
        parser.components_["domain"] = "hcs.cloud.sap";
        if (matches.size() > 4 && matches[4].matched) {
            parser.components_["path"] = matches[4];
        }
        return parser;
    }

    // Try generic pattern as fallback
    if (std::regex_match(url, matches, GENERIC_URL_PATTERN)) {
        parser.components_["scheme"] = matches[1];
        parser.components_["host"] = matches[2];
        if (matches.size() > 3 && matches[3].matched) {
            parser.components_["port"] = matches[3];
        }
        if (matches.size() > 4 && matches[4].matched) {
            parser.components_["path"] = matches[4];
        }
        return parser;
    }

    return std::nullopt;
}

std::string UrlParser::GetComponent(const std::string& component) const {
    auto it = components_.find(component);
    if (it != components_.end()) {
        return it->second;
    }
    return "";
}

std::string UrlParser::Reconstruct(
    const std::map<std::string, std::string>& component_overrides) const {

    auto components = components_;
    for (const auto& [key, value] : component_overrides) {
        components[key] = value;
    }

    std::string result;

    // Reconstruct based on available components
    if (!components["scheme"].empty()) {
        result = components["scheme"];
        result += "://";
    }

    if (!components["tenant"].empty()) {
        result += components["tenant"];
        result += ".";
    }

    if (!components["region"].empty()) {
        result += components["region"];
    } else if (!components["datacenter"].empty()) {
        result += components["datacenter"];
    }

    if (!components["domain"].empty()) {
        result += ".";
        result += components["domain"];
    }

    if (!components["path"].empty()) {
        result += components["path"];
    }

    return result;
}

std::optional<std::pair<std::string, std::string>> UrlParser::ExtractTenantRegion(
    const std::string& url) {

    auto parsed = Parse(url);
    if (!parsed) {
        return std::nullopt;
    }

    auto tenant = parsed->GetComponent("tenant");
    auto region = parsed->GetComponent("region");

    if (tenant.empty() || region.empty()) {
        return std::nullopt;
    }

    return std::make_pair(tenant, region);
}

std::optional<std::pair<std::string, std::string>> UrlParser::ExtractTenantDatacenter(
    const std::string& url) {

    auto parsed = Parse(url);
    if (!parsed) {
        return std::nullopt;
    }

    auto tenant = parsed->GetComponent("tenant");
    auto datacenter = parsed->GetComponent("datacenter");

    if (tenant.empty() || datacenter.empty()) {
        return std::nullopt;
    }

    return std::make_pair(tenant, datacenter);
}

bool UrlParser::MatchesPattern(const std::string& url, const std::string& pattern_type) {
    if (pattern_type == "sac") {
        return std::regex_match(url, SAC_URL_PATTERN);
    } else if (pattern_type == "datasphere") {
        return std::regex_match(url, DATASPHERE_URL_PATTERN);
    } else if (pattern_type == "oauth") {
        return std::regex_match(url, OAUTH_PATTERN);
    } else if (pattern_type == "odata") {
        return std::regex_match(url, ODATA_PATTERN);
    }
    return false;
}

std::string UrlParser::Normalize(const std::string& url) {
    std::string result = url;

    // Convert to lowercase
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Remove trailing slash if present (but keep single slash after domain)
    while (result.length() > 1 && result.back() == '/' &&
           result[result.length() - 2] != '/') {
        result.pop_back();
    }

    return result;
}

} // namespace erpl_web

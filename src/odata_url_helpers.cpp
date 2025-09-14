#include "odata_url_helpers.hpp"
#include <sstream>

namespace erpl_web {

std::string ODataUrlResolver::resolveMetadataUrl(const HttpUrl &request_url,
                                                 const std::string &odata_context_if_any) const
{
    // Prefer @odata.context without fragment if provided
    if (!odata_context_if_any.empty()) {
        auto ctx = odata_context_if_any;
        auto hash_pos = ctx.find('#');
        if (hash_pos != std::string::npos) {
            ctx = ctx.substr(0, hash_pos);
        }
        auto merged = HttpUrl::MergeWithBaseUrlIfRelative(request_url, ctx);
        return merged.ToString();
    }

    // Fallback: derive $metadata next to the service root; keep Datasphere-specific rules
    HttpUrl base(request_url);
    auto path = base.Path();
    base.Query("");

    if (path.find("/V2/") != std::string::npos) {
        auto svc_pos = path.find(".svc");
        if (svc_pos != std::string::npos) {
            auto service_root = path.substr(0, svc_pos + 4);
            base.Path(service_root + "/$metadata");
        } else {
            auto v2_pos = path.find("/V2/");
            auto service_pos = path.find("/", v2_pos + 4);
            if (service_pos != std::string::npos) {
                auto service_root = path.substr(0, service_pos);
                base.Path(service_root + "/$metadata");
            } else {
                base.Path(path + "/$metadata");
            }
        }
    } else if (path.find("/V4/") != std::string::npos) {
        auto v4_pos = path.find("/V4/");
        auto service_pos = path.find("/", v4_pos + 4);
        if (service_pos != std::string::npos) {
            auto service_root = path.substr(0, service_pos);
            base.Path(service_root + "/$metadata");
        } else {
            base.Path(path + "/$metadata");
        }
    } else if (path.find("/api/v1/dwc/consumption/relational/") != std::string::npos) {
        auto datasphere_pos = path.find("/api/v1/dwc/consumption/relational/");
        auto after_relational = datasphere_pos + 35;
        std::vector<std::string> segments;
        size_t i = after_relational;
        while (i < path.size()) {
            size_t j = path.find('/', i);
            if (j == std::string::npos) j = path.size();
            if (j > i) segments.push_back(path.substr(i, j - i));
            i = j + 1;
        }
        if (segments.size() >= 2) {
            auto tenant = segments[0];
            auto asset  = segments[1];
            auto service_root = std::string("/api/v1/dwc/consumption/relational/") + tenant + "/" + asset;
            base.Path(service_root + "/$metadata");
        } else {
            auto tenant_end = path.find("/", after_relational);
            if (tenant_end != std::string::npos) {
                auto service_root = path.substr(0, tenant_end);
                base.Path(service_root + "/$metadata");
            } else {
                base.Path(path + "/$metadata");
            }
        }
    } else {
        auto last_slash = path.find_last_of('/');
        if (last_slash != std::string::npos && last_slash > 0) {
            auto service_root = path.substr(0, last_slash);
            base.Path(service_root + "/$metadata");
        } else {
            base.Path("/$metadata");
        }
    }

    return base.ToString();
}

HttpUrl InputParametersFormatter::addParams(const HttpUrl &url,
                                           const std::map<std::string, std::string> &params) const
{
    if (params.empty()) return url;
    HttpUrl modified_url = url;
    std::string current_path = modified_url.Path();
    std::string new_path = current_path;

    std::string params_string = "(";
    bool first = true;
    for (const auto &kv : params) {
        const auto &key = kv.first;
        const auto &value = kv.second;
        if (!first) params_string += ",";
        first = false;
        const bool only_digits_and_sign = (value.find_first_not_of("0123456789-") == std::string::npos);
        const bool is_decimal = (value.find_first_not_of("0123456789.-") == std::string::npos && value.find('.') != std::string::npos);
        const bool is_date = (only_digits_and_sign && value.find('-') != std::string::npos && value.length() == 10);
        const bool is_integer = (only_digits_and_sign && !is_date);
        if (is_decimal || is_integer || is_date) {
            params_string += key + "=" + value;
        } else {
            params_string += key + "='" + value + "'";
        }
    }
    params_string += ")";

    const bool already_has_set = (current_path.length() >= 4 && current_path.substr(current_path.length() - 4) == "/Set");
    if (already_has_set) {
        new_path = current_path.substr(0, current_path.length() - 4) + params_string + "/Set";
    } else {
        new_path += params_string + "/Set";
    }

    std::string params_without_set = params_string;
    if (params_without_set.length() >= 4 && params_without_set.substr(params_without_set.length() - 4) == "/Set") {
        params_without_set = params_without_set.substr(0, params_without_set.length() - 4);
    }
    if (current_path.find(params_without_set) != std::string::npos) {
        return url; // avoid duplicates
    }
    modified_url.Path(new_path);
    return modified_url;
}

// ----------------------- ODataUrlCodec -----------------------

static inline bool has_format_param(const std::string &query) {
    if (query.empty() || query == "?") return false;
    std::string q = query;
    if (!q.empty() && q[0] == '?') q = q.substr(1);
    std::istringstream iss(q);
    std::string kv;
    while (std::getline(iss, kv, '&')) {
        auto pos = kv.find('=');
        std::string key = pos == std::string::npos ? kv : kv.substr(0, pos);
        if (key == "$format") return true;
    }
    return false;
}

std::string ODataUrlCodec::encodeQueryValue(const std::string &value) {
    // Use httplib encode_query_param to encode query values
    return duckdb_httplib_openssl::detail::encode_query_param(value);
}

std::string ODataUrlCodec::decodeQueryValue(const std::string &value) {
    // Use httplib decode_url; do not convert '+' into spaces for safety
    static const std::set<char> exclude{};
    return duckdb_httplib_openssl::detail::decode_url(value, false, exclude);
}

void ODataUrlCodec::ensureJsonFormat(HttpUrl &url) {
    auto q = url.Query();
    if (!has_format_param(q)) {
        if (q.empty()) {
            url.Query("?$format=json");
        } else {
            if (q == "?") {
                url.Query("?$format=json");
            } else if (q[0] == '?') {
                url.Query(q + "&$format=json");
            } else {
                url.Query("?" + q + "&$format=json");
            }
        }
    }
}

std::string ODataUrlCodec::encodeFilterExpression(const std::string &filter_expr) {
    // Encode the entire filter expression as a query value so that spaces and quotes are percent-encoded
    // This avoids 400s on services that require strict URL encoding of the $filter value
    return duckdb_httplib_openssl::detail::encode_url(filter_expr);
}

// Normalize $expand syntax:
// - Ensure nested option keys are prefixed with '$' (expand, select, filter, orderby, top, skip, levels, count, search, apply)
// - Preserve nested structure and commas/parentheses
// This is a best-effort normalizer; it does not fully parse OData grammar but fixes common cases.
std::string ODataUrlCodec::normalizeExpand(const std::string &expand_value) {
    if (expand_value.empty()) return expand_value;

    // Known option keys that require a '$' prefix
    static const std::set<std::string> option_keys = {
        "expand", "select", "filter", "orderby", "top", "skip", "levels", "count", "search", "apply"
    };

    std::string out;
    out.reserve(expand_value.size() + 8);

    size_t i = 0;
    while (i < expand_value.size()) {
        // Copy navigation/property path until options or separator
        while (i < expand_value.size()) {
            char c = expand_value[i];
            if (c == '(' || c == ',') break;
            out += c;
            i++;
        }

        if (i >= expand_value.size()) break;

        if (expand_value[i] == ',') {
            out += ',';
            i++;
            continue;
        }

        if (expand_value[i] == '(') {
            // Parse options segment until matching ')'
            out += '(';
            i++; // after '('
            size_t depth = 1;
            std::string opt;
            while (i < expand_value.size() && depth > 0) {
                char c = expand_value[i];
                if (c == '(') { depth++; opt += c; i++; continue; }
                if (c == ')') {
                    depth--;
                    if (depth == 0) { i++; break; }
                    opt += c; i++; continue;
                }
                opt += c; i++;
            }

            // Now normalize options list like "expand=Services();select=Id" (semicolon or comma separated)
            // Split on ';' and ',' but not inside nested parentheses
            std::vector<std::string> parts;
            {
                size_t j = 0, start = 0; int d = 0;
                while (j <= opt.size()) {
                    if (j == opt.size() || ((opt[j] == ';' || opt[j] == ',') && d == 0)) {
                        if (j > start) parts.emplace_back(opt.substr(start, j - start));
                        if (j < opt.size()) parts.emplace_back(std::string(1, opt[j]));
                        j++; start = j; continue;
                    }
                    if (opt[j] == '(') d++;
                    else if (opt[j] == ')') d--;
                    j++;
                }
            }

            // Rebuild with $-prefixed keys (default to $expand when key is missing)
            for (auto &p : parts) {
                if (p == ";" || p == ",") { out += p; continue; }
                auto trimmed = p;
                // trim spaces
                size_t s = trimmed.find_first_not_of(' ');
                size_t e = trimmed.find_last_not_of(' ');
                if (s == std::string::npos) { out += p; continue; }
                std::string core = trimmed.substr(s, e - s + 1);
                auto eq = core.find('=');
                if (eq != std::string::npos) {
                    std::string key = core.substr(0, eq);
                    std::string val = core.substr(eq + 1);
                    // If key is missing (e.g., "=Services()"), assume $expand
                    if (key.empty()) {
                        out += "$expand=" + val;
                    } else {
                        // strip leading '$' from key for lookup
                        std::string key_stripped = key;
                        if (!key_stripped.empty() && key_stripped[0] == '$') key_stripped = key_stripped.substr(1);
                        std::string key_lower;
                        key_lower.reserve(key_stripped.size());
                        for (char ch : key_stripped) key_lower.push_back(std::tolower(ch));
                        if (option_keys.count(key_lower)) {
                            out += "$" + key_lower + "=" + val;
                        } else {
                            // not a known option key, keep as-is
                            out += core;
                        }
                    }
                } else {
                    out += core; // no '=' -> keep
                }
            }

            out += ')';
        }
    }
    return out.empty() ? expand_value : out;
}

// Normalize expand and percent-encode ONLY nested $filter values inside option sections
std::string ODataUrlCodec::normalizeAndSanitizeExpand(const std::string &expand_value) {
    if (expand_value.empty()) return expand_value;
    // First normalize structure
    std::string normalized = normalizeExpand(expand_value);

    // Walk through normalized string and encode values of $filter=... inside parentheses
    std::string out;
    out.reserve(normalized.size() + 16);
    size_t i = 0;
    while (i < normalized.size()) {
        char c = normalized[i];
        if (c == '(') {
            // Inside options segment until matching ')'
            out.push_back(c);
            i++;
            // Process options: key=value;key2=value2
            while (i < normalized.size() && normalized[i] != ')') {
                // Extract key
                size_t key_start = i;
                while (i < normalized.size() && normalized[i] != '=' && normalized[i] != ';' && normalized[i] != ')') i++;
                std::string key = normalized.substr(key_start, i - key_start);
                // Trim spaces
                while (!key.empty() && key.front() == ' ') key.erase(key.begin());
                while (!key.empty() && key.back() == ' ') key.pop_back();
                if (i < normalized.size() && normalized[i] == '=') {
                    // Read value until ';' or ')'
                    i++; // skip '='
                    size_t val_start = i;
                    int depth = 0;
                    while (i < normalized.size()) {
                        if (normalized[i] == '(') depth++;
                        if (normalized[i] == ')') { if (depth == 0) break; depth--; }
                        if (depth == 0 && normalized[i] == ';') break;
                        i++;
                    }
                    std::string value = normalized.substr(val_start, i - val_start);
                    if (key == "$filter") {
                        // Encode filter expression only
                        value = encodeFilterExpression(value);
                    }
                    out += key;
                    out.push_back('=');
                    out += value;
                } else {
                    // No value (malformed), just copy key
                    out += key;
                }
                if (i < normalized.size() && normalized[i] == ';') {
                    out.push_back(';');
                    i++;
                }
            }
            if (i < normalized.size() && normalized[i] == ')') {
                out.push_back(')');
                i++;
            }
        } else {
            out.push_back(c);
            i++;
        }
    }
    return out;
}

} // namespace erpl_web



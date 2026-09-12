#include "odata_url_helpers.hpp"
#include "odata_text_scanning.hpp"
#include "yyjson.hpp"
#include <sstream>

namespace erpl_web {

std::shared_ptr<HttpClient> CreateODataHttpClient() {
    HttpParams http_params;
    http_params.url_encode = false;
    return std::make_shared<HttpClient>(http_params);
}

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

        // A relative context is resolved the way RFC 3986 resolves any relative
        // reference: against the base's *directory*, so the last segment of the
        // request path is replaced rather than appended to. OData services state
        // the context of an entity set response as the bare "$metadata", and
        // appending that to ".../0001/Travel" produces ".../0001/Travel/$metadata",
        // which SAP Gateway answers with 400. The metadata document is then
        // unreachable and every column falls back to VARCHAR without a word.
        //
        // The request's query string is dropped for the same reason: $format,
        // $filter and $skiptoken describe the entity request, not the metadata
        // document. See GitHub #152.
        const bool is_absolute_reference =
            ctx.find("://") != std::string::npos || (!ctx.empty() && ctx.front() == '/');
        if (!is_absolute_reference) {
            HttpUrl context_base(request_url);
            context_base.Query("");
            auto base_path = context_base.Path();
            const auto last_slash = base_path.rfind('/');
            base_path = (last_slash == std::string::npos) ? std::string("/")
                                                          : base_path.substr(0, last_slash + 1);
            context_base.Path(base_path);
            return HttpUrl::MergeWithBaseUrlIfRelative(context_base, ctx).ToString();
        }

        auto merged = HttpUrl::MergeWithBaseUrlIfRelative(request_url, ctx);
        merged.Query("");
        return merged.ToString();
    }

    // Fallback: derive $metadata next to the service root; keep Datasphere-specific rules
    HttpUrl base(request_url);
    auto path = base.Path();
    base.Query("");

    // A ".svc" segment names the service root explicitly (the WCF Data Services
    // convention used by Northwind, Business Central and many on-premise services).
    // It is independent of the OData version, so it is honoured before any
    // version-specific path heuristic. See GitHub #60.
    // ".svc" only names a service root when it ENDS a path segment. Matching it as a
    // bare substring would also fire on "/catalog.svcdata/Orders" or a segment merely
    // containing it, truncating the path at the wrong place.
    const auto find_svc_segment_end = [](const std::string &candidate) -> size_t {
        size_t search_from = 0;
        while (true) {
            const auto found = candidate.find(".svc", search_from);
            if (found == std::string::npos) {
                return std::string::npos;
            }
            const auto segment_end = found + 4;
            if (segment_end == candidate.size() || candidate[segment_end] == '/') {
                return segment_end;
            }
            search_from = found + 1;
        }
    };

    const auto svc_segment_end = find_svc_segment_end(path);
    if (svc_segment_end != std::string::npos) {
        base.Path(path.substr(0, svc_segment_end) + "/$metadata");
    } else if (path.find("/V2/") != std::string::npos) {
        auto v2_pos = path.find("/V2/");
        auto service_pos = path.find("/", v2_pos + 4);
        if (service_pos != std::string::npos) {
            auto service_root = path.substr(0, service_pos);
            base.Path(service_root + "/$metadata");
        } else {
            base.Path(path + "/$metadata");
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

namespace {

// OData escapes a single quote inside a string literal by doubling it: O'Brien is written
// 'O''Brien'. Emitting the raw quote closed the literal early, which either produced a
// malformed key predicate the service rejected or - worse - let a parameter value inject
// further OData syntax into the URL. See GitHub #104.
std::string EscapeSingleQuotes(const std::string &value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char c : value) {
        escaped += c;
        if (c == '\'') {
            escaped += '\'';
        }
    }
    return escaped;
}

// Integers, decimals and ISO dates go into the key predicate unquoted; everything else is
// a string literal. An empty value is a string literal, not a zero-length number: the old
// find_first_not_of test answered npos for "" and emitted a bare "Key=".
bool IsUnquotedParameterLiteral(const std::string &value) {
    if (value.empty()) {
        return false;
    }
    const bool only_digits_and_sign = (value.find_first_not_of("0123456789-") == std::string::npos);
    const bool is_decimal = (value.find_first_not_of("0123456789.-") == std::string::npos &&
                             value.find('.') != std::string::npos);
    const bool is_date = (only_digits_and_sign && value.find('-') != std::string::npos && value.length() == 10);
    const bool is_integer = (only_digits_and_sign && !is_date);
    // "-", "--" and friends satisfy only_digits_and_sign but are not numbers.
    if ((is_integer || is_decimal) && value.find_first_of("0123456789") == std::string::npos) {
        return false;
    }
    return is_decimal || is_integer || is_date;
}

} // namespace

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
        if (IsUnquotedParameterLiteral(value)) {
            params_string += key + "=" + value;
        } else {
            params_string += key + "='" + EscapeSingleQuotes(value) + "'";
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

// ----------------------- ODataDeltaLink -----------------------

namespace {

constexpr const char *V2_DELTA_SIGIL = "!deltatoken=";
constexpr const char *V4_DELTA_SIGIL = "$deltatoken=";

std::string ReadStringMember(duckdb_yyjson::yyjson_val *object, const char *key) {
    if (object == nullptr) {
        return std::string();
    }
    auto *value = duckdb_yyjson::yyjson_obj_get(object, key);
    if (value == nullptr || !duckdb_yyjson::yyjson_is_str(value)) {
        return std::string();
    }
    return std::string(duckdb_yyjson::yyjson_get_str(value));
}

// Reads the token that follows `sigil`, up to the next query separator.
std::string TokenAfterSigil(const std::string &url, const char *sigil) {
    const auto sigil_pos = url.find(sigil);
    if (sigil_pos == std::string::npos) {
        return std::string();
    }
    const auto start = sigil_pos + std::char_traits<char>::length(sigil);
    const auto end = url.find_first_of("&#", start);
    auto token = url.substr(start, end == std::string::npos ? std::string::npos : end - start);

    // SAP Gateway quotes the token in some releases; the state store keeps it unquoted so
    // that BuildDeltaUrl can requote it consistently.
    if (token.size() >= 2 && (token.front() == '\'' || token.front() == '"') && token.back() == token.front()) {
        token = token.substr(1, token.size() - 2);
    }
    return token;
}

} // namespace

bool ODataDeltaLink::IsDeltaLink(const std::string &url) {
    return url.find(V2_DELTA_SIGIL) != std::string::npos ||
           url.find(V4_DELTA_SIGIL) != std::string::npos;
}

std::string ODataDeltaLink::ExtractToken(const std::string &delta_link) {
    auto token = TokenAfterSigil(delta_link, V2_DELTA_SIGIL);
    if (!token.empty()) {
        return token;
    }
    return TokenAfterSigil(delta_link, V4_DELTA_SIGIL);
}

std::string ODataDeltaLink::ExtractDeltaLink(const std::string &json_body) {
    if (json_body.empty()) {
        return std::string();
    }

    auto *doc = duckdb_yyjson::yyjson_read(json_body.c_str(), json_body.size(), 0);
    if (doc == nullptr) {
        return std::string();
    }

    std::string delta_link;
    auto *root = duckdb_yyjson::yyjson_doc_get_root(doc);
    if (root != nullptr && duckdb_yyjson::yyjson_is_obj(root)) {
        auto *d_wrapper = duckdb_yyjson::yyjson_obj_get(root, "d");
        if (d_wrapper != nullptr && !duckdb_yyjson::yyjson_is_obj(d_wrapper)) {
            d_wrapper = nullptr;
        }

        delta_link = ReadStringMember(root, "@odata.deltaLink");
        if (delta_link.empty()) {
            delta_link = ReadStringMember(d_wrapper, "__delta");
        }
        if (delta_link.empty()) {
            delta_link = ReadStringMember(root, "__delta");
        }
        if (delta_link.empty()) {
            // The form the older parser missed: the terminal page carries the token on its
            // paging link rather than on a dedicated delta property. See GitHub #102.
            auto next_link = ReadStringMember(d_wrapper, "__next");
            if (next_link.empty()) {
                next_link = ReadStringMember(root, "__next");
            }
            if (IsDeltaLink(next_link)) {
                delta_link = next_link;
            }
        }
    }

    duckdb_yyjson::yyjson_doc_free(doc);
    return delta_link;
}

std::string ODataDeltaLink::ExtractTokenFromDeltaLinksPayload(const std::string &json_body) {
    if (json_body.empty()) {
        return std::string();
    }

    auto *doc = duckdb_yyjson::yyjson_read(json_body.c_str(), json_body.size(), 0);
    if (doc == nullptr) {
        return std::string();
    }

    std::string token;
    std::string initial_load_token;

    auto *root = duckdb_yyjson::yyjson_doc_get_root(doc);
    auto *d_wrapper = (root != nullptr && duckdb_yyjson::yyjson_is_obj(root))
                          ? duckdb_yyjson::yyjson_obj_get(root, "d")
                          : nullptr;
    auto *results = (d_wrapper != nullptr && duckdb_yyjson::yyjson_is_obj(d_wrapper))
                        ? duckdb_yyjson::yyjson_obj_get(d_wrapper, "results")
                        : nullptr;

    if (results != nullptr && duckdb_yyjson::yyjson_is_arr(results)) {
        size_t idx = 0;
        size_t max = 0;
        duckdb_yyjson::yyjson_val *row = nullptr;
        yyjson_arr_foreach(results, idx, max, row) {
            if (!duckdb_yyjson::yyjson_is_obj(row)) {
                continue;
            }
            const auto row_token = ReadStringMember(row, "DeltaToken");
            if (row_token.empty()) {
                continue;
            }
            if (token.empty()) {
                token = row_token;
            }
            // SAP reports IsInitialLoad as the string "True"/"False" in the v2 JSON shape.
            const auto is_initial = ReadStringMember(row, "IsInitialLoad");
            if (initial_load_token.empty() &&
                (is_initial == "True" || is_initial == "true")) {
                initial_load_token = row_token;
            }
        }
    }

    duckdb_yyjson::yyjson_doc_free(doc);
    return initial_load_token.empty() ? token : initial_load_token;
}

std::string ODataDeltaLink::ExtractDeltaToken(const std::string &json_body) {
    return ExtractToken(ExtractDeltaLink(json_body));
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
#ifdef DUCKDB_HAS_EXTENSION_CALLBACK_MANAGER
    // DuckDB v1.5+: encode_query_component moved to top-level namespace
    return duckdb_httplib_openssl::encode_query_component(value, false);
#else
    return duckdb_httplib_openssl::detail::encode_query_param(value);
#endif
}

std::string ODataUrlCodec::decodeQueryValue(const std::string &value) {
#ifdef DUCKDB_HAS_EXTENSION_CALLBACK_MANAGER
    // DuckDB v1.5+: decode_query_component moved to top-level namespace
    return duckdb_httplib_openssl::decode_query_component(value, false);
#else
    return duckdb_httplib_openssl::detail::decode_url(value, false);
#endif
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
    // Percent-encode all characters except RFC 3986 unreserved characters.
    // This ensures spaces (%20), single quotes (%27), semicolons (%3B), and other
    // special chars are encoded, which is required by strict OData services.
    std::string result;
    result.reserve(filter_expr.size() * 3);
    for (auto c : filter_expr) {
        auto uc = static_cast<unsigned char>(c);
        if (std::isalnum(uc) || uc == '-' || uc == '_' || uc == '.' || uc == '~') {
            result += c;
        } else {
            char hex[4];
            snprintf(hex, sizeof(hex), "%%%02X", uc);
            result += hex;
        }
    }
    return result;
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
        // Copy the navigation/property path until options or a separator. Surrounding
        // whitespace is dropped: "$expand= Category , Orders " is sloppy at best and is
        // rejected outright by strict services such as SAP Gateway, which treats the
        // leading space as part of the property name. See GitHub #115.
        std::string segment;
        while (i < expand_value.size()) {
            char c = expand_value[i];
            if (c == '(' || c == ',') break;
            segment += c;
            i++;
        }
        while (!segment.empty() && std::isspace(static_cast<unsigned char>(segment.front()))) {
            segment.erase(segment.begin());
        }
        while (!segment.empty() && std::isspace(static_cast<unsigned char>(segment.back()))) {
            segment.pop_back();
        }
        out += segment;

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
                if (c == '\'') {
                    // A parenthesis inside a string literal must not change the depth.
                    const auto literal_end = odata_text::SkipQuotedLiteral(expand_value, i);
                    opt.append(expand_value, i, literal_end - i);
                    i = literal_end;
                    continue;
                }
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
            // Split on ';' and ',' at depth 0 only, and never inside a string literal:
            // "$filter=Name eq 'Product;Name'" is one option, not two. See GitHub #122.
            std::vector<std::string> raw_parts = odata_text::SplitTopLevel(opt, ";,");
            std::vector<std::string> parts;
            parts.reserve(raw_parts.size());
            for (auto &raw_part : raw_parts) {
                if (!raw_part.empty()) {
                    parts.emplace_back(std::move(raw_part));
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
                while (i < normalized.size() && normalized[i] != '=' && normalized[i] != ';' && normalized[i] != ')') {
                    if (normalized[i] == '\'') {
                        i = odata_text::SkipQuotedLiteral(normalized, i);
                        continue;
                    }
                    i++;
                }
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
                        if (normalized[i] == '\'') {
                            // ';', ')' and '(' inside a string literal are data, not
                            // structure - stepping over the literal keeps the option
                            // boundaries correct. See GitHub #122.
                            i = odata_text::SkipQuotedLiteral(normalized, i);
                            continue;
                        }
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



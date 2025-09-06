#include "odata_url_helpers.hpp"

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

} // namespace erpl_web



#pragma once

#include "http_client.hpp"

namespace erpl_web {

class ODataUrlResolver {
public:
    std::string resolveMetadataUrl(const HttpUrl &request_url,
                                   const std::string &odata_context_if_any) const;
};

class InputParametersFormatter {
public:
    HttpUrl addParams(const HttpUrl &url,
                      const std::map<std::string, std::string> &params) const;
};

// URL encoding/decoding helpers for OData query construction.
// Always use httplib encode/decode to guarantee consistency with the HTTP client.
class ODataUrlCodec {
public:
    // Percent-encode a complete query value (e.g., filter expression) per RFC3986
    static std::string encodeQueryValue(const std::string &value);

    // Decode a query value; convert_plus_to_space=false to keep '+' literal
    static std::string decodeQueryValue(const std::string &value);

    // Ensure $format=json is present in the `HttpUrl` query
    static void ensureJsonFormat(HttpUrl &url);

    // Smart encoding for OData $filter expressions - only encodes literal values, not operators or spaces
    static std::string encodeFilterExpression(const std::string &filter_expr);

    // Normalize $expand value: ensure nested option keys have a leading '$' and retain structure
    static std::string normalizeExpand(const std::string &expand_value);

    // Normalize expand and percent-encode ONLY nested $filter values inside option sections
    static std::string normalizeAndSanitizeExpand(const std::string &expand_value);
};

} // namespace erpl_web



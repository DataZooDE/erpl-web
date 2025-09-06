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

} // namespace erpl_web



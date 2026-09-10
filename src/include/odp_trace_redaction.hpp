#pragma once

#include <algorithm>
#include <cctype>
#include <map>
#include <string>
#include <unordered_set>

namespace erpl_web {

// Trace redaction for ODP requests.
//
// A DEBUG trace used to write Authorization headers verbatim, which puts SAP
// basic-auth passwords and Entra bearer tokens into the trace file (GitHub
// #100). datazoo-oauth2's http_client.cpp has the same logic, but it is a
// file-local static there and not exported, so this is a deliberate local copy
// with the same policy: keep the scheme visible, drop the credential.
namespace odp_trace {

inline std::string ToLowerAscii(const std::string& value) {
    std::string lowered = value;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return lowered;
}

inline bool IsSensitiveHeader(const std::string& name) {
    static const std::unordered_set<std::string> SENSITIVE_HEADERS = {
        "authorization", "proxy-authorization", "cookie", "set-cookie",
        "x-auth-token", "x-api-key", "x-access-token", "x-csrf-token"
    };
    return SENSITIVE_HEADERS.count(ToLowerAscii(name)) > 0;
}

// Returns a value that is safe to write to a trace log. For the two
// Authorization headers the scheme prefix is kept so the auth type stays
// diagnosable ("Bearer ***"), everything else collapses to "***".
inline std::string RedactHeaderValue(const std::string& name, const std::string& value) {
    if (!IsSensitiveHeader(name)) {
        return value;
    }

    const std::string lowered = ToLowerAscii(name);
    if (lowered == "authorization" || lowered == "proxy-authorization") {
        const auto space = value.find(' ');
        if (space != std::string::npos) {
            return value.substr(0, space + 1) + "***";
        }
    }
    return "***";
}

// Renders a header map for a trace line with every credential redacted.
template <typename HeaderMap>
inline std::string FormatHeaders(const HeaderMap& headers, const std::string& indent = "    ") {
    std::string rendered;
    for (const auto& header : headers) {
        rendered += "\n" + indent + header.first + ": " + RedactHeaderValue(header.first, header.second);
    }
    return rendered;
}

// Caps a server-supplied body before it reaches a log or an audit row: SAP
// error bodies can be large and carry request context worth not persisting.
inline std::string TruncateBody(const std::string& body, size_t max_length = 512) {
    if (body.length() <= max_length) {
        return body;
    }
    return body.substr(0, max_length) + "... [truncated, " + std::to_string(body.length()) + " bytes total]";
}

} // namespace odp_trace
} // namespace erpl_web

#pragma once

#include "http_client.hpp"
#include <memory>

namespace erpl_web {

// Singleton HTTP client pool for SAC factory methods
// Reuses a single shared HttpClient instance across all SAC factory operations
// This reduces connection overhead and improves performance through keep-alive pooling
class SacHttpPool {
public:
    // Get or create the singleton HTTP client instance
    // Thread-safe: HttpClient itself manages thread safety
    static std::shared_ptr<HttpClient> GetHttpClient();

private:
    SacHttpPool() = delete;  // Static utility only
    static std::shared_ptr<HttpClient> http_client_;
};

} // namespace erpl_web

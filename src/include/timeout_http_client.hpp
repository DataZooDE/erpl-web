#pragma once

#include "http_client.hpp"
#include <chrono>
#include <future>
#include <memory>

namespace erpl_web {

class TimeoutHttpClient {
public:
    explicit TimeoutHttpClient(std::chrono::milliseconds default_timeout = std::chrono::milliseconds(30000));
    ~TimeoutHttpClient() = default;
    
    // Non-copyable, non-movable
    TimeoutHttpClient(const TimeoutHttpClient&) = delete;
    TimeoutHttpClient& operator=(const TimeoutHttpClient&) = delete;
    TimeoutHttpClient(TimeoutHttpClient&&) = delete;
    TimeoutHttpClient& operator=(TimeoutHttpClient&&) = delete;
    
    // Send request with timeout
    std::unique_ptr<HttpResponse> SendRequestWithTimeout(
        const HttpRequest& request,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0)
    );
    
    // Send request with default timeout
    std::unique_ptr<HttpResponse> SendRequest(const HttpRequest& request);
    
    // Set default timeout
    void SetDefaultTimeout(std::chrono::milliseconds timeout);
    
    // Get default timeout
    std::chrono::milliseconds GetDefaultTimeout() const { return default_timeout_; }

private:
    std::unique_ptr<HttpClient> http_client_;
    std::chrono::milliseconds default_timeout_;
    
    std::unique_ptr<HttpResponse> ExecuteRequestWithTimeout(
        const HttpRequest& request,
        std::chrono::milliseconds timeout
    );
};

} // namespace erpl_web

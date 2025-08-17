#include "erpl_timeout_http_client.hpp"
#include "erpl_tracing.hpp"
#include <stdexcept>
#include <future>

namespace erpl_web {

TimeoutHttpClient::TimeoutHttpClient(std::chrono::milliseconds default_timeout)
    : http_client_(std::make_unique<HttpClient>()), default_timeout_(default_timeout) {
}

std::unique_ptr<HttpResponse> TimeoutHttpClient::SendRequestWithTimeout(
    const HttpRequest& request,
    std::chrono::milliseconds timeout) {
    
    if (timeout == std::chrono::milliseconds(0)) {
        timeout = default_timeout_;
    }
    
    ERPL_TRACE_INFO("TIMEOUT_HTTP_CLIENT", "Sending request with timeout: " + std::to_string(timeout.count()) + "ms");
    
    return ExecuteRequestWithTimeout(request, timeout);
}

std::unique_ptr<HttpResponse> TimeoutHttpClient::SendRequest(const HttpRequest& request) {
    // Make a copy of the request to pass to the underlying HttpClient
    HttpRequest request_copy = request;
    return SendRequestWithTimeout(request_copy, default_timeout_);
}

void TimeoutHttpClient::SetDefaultTimeout(std::chrono::milliseconds timeout) {
    default_timeout_ = timeout;
}

std::unique_ptr<HttpResponse> TimeoutHttpClient::ExecuteRequestWithTimeout(
    const HttpRequest& request,
    std::chrono::milliseconds timeout) {
    
    ERPL_TRACE_DEBUG("TIMEOUT_HTTP_CLIENT", "Executing request with timeout...");
    
    // Create future for the HTTP request
    auto future = std::async(std::launch::async, [this, request_copy = request]() mutable -> std::unique_ptr<HttpResponse> {
        try {
            return http_client_->SendRequest(request_copy);
        } catch (const std::exception& e) {
            ERPL_TRACE_ERROR("TIMEOUT_HTTP_CLIENT", std::string("HTTP request failed: ") + e.what());
            throw;
        }
    });
    
    // Wait for the request with timeout
    if (future.wait_for(timeout) == std::future_status::timeout) {
        ERPL_TRACE_WARN("TIMEOUT_HTTP_CLIENT", "Request timed out after " + std::to_string(timeout.count()) + "ms");
        throw std::runtime_error("HTTP request timed out after " + std::to_string(timeout.count()) + "ms");
    }
    
    try {
        auto response = future.get();
        ERPL_TRACE_INFO("TIMEOUT_HTTP_CLIENT", "Request completed successfully");
        return response;
    } catch (const std::exception& e) {
        ERPL_TRACE_ERROR("TIMEOUT_HTTP_CLIENT", std::string("Error getting response: ") + e.what());
        throw;
    }
}

} // namespace erpl_web

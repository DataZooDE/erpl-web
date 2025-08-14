#include "erpl_timeout_http_client.hpp"
#include <iostream>
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
    
    std::cout << "TimeoutHttpClient: Sending request with timeout: " << timeout.count() << "ms" << std::endl;
    
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
    
    std::cout << "TimeoutHttpClient: Executing request with timeout..." << std::endl;
    
    // Create future for the HTTP request
    auto future = std::async(std::launch::async, [this, request_copy = request]() mutable -> std::unique_ptr<HttpResponse> {
        try {
            return http_client_->SendRequest(request_copy);
        } catch (const std::exception& e) {
            std::cout << "TimeoutHttpClient: HTTP request failed: " << e.what() << std::endl;
            throw;
        }
    });
    
    // Wait for the request with timeout
    if (future.wait_for(timeout) == std::future_status::timeout) {
        std::cout << "TimeoutHttpClient: Request timed out after " << timeout.count() << "ms" << std::endl;
        throw std::runtime_error("HTTP request timed out after " + std::to_string(timeout.count()) + "ms");
    }
    
    try {
        auto response = future.get();
        std::cout << "TimeoutHttpClient: Request completed successfully" << std::endl;
        return response;
    } catch (const std::exception& e) {
        std::cout << "TimeoutHttpClient: Error getting response: " << e.what() << std::endl;
        throw;
    }
}

} // namespace erpl_web

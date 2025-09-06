#include "oauth2_server.hpp"
#include "oauth2_callback_handler.hpp"
#include "tracing.hpp"
#include <stdexcept>
#include <chrono>
#include <thread>
#include <atomic>
#include <string>

// Windows headers define min/max macros that conflict with C++ std:: functions
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

// httplib includes
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace erpl_web {

OAuth2Server::OAuth2Server(int port) : port_(port) {
    callback_handler_ = std::make_unique<OAuth2CallbackHandler>();
    ERPL_TRACE_INFO("OAUTH2_SERVER", "Created server for port " + std::to_string(port));
}

OAuth2Server::~OAuth2Server() {
    ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Destructor called, stopping server...");
    Stop();
}

std::string OAuth2Server::StartAndWaitForCode(const std::string& expected_state, int port) {
    ERPL_TRACE_INFO("OAUTH2_SERVER", "Starting server and waiting for code...");
    
    // Use provided port or default port
    int server_port = (port > 0) ? port : port_;
    
    // Simple approach: run server in main thread with wait loop
    return WaitForCallback(expected_state, server_port);
}

void OAuth2Server::Stop() {
    ERPL_TRACE_INFO("OAUTH2_SERVER", "Stopping server...");
    running_.store(false);
    
    // Stop the httplib server if it's running
    if (server_instance_) {
        ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Stopping httplib server...");
        server_instance_->stop();
    }
    
    // Wait for server thread to finish
    if (server_thread_.joinable()) {
        ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Waiting for server thread to finish...");
        server_thread_.join();
        ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Server thread finished");
    }
    
    // Clean up
    server_instance_.reset();
    ERPL_TRACE_INFO("OAUTH2_SERVER", "Server stopped successfully");
}



std::string OAuth2Server::WaitForCallback(const std::string& expected_state, int port) {
    ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Setting up callback handler...");
    
    // Reset handler for new flow
    callback_handler_->Reset();
    callback_handler_->SetExpectedState(expected_state);
    
    // Create httplib server instance
    server_instance_ = std::make_unique<duckdb_httplib_openssl::Server>();
    
    // Set up OAuth callback route
    server_instance_->Get("/", [this](const duckdb_httplib_openssl::Request& req, duckdb_httplib_openssl::Response& res) {
        ERPL_TRACE_DEBUG("OAUTH2_SERVER", std::string("Received HTTP request: ") + req.path);
        
        // Check for authorization code
        if (req.has_param("code")) {
            std::string code = req.get_param_value("code");
            std::string state = req.get_param_value("state");
            
            ERPL_TRACE_INFO("OAUTH2_SERVER", std::string("Received OAuth callback with code=") + code.substr(0, 10) + "... state=" + state);
            
            // Handle the callback
            callback_handler_->HandleCallback(code, state);
            
            // Serve success page
            res.set_content(
                "<!DOCTYPE html>"
                "<html>"
                "<head>"
                "<title>OAuth2 Authorization Complete</title>"
                "<meta charset='utf-8'>"
                "<style>"
                "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); margin: 0; padding: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }"
                ".container { background: white; border-radius: 20px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); padding: 40px; text-align: center; max-width: 500px; margin: 20px; }"
                ".success-icon { font-size: 80px; margin-bottom: 20px; }"
                "h1 { color: #2d3748; margin-bottom: 20px; font-size: 28px; }"
                ".message { color: #4a5568; font-size: 16px; line-height: 1.6; margin-bottom: 30px; }"
                ".countdown { background: #f7fafc; border-radius: 10px; padding: 20px; margin: 20px 0; }"
                ".timer { font-size: 24px; font-weight: bold; color: #667eea; }"
                ".close-btn { background: #667eea; color: white; border: none; padding: 12px 24px; border-radius: 8px; font-size: 16px; cursor: pointer; transition: background 0.3s; }"
                ".close-btn:hover { background: #5a67d8; }"
                "</style>"
                "</head>"
                "<body>"
                "<div class='container'>"
                "<div class='success-icon'>🎉</div>"
                "<h1>Authorization Successful!</h1>"
                "<div class='message'>"
                "<p>Your OAuth2 authorization has been completed successfully.</p>"
                "<p>The application will now receive your access token.</p>"
                "</div>"
                "<div class='countdown'>"
                "<p>This window will close automatically in:</p>"
                "<div class='timer' id='timer'>3</div>"
                "</div>"
                "<button class='close-btn' onclick='window.close()'>Close Now</button>"
                "</div>"
                "<script>"
                "let timeLeft = 3;"
                "const timerElement = document.getElementById('timer');"
                "const countdown = setInterval(function() {"
                "timeLeft--;"
                "timerElement.textContent = timeLeft;"
                "if (timeLeft <= 0) {"
                "clearInterval(countdown);"
                "window.close();"
                "}"
                "}, 1000);"
                "</script>"
                "</body>"
                "</html>",
                "text/html"
            );
            
        } else if (req.has_param("error")) {
            std::string error = req.get_param_value("error");
            std::string error_description = req.get_param_value("error_description");
            std::string state = req.get_param_value("state");
            
            ERPL_TRACE_WARN("OAUTH2_SERVER", std::string("Received OAuth error: ") + error + " - " + error_description);
            
            // Handle the error
            callback_handler_->HandleError(error, error_description, state);
            
            // Serve error page
            res.set_content(
                "<!DOCTYPE html>"
                "<html>"
                "<head>"
                "<title>OAuth2 Authorization Failed</title>"
                "<meta charset='utf-8'>"
                "<style>"
                "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); margin: 0; padding: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }"
                ".container { background: white; border-radius: 20px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); padding: 40px; text-align: center; max-width: 500px; margin: 20px; }"
                ".error-icon { font-size: 80px; margin-bottom: 20px; }"
                "h1 { color: #c53030; margin-bottom: 20px; font-size: 28px; }"
                ".error-details { background: #fed7d7; border-radius: 10px; padding: 20px; margin: 20px 0; text-align: left; }"
                ".error-label { font-weight: bold; color: #c53030; }"
                ".close-btn { background: #e53e3e; color: white; border: none; padding: 12px 24px; border-radius: 8px; font-size: 16px; cursor: pointer; transition: background 0.3s; }"
                ".close-btn:hover { background: #c53030; }"
                "</style>"
                "</head>"
                "<body>"
                "<div class='container'>"
                "<div class='error-icon'>❌</div>"
                "<h1>Authorization Failed</h1>"
                "<div class='error-details'>"
                "<p><span class='error-label'>Error:</span> " + error + "</p>"
                "<p><span class='error-label'>Description:</span> " + error_description + "</p>"
                "</div>"
                "<p>Please try again or contact your system administrator.</p>"
                "<button class='close-btn' onclick='window.close()'>Close Window</button>"
                "</div>"
                "</body>"
                "</html>",
                "text/html"
            );
            
        } else {
            // Default page
            res.set_content(
                "<!DOCTYPE html>"
                "<html>"
                "<head>"
                "<title>OAuth2 Callback Server</title>"
                "<meta charset='utf-8'>"
                "<style>"
                "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); margin: 0; padding: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }"
                ".container { background: white; border-radius: 20px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); padding: 40px; text-align: center; max-width: 500px; margin: 20px; }"
                ".waiting-icon { font-size: 80px; margin-bottom: 20px; animation: pulse 2s infinite; }"
                "@keyframes pulse { 0% { transform: scale(1); } 50% { transform: scale(1.1); } 100% { transform: scale(1); } }"
                "h1 { color: #2d3748; margin-bottom: 20px; font-size: 28px; }"
                ".message { color: #4a5568; font-size: 16px; line-height: 1.6; margin-bottom: 20px; }"
                ".status { background: #ebf8ff; border-radius: 10px; padding: 20px; margin: 20px 0; border-left: 4px solid #4facfe; }"
                "</style>"
                "</head>"
                "<body>"
                "<div class='container'>"
                "<div class='waiting-icon'>⏳</div>"
                "<h1>OAuth2 Callback Server</h1>"
                "<div class='message'>"
                "<p>Waiting for authorization callback...</p>"
                "<p>Please complete the authentication in your browser.</p>"
                "</div>"
                "<div class='status'>"
                "<p><strong>Status:</strong> Ready to receive callback</p>"
                "<p><strong>Port:</strong> 65000</p>"
                "</div>"
                "</div>"
                "</body>"
                "</html>",
                "text/html"
            );
        }
    });
    
    // Start server in main thread (httplib handles requests in its own threads)
    ERPL_TRACE_INFO("OAUTH2_SERVER", "Starting server on port " + std::to_string(port));
    
    // Start server in background thread (httplib handles requests in its own thread pool)
    ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Starting server in background thread...");
    
    server_thread_ = std::thread([this, port]() {
        if (!server_instance_->listen("localhost", port)) {
            ERPL_TRACE_ERROR("OAUTH2_SERVER", "Failed to start server on port " + std::to_string(port));
            return;
        }
    });
    
    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Simple wait loop in main thread - check every 250ms with 60 second timeout
    ERPL_TRACE_INFO("OAUTH2_SERVER", "Waiting for OAuth callback (timeout: 60 seconds)...");
    running_.store(true);
    
    auto start_time = std::chrono::steady_clock::now();
    const auto timeout_duration = std::chrono::seconds(60);
    
    while (running_.load() && !callback_handler_->IsCallbackReceived() && !callback_handler_->HasError()) {
        // Check if timeout has been reached
        auto current_time = std::chrono::steady_clock::now();
        if (current_time - start_time > timeout_duration) {
            ERPL_TRACE_WARN("OAUTH2_SERVER", "Timeout reached (60 seconds)");
            break;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    
    // Server will be stopped by the Stop() method when called
    ERPL_TRACE_DEBUG("OAUTH2_SERVER", "Callback received, server will be stopped by Stop() method");
    
    // Return the authorization code or throw if there was an error
    if (callback_handler_->HasError()) {
        throw std::runtime_error("OAuth2 error: " + callback_handler_->GetErrorMessage());
    }
    
    if (callback_handler_->IsCallbackReceived()) {
        // The code was already received and stored in the callback handler
        return callback_handler_->GetReceivedCode();
    }
    
    throw std::runtime_error("Timeout waiting for OAuth2 callback");
}

} // namespace erpl_web

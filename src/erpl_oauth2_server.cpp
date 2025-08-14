#include "erpl_oauth2_server.hpp"
#include "erpl_oauth2_callback_handler.hpp"
#include <iostream>
#include <stdexcept>
#include <chrono>
#include <thread>
#include <atomic>
#include <string>

// httplib includes
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace erpl_web {

OAuth2Server::OAuth2Server(int port) : port_(port) {
    callback_handler_ = std::make_unique<OAuth2CallbackHandler>();
    std::cout << "OAuth2Server: Created server for port " << port << std::endl;
}

OAuth2Server::~OAuth2Server() {
    std::cout << "OAuth2Server: Destructor called, stopping server..." << std::endl;
    Stop();
}

std::string OAuth2Server::StartAndWaitForCode(const std::string& expected_state, int port) {
    std::cout << "OAuth2Server: Starting server and waiting for code..." << std::endl;
    
    // Use provided port or default port
    int server_port = (port > 0) ? port : port_;
    
    // Simple approach: run server in main thread with wait loop
    return WaitForCallback(expected_state, server_port);
}

void OAuth2Server::Stop() {
    std::cout << "OAuth2Server: Stopping server..." << std::endl;
    running_.store(false);
    
    // Stop the httplib server if it's running
    if (server_instance_) {
        std::cout << "OAuth2Server: Stopping httplib server..." << std::endl;
        server_instance_->stop();
    }
    
    // Wait for server thread to finish
    if (server_thread_.joinable()) {
        std::cout << "OAuth2Server: Waiting for server thread to finish..." << std::endl;
        server_thread_.join();
        std::cout << "OAuth2Server: Server thread finished" << std::endl;
    }
    
    // Clean up
    server_instance_.reset();
    std::cout << "OAuth2Server: Server stopped successfully" << std::endl;
}



std::string OAuth2Server::WaitForCallback(const std::string& expected_state, int port) {
    std::cout << "OAuth2Server: Setting up callback handler..." << std::endl;
    
    // Reset handler for new flow
    callback_handler_->Reset();
    callback_handler_->SetExpectedState(expected_state);
    
    // Create httplib server instance
    server_instance_ = std::make_unique<duckdb_httplib_openssl::Server>();
    
    // Set up OAuth callback route
    server_instance_->Get("/", [this](const duckdb_httplib_openssl::Request& req, duckdb_httplib_openssl::Response& res) {
        std::cout << "OAuth2Server: Received HTTP request: " << req.path << std::endl;
        
        // Check for authorization code
        if (req.has_param("code")) {
            std::string code = req.get_param_value("code");
            std::string state = req.get_param_value("state");
            
            std::cout << "OAuth2Server: Received OAuth callback with code=" << code.substr(0, 10) << "... state=" << state << std::endl;
            
            // Handle the callback
            callback_handler_->HandleCallback(code, state);
            
            // Serve success page
            res.set_content(
                "<html><body><h1>Authorization Successful!</h1>"
                "<p>You can close this window now.</p></body></html>",
                "text/html"
            );
            
        } else if (req.has_param("error")) {
            std::string error = req.get_param_value("error");
            std::string error_description = req.get_param_value("error_description");
            std::string state = req.get_param_value("state");
            
            std::cout << "OAuth2Server: Received OAuth error: " << error << " - " << error_description << std::endl;
            
            // Handle the error
            callback_handler_->HandleError(error, error_description, state);
            
            // Serve error page
            res.set_content(
                "<html><body><h1>Authorization Failed</h1>"
                "<p>Error: " + error + "</p>"
                "<p>Description: " + error_description + "</p>"
                "<p>You can close this window now.</p></body></html>",
                "text/html"
            );
            
        } else {
            // Default page
            res.set_content(
                "<html><body><h1>OAuth Callback Server</h1>"
                "<p>Waiting for authorization...</p></body></html>",
                "text/html"
            );
        }
    });
    
    // Start server in main thread (httplib handles requests in its own threads)
    std::cout << "OAuth2Server: Starting server on port " << port << std::endl;
    
    // Start server in background thread (httplib handles requests in its own thread pool)
    std::cout << "OAuth2Server: Starting server in background thread..." << std::endl;
    
    server_thread_ = std::thread([this, port]() {
        if (!server_instance_->listen("localhost", port)) {
            std::cout << "OAuth2Server: Failed to start server on port " << port << std::endl;
            return;
        }
    });
    
    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Simple wait loop in main thread - check every 250ms with 60 second timeout
    std::cout << "OAuth2Server: Waiting for OAuth callback (timeout: 60 seconds)..." << std::endl;
    running_.store(true);
    
    auto start_time = std::chrono::steady_clock::now();
    const auto timeout_duration = std::chrono::seconds(60);
    
    while (running_.load() && !callback_handler_->IsCallbackReceived() && !callback_handler_->HasError()) {
        // Check if timeout has been reached
        auto current_time = std::chrono::steady_clock::now();
        if (current_time - start_time > timeout_duration) {
            std::cout << "OAuth2Server: Timeout reached (60 seconds)" << std::endl;
            break;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    
    // Server will be stopped by the Stop() method when called
    std::cout << "OAuth2Server: Callback received, server will be stopped by Stop() method" << std::endl;
    
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

#pragma once
#include <string>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>
#include <map>

namespace erpl_web {

// Local HTTP server for OAuth2 callback handling (matching SAP CLI exactly)
// Simplified version for initial OAuth2 implementation
class DatasphereLocalServer {
public:
    explicit DatasphereLocalServer(int port);
    ~DatasphereLocalServer();
    
    // Start the server
    void Start();
    
    // Stop the server
    void Stop();
    
    // Wait for authorization code from callback (matching SAP CLI exactly)
    std::string WaitForAuthorizationCode(const std::string& expected_state, 
                                       std::chrono::seconds timeout = std::chrono::seconds(30));
    
    // Check if server is running
    bool IsRunning() const;
    
    // Get the callback URL
    std::string GetCallbackUrl() const;
    
    // Set callback handler (matching SAP CLI exactly)
    void SetCallbackHandler(std::function<void(const std::string&, const std::string&)> handler);

private:
    int port;
    std::string received_code;
    std::string received_state;
    mutable std::mutex server_mutex;
    std::condition_variable server_cv;
    bool code_received = false;
    bool is_running = false;
    std::function<void(const std::string&, const std::string&)> callback_handler;
    
    // Setup server routes (matching SAP CLI exactly)
    void SetupRoutes();
    
    // Handle OAuth2 callback (matching SAP CLI exactly)
    void HandleCallback(const std::string& code, const std::string& state);
    
    // Handle error callback (matching SAP CLI exactly)
    void HandleErrorCallback(const std::string& error, const std::string& error_description);
    
    // Generate HTML response for callback (matching SAP CLI exactly)
    std::string GenerateCallbackHtml(bool success, const std::string& message);
    
    // Generate success HTML (matching SAP CLI exactly)
    std::string GenerateSuccessHtml();
    
    // Generate error HTML (matching SAP CLI exactly)
    std::string GenerateErrorHtml();
    
    // Validate callback parameters
    bool ValidateCallback(const std::string& code, const std::string& state, const std::string& expected_state);
    
    // Parse URL parameters (matching SAP CLI exactly)
    std::map<std::string, std::string> ParseUrlParams(const std::string& url);
    
    // Extract authorization code from URL (matching SAP CLI exactly)
    std::string ExtractAuthorizationCode(const std::string& url);
    
    // Extract state from URL (matching SAP CLI exactly)
    std::string ExtractState(const std::string& url);
    
    // Simulate server functionality for now
    void SimulateServer();
};

} // namespace erpl_web

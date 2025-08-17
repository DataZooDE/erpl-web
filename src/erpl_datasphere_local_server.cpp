#include "erpl_datasphere_local_server.hpp"
#include "erpl_tracing.hpp"
#include <sstream>
#include <algorithm>
#include <thread>
#include <chrono>

namespace erpl_web {

DatasphereLocalServer::DatasphereLocalServer(int port) : port(port) {
    ERPL_TRACE_INFO("LOCAL_SERVER", "Initialized on port " + std::to_string(port));
}

DatasphereLocalServer::~DatasphereLocalServer() {
    Stop();
}

void DatasphereLocalServer::Start() {
    std::lock_guard<std::mutex> lock(server_mutex);
    is_running = true;
    ERPL_TRACE_INFO("LOCAL_SERVER", "Started on port " + std::to_string(port));
    
    // Simulate server functionality for now
    SimulateServer();
}

void DatasphereLocalServer::Stop() {
    std::lock_guard<std::mutex> lock(server_mutex);
    is_running = false;
    ERPL_TRACE_INFO("LOCAL_SERVER", "Stopped");
}

std::string DatasphereLocalServer::WaitForAuthorizationCode(const std::string& expected_state, 
                                                          std::chrono::seconds timeout) {
    ERPL_TRACE_INFO("LOCAL_SERVER", "Waiting for authorization code with state: " + expected_state);
    
    // For now, simulate receiving a code after a short delay
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Simulate receiving an authorization code
    received_code = "simulated_auth_code_12345";
    received_state = expected_state;
    code_received = true;
    
    ERPL_TRACE_INFO("LOCAL_SERVER", "Received simulated authorization code");
    return received_code;
}

bool DatasphereLocalServer::IsRunning() const {
    std::lock_guard<std::mutex> lock(server_mutex);
    return is_running;
}

std::string DatasphereLocalServer::GetCallbackUrl() const {
    return "http://localhost:" + std::to_string(port) + "/callback";
}

void DatasphereLocalServer::SetCallbackHandler(std::function<void(const std::string&, const std::string&)> handler) {
    callback_handler = handler;
}

void DatasphereLocalServer::SetupRoutes() {
    // This would set up HTTP routes in a real implementation
    ERPL_TRACE_DEBUG("LOCAL_SERVER", "Setting up server routes");
}

void DatasphereLocalServer::HandleCallback(const std::string& code, const std::string& state) {
    ERPL_TRACE_INFO("LOCAL_SERVER", std::string("Handling callback with code: ") + code + ", state: " + state);
    
    if (callback_handler) {
        callback_handler(code, state);
    }
    
    received_code = code;
    received_state = state;
    code_received = true;
}

void DatasphereLocalServer::HandleErrorCallback(const std::string& error, const std::string& error_description) {
    ERPL_TRACE_WARN("LOCAL_SERVER", std::string("Handling error callback: ") + error + " - " + error_description);
}

std::string DatasphereLocalServer::GenerateCallbackHtml(bool success, const std::string& message) {
    if (success) {
        return GenerateSuccessHtml();
    } else {
        return GenerateErrorHtml();
    }
}

std::string DatasphereLocalServer::GenerateSuccessHtml() {
    return R"(
<!DOCTYPE html>
<html>
<head>
    <title>OAuth2 Success</title>
    <meta charset="utf-8">
    <style>
        body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
        .success { color: #28a745; }
    </style>
</head>
<body>
    <h1 class="success">✅ Authentication Successful!</h1>
    <p>You can now close this window and return to the application.</p>
</body>
</html>
    )";
}

std::string DatasphereLocalServer::GenerateErrorHtml() {
    return R"(
<!DOCTYPE html>
<html>
<head>
    <title>OAuth2 Error</title>
    <meta charset="utf-8">
    <style>
        body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
        .error { color: #dc3545; }
    </style>
</head>
<body>
    <h1 class="error">❌ Authentication Failed</h1>
    <p>Please try again or contact support.</p>
</body>
</html>
    )";
}

bool DatasphereLocalServer::ValidateCallback(const std::string& code, const std::string& state, const std::string& expected_state) {
    if (code.empty()) {
        return false;
    }
    
    if (state != expected_state) {
        ERPL_TRACE_WARN("LOCAL_SERVER", std::string("State mismatch: expected ") + expected_state + ", got " + state);
        return false;
    }
    
    return true;
}

std::map<std::string, std::string> DatasphereLocalServer::ParseUrlParams(const std::string& url) {
    std::map<std::string, std::string> params;
    
    size_t query_start = url.find('?');
    if (query_start == std::string::npos) {
        return params;
    }
    
    std::string query_string = url.substr(query_start + 1);
    std::istringstream iss(query_string);
    std::string param;
    
    while (std::getline(iss, param, '&')) {
        size_t equal_pos = param.find('=');
        if (equal_pos != std::string::npos) {
            std::string key = param.substr(0, equal_pos);
            std::string value = param.substr(equal_pos + 1);
            params[key] = value;
        }
    }
    
    return params;
}

std::string DatasphereLocalServer::ExtractAuthorizationCode(const std::string& url) {
    auto params = ParseUrlParams(url);
    auto it = params.find("code");
    if (it != params.end()) {
        return it->second;
    }
    return "";
}

std::string DatasphereLocalServer::ExtractState(const std::string& url) {
    auto params = ParseUrlParams(url);
    auto it = params.find("state");
    if (it != params.end()) {
        return it->second;
    }
    return "";
}

void DatasphereLocalServer::SimulateServer() {
    ERPL_TRACE_INFO("LOCAL_SERVER", "Simulating local HTTP server functionality");
    ERPL_TRACE_INFO("LOCAL_SERVER", std::string("Callback URL: ") + GetCallbackUrl());
    ERPL_TRACE_INFO("LOCAL_SERVER", "Server is ready to receive OAuth2 callbacks");
}

} // namespace erpl_web

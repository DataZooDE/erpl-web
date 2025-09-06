#pragma once

#include <memory>
#include <thread>
#include <future>
#include <string>
#include <atomic>
#include <functional>
#include <mutex>

// Forward declaration
namespace duckdb_httplib_openssl {
    class Server;
}

namespace erpl_web {

// Forward declaration
class OAuth2CallbackHandler;

class OAuth2Server {
public:
    explicit OAuth2Server(int port);
    ~OAuth2Server();
    
    // Non-copyable, non-movable
    OAuth2Server(const OAuth2Server&) = delete;
    OAuth2Server& operator=(const OAuth2Server&) = delete;
    OAuth2Server(OAuth2Server&&) = delete;
    OAuth2Server& operator=(OAuth2Server&&) = delete;
    
    // Start server and wait for authorization code
    std::string StartAndWaitForCode(const std::string& expected_state, int port = 0);
    
    // Stop server gracefully
    void Stop();
    
    // Check if server is running
    bool IsRunning() const { return running_.load(); }
    
    // Get server port
    int GetPort() const { return port_; }

private:
    int port_;
    std::atomic<bool> running_{false};
    std::unique_ptr<OAuth2CallbackHandler> callback_handler_;
    std::unique_ptr<duckdb_httplib_openssl::Server> server_instance_;
    std::thread server_thread_;
    
    // Simple wait loop in main thread
    std::string WaitForCallback(const std::string& expected_state, int port);
};

} // namespace erpl_web

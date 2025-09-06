#pragma once

#include <string>
#include <future>
#include <mutex>
#include <atomic>

namespace erpl_web {

class OAuth2CallbackHandler {
public:
    OAuth2CallbackHandler();
    ~OAuth2CallbackHandler() = default;
    
    // Non-copyable, non-movable
    OAuth2CallbackHandler(const OAuth2CallbackHandler&) = delete;
    OAuth2CallbackHandler& operator=(const OAuth2CallbackHandler&) = delete;
    OAuth2CallbackHandler(OAuth2CallbackHandler&&) = delete;
    OAuth2CallbackHandler& operator=(OAuth2CallbackHandler&&) = delete;
    
    // Reset handler for new OAuth2 flow
    void Reset();
    
    // Set expected state for validation
    void SetExpectedState(const std::string& expected_state);
    
    // Handle OAuth2 callback
    void HandleCallback(const std::string& code, const std::string& state);
    
    // Handle OAuth2 error
    void HandleError(const std::string& error, const std::string& error_description, const std::string& state);
    
    // Wait for authorization code with timeout (mutex-based)
    std::string WaitForCode(std::chrono::seconds timeout = std::chrono::seconds(60));
    
    // Check if callback was received
    bool IsCallbackReceived() const { return callback_received_.load(); }
    
    // Check if error occurred
    bool HasError() const { return has_error_.load(); }
    
    // Get error message
    std::string GetErrorMessage() const;
    
    // Get received authorization code
    std::string GetReceivedCode() const;

private:
    std::string expected_state_;
    std::string received_code_;
    std::string error_message_;
    std::atomic<bool> callback_received_{false};
    std::atomic<bool> has_error_{false};
    mutable std::mutex mutex_;
    std::condition_variable code_cv_;
    
    bool ValidateState(const std::string& received_state) const;
};

} // namespace erpl_web

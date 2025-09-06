#pragma once

#include "oauth2_types.hpp"
#include "oauth2_server.hpp"
#include "timeout_http_client.hpp"
#include <memory>
#include <string>

namespace erpl_web {

class OAuth2FlowV2 {
public:
    OAuth2FlowV2();
    ~OAuth2FlowV2();
    
    OAuth2Tokens ExecuteFlow(const OAuth2Config& config);
    
private:
    std::string ExecuteAuthorizationCodeFlow(const OAuth2Config& config);
    OAuth2Tokens ExchangeCodeForTokens(const OAuth2Config& config, const std::string& authorization_code, const std::string& code_verifier);
    std::string BuildTokenExchangePostData(const OAuth2Config& config, const std::string& authorization_code, const std::string& code_verifier);
    OAuth2Tokens ParseTokenResponse(const std::string& response_content);
    std::string GenerateCodeVerifier();
    std::string GenerateCodeChallenge(const std::string& code_verifier);
    std::string GenerateState();
    std::string BuildAuthorizationUrl(const OAuth2Config& config, const std::string& code_challenge, const std::string& state);
    void OpenBrowser(const std::string& url);
    void DisplayOAuth2Instructions(const std::string& auth_url);
    
    std::unique_ptr<OAuth2Server> server_;
    std::unique_ptr<TimeoutHttpClient> http_client_;
    std::string stored_code_verifier_;
};

} // namespace erpl_web

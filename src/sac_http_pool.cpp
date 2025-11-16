#include "sac_http_pool.hpp"

namespace erpl_web {

// Initialize static member
std::shared_ptr<HttpClient> SacHttpPool::http_client_ = nullptr;

std::shared_ptr<HttpClient> SacHttpPool::GetHttpClient() {
    if (!http_client_) {
        http_client_ = std::make_shared<HttpClient>();
    }
    return http_client_;
}

} // namespace erpl_web

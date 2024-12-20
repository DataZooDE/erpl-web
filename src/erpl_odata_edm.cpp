#include "erpl_odata_edm.hpp"
#include "erpl_http_client.hpp"

namespace erpl_web {

EdmCache& EdmCache::GetInstance() {
    static EdmCache instance;
    return instance;
}

duckdb::optional_ptr<Edmx> EdmCache::Get(const std::string& metadata_url) {
    std::lock_guard<std::mutex> lock(cache_lock);
    auto url_without_fragment = UrlWithoutFragment(metadata_url);
    auto it = cache.find(url_without_fragment);
    if (it != cache.end()) {
        return duckdb::optional_ptr<Edmx>(&(it->second));
    }
    return duckdb::optional_ptr<Edmx>();
}

void EdmCache::Set(const std::string& metadata_url, Edmx edmx) {
    std::lock_guard<std::mutex> lock(cache_lock);
    auto url_without_fragment = UrlWithoutFragment(metadata_url);
    cache[url_without_fragment] = std::move(edmx);
}

std::string EdmCache::UrlWithoutFragment(const std::string& url_str) const {
    std::stringstream ss;
    auto url = HttpUrl(url_str);
    ss << url.ToSchemeHostAndPort() << url.ToPathQuery();
    return ss.str();
}

} // namespace erpl_web
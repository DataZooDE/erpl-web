#pragma once

#include <string>
#include <optional>
#include <regex>
#include <map>

namespace erpl_web {

/**
 * URL Parser and Utilities
 *
 * Provides simplified URL parsing with regex-based component extraction.
 * Reduces string manipulation complexity across SAC and Datasphere modules.
 *
 * Patterns supported:
 * - Cloud URLs: https://tenant.region.sapanalytics.cloud
 * - Data center URLs: https://tenant.dc.hcs.cloud.sap
 * - OData endpoints: /api/v1/odata/..., /dwaas-core/api/v1/...
 *
 * Usage:
 *   auto parsed = UrlParser::Parse("https://mytenant.us10.sapanalytics.cloud");
 *   if (parsed) {
 *       auto tenant = parsed->GetComponent("tenant");  // "mytenant"
 *   }
 */
class UrlParser {
public:
    /**
     * Parse a URL and extract components
     *
     * @param url The URL to parse
     * @return Optional containing parsed URL, or empty if parse fails
     *
     * Extracted components depend on URL pattern:
     * - "scheme": https or http
     * - "tenant": subdomain before first dot (for cloud URLs)
     * - "region" or "datacenter": between first and second dot (depending on pattern)
     * - "domain": main domain (sapanalytics.cloud, hcs.cloud.sap, etc.)
     * - "path": URL path component
     */
    static std::optional<UrlParser> Parse(const std::string& url);

    /**
     * Get a component from the parsed URL
     *
     * @param component The component name (tenant, region, path, etc.)
     * @return The component value, or empty string if not found
     *
     * Example:
     *   auto tenant = parsed.GetComponent("tenant");
     */
    std::string GetComponent(const std::string& component) const;

    /**
     * Reconstruct the URL with optional component replacement
     *
     * @param component_overrides Map of components to replace
     * @return Reconstructed URL with replacements applied
     *
     * Example:
     *   auto new_url = parsed.Reconstruct({{"region", "eu10"}});
     */
    std::string Reconstruct(
        const std::map<std::string, std::string>& component_overrides = {}) const;

    /**
     * Extract tenant and region from SAC cloud URL
     * Convenience method for SAC URLs
     *
     * @param url The SAC URL to parse (https://tenant.region.sapanalytics.cloud/...)
     * @return Pair of (tenant, region), or empty optionals if parse fails
     *
     * Example:
     *   auto [tenant, region] = UrlParser::ExtractTenantRegion(url).value();
     */
    static std::optional<std::pair<std::string, std::string>> ExtractTenantRegion(
        const std::string& url);

    /**
     * Extract tenant and data center from Datasphere URL
     * Convenience method for Datasphere URLs
     *
     * @param url The Datasphere URL to parse (https://tenant.dc.hcs.cloud.sap/...)
     * @return Pair of (tenant, datacenter), or empty optionals if parse fails
     *
     * Example:
     *   auto [tenant, dc] = UrlParser::ExtractTenantDatacenter(url).value();
     */
    static std::optional<std::pair<std::string, std::string>> ExtractTenantDatacenter(
        const std::string& url);

    /**
     * Check if URL matches a specific pattern
     *
     * Pattern types:
     * - "sac": SAP Analytics Cloud (tenant.region.sapanalytics.cloud)
     * - "datasphere": SAP Datasphere (tenant.dc.hcs.cloud.sap)
     * - "oauth": OAuth endpoint (/oauth/...)
     * - "odata": OData endpoint (/api/v1/odata/...)
     *
     * @param url The URL to check
     * @param pattern_type The pattern type to match against
     * @return true if URL matches pattern, false otherwise
     */
    static bool MatchesPattern(const std::string& url, const std::string& pattern_type);

    /**
     * Normalize a URL (lowercase, remove trailing slash, etc.)
     *
     * @param url The URL to normalize
     * @return Normalized URL
     */
    static std::string Normalize(const std::string& url);

private:
    std::map<std::string, std::string> components_;

    // Regex patterns for different URL formats
    static const std::regex SAC_URL_PATTERN;
    static const std::regex DATASPHERE_URL_PATTERN;
    static const std::regex OAUTH_PATTERN;
    static const std::regex ODATA_PATTERN;
    static const std::regex GENERIC_URL_PATTERN;
};

} // namespace erpl_web

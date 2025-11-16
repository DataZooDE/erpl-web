#pragma once

#include <string>

namespace erpl_web {

/**
 * SAC Infrastructure Constants
 *
 * Centralizes magic strings used across SAC (SAP Analytics Cloud) and
 * Datasphere modules. Single source of truth for domains, URLs, field names,
 * and trace identifiers.
 */

// ===== Domains =====
namespace constants::domains {
	// SAC cloud domain
	inline constexpr std::string_view SAC_CLOUD = "sapanalytics.cloud";

	// Datasphere cloud domain
	inline constexpr std::string_view DATASPHERE_CLOUD = "hcs.cloud.sap";

	// Domain separators and patterns
	inline constexpr std::string_view CLOUD_SUFFIX = ".sapanalytics.cloud";
	inline constexpr std::string_view DATASPHERE_SUFFIX = ".hcs.cloud.sap";
}

// ===== API Endpoints =====
namespace constants::api_endpoints {
	// OData service root
	inline constexpr std::string_view ODATA_SERVICE_ROOT = "/api/odata/v2";

	// Planning data endpoints
	inline constexpr std::string_view PLANNING_DATA = "/api/planning/data";
	inline constexpr std::string_view PLANNING_MODELS = "/api/planning/models";
	inline constexpr std::string_view PLANNING_DIMENSIONS = "/dimensions";
	inline constexpr std::string_view PLANNING_MEASURES = "/measures";

	// Story service endpoints
	inline constexpr std::string_view STORY_SERVICE = "/api/story/v1";
	inline constexpr std::string_view STORY_LIST = "/stories";
	inline constexpr std::string_view STORY_INFO = "/info";

	// Datasphere endpoints
	inline constexpr std::string_view DATASPHERE_API = "/api/v1";
	inline constexpr std::string_view DATASPHERE_SPACES = "/spaces";
	inline constexpr std::string_view DATASPHERE_ASSETS = "/assets";

	// OAuth endpoints
	inline constexpr std::string_view OAUTH_TOKEN = "/oauth/token";
	inline constexpr std::string_view OAUTH_AUTHORIZE = "/oauth/authorize";
}

// ===== OData Constants =====
namespace constants::odata {
	// Response wrapper fields
	inline constexpr std::string_view RESPONSE_VALUE = "value";
	inline constexpr std::string_view RESPONSE_D = "d";
	inline constexpr std::string_view RESPONSE_RESULTS = "results";

	// OData v4 metadata fields
	inline constexpr std::string_view ODATA_CONTEXT = "@odata.context";
	inline constexpr std::string_view ODATA_TYPE = "@odata.type";
	inline constexpr std::string_view ODATA_METADATA = "@odata.metadata";

	// OData v2 metadata fields
	inline constexpr std::string_view METADATA_V2 = "__metadata";
	inline constexpr std::string_view METADATA_TYPE = "type";
	inline constexpr std::string_view METADATA_URI = "uri";

	// Query parameters
	inline constexpr std::string_view QUERY_FILTER = "$filter";
	inline constexpr std::string_view QUERY_SELECT = "$select";
	inline constexpr std::string_view QUERY_TOP = "$top";
	inline constexpr std::string_view QUERY_SKIP = "$skip";
	inline constexpr std::string_view QUERY_ORDERBY = "$orderby";
	inline constexpr std::string_view QUERY_EXPAND = "$expand";
	inline constexpr std::string_view QUERY_COUNT = "$count";

	// Query operators
	inline constexpr std::string_view FILTER_EQ = " eq ";
	inline constexpr std::string_view FILTER_AND = " and ";
	inline constexpr std::string_view FILTER_OR = " or ";
	inline constexpr std::string_view FILTER_NOT = " not ";

	// Entity field names (SAC)
	inline constexpr std::string_view ENTITY_ID = "ID";
	inline constexpr std::string_view ENTITY_NAME = "name";
	inline constexpr std::string_view ENTITY_DESCRIPTION = "description";
	inline constexpr std::string_view ENTITY_TYPE = "type";
	inline constexpr std::string_view ENTITY_OWNER = "owner";
	inline constexpr std::string_view ENTITY_CREATED = "createdAt";
	inline constexpr std::string_view ENTITY_MODIFIED = "modifiedAt";

	// Common OData types
	inline constexpr std::string_view TYPE_PLANNING = "PLANNING";
	inline constexpr std::string_view TYPE_ANALYTICS = "ANALYTICS";
	inline constexpr std::string_view TYPE_DRAFT = "DRAFT";
	inline constexpr std::string_view TYPE_PUBLISHED = "PUBLISHED";
}

// ===== Trace Components =====
namespace constants::trace {
	// SAC tracing
	inline constexpr std::string_view COMP_SAC_CATALOG = "SAC_CATALOG";
	inline constexpr std::string_view COMP_SAC_CLIENT = "SAC_CLIENT";
	inline constexpr std::string_view COMP_SAC_URL = "SAC_URL";
	inline constexpr std::string_view COMP_SAC_READ = "SAC_READ";

	// Datasphere tracing
	inline constexpr std::string_view COMP_DATASPHERE = "DATASPHERE";
	inline constexpr std::string_view COMP_DATASPHERE_CLIENT = "DATASPHERE_CLIENT";
	inline constexpr std::string_view COMP_DATASPHERE_CATALOG = "DATASPHERE_CATALOG";

	// OData tracing
	inline constexpr std::string_view COMP_ODATA = "ODATA";
	inline constexpr std::string_view COMP_ODATA_QUERY = "ODATA_QUERY";

	// Trace messages
	inline constexpr std::string_view MSG_LISTING = "Listing";
	inline constexpr std::string_view MSG_FETCHING = "Fetching";
	inline constexpr std::string_view MSG_PARSING = "Parsing";
	inline constexpr std::string_view MSG_FAILED = "Failed";
	inline constexpr std::string_view MSG_STUB = "[STUB]";
}

// ===== HTTP Constants =====
namespace constants::http {
	// Content types
	inline constexpr std::string_view CONTENT_TYPE_JSON = "application/json";
	inline constexpr std::string_view CONTENT_TYPE_XML = "application/xml";
	inline constexpr std::string_view CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";
	inline constexpr std::string_view CONTENT_TYPE_OCTET = "application/octet-stream";

	// HTTP methods
	inline constexpr std::string_view METHOD_GET = "GET";
	inline constexpr std::string_view METHOD_POST = "POST";
	inline constexpr std::string_view METHOD_PUT = "PUT";
	inline constexpr std::string_view METHOD_PATCH = "PATCH";
	inline constexpr std::string_view METHOD_DELETE = "DELETE";

	// Headers
	inline constexpr std::string_view HEADER_CONTENT_TYPE = "Content-Type";
	inline constexpr std::string_view HEADER_AUTHORIZATION = "Authorization";
	inline constexpr std::string_view HEADER_ACCEPT = "Accept";
	inline constexpr std::string_view HEADER_CHARSET = "charset";

	// Query separators
	inline constexpr std::string_view QUERY_SEP_PARAMS = "&";
	inline constexpr std::string_view QUERY_SEP_KEY_VALUE = "=";
	inline constexpr std::string_view QUERY_START = "?";
}

// ===== OAuth2 Constants =====
namespace constants::oauth2 {
	// Token types
	inline constexpr std::string_view BEARER = "Bearer";
	inline constexpr std::string_view BASIC = "Basic";

	// Grant types
	inline constexpr std::string_view GRANT_CLIENT_CREDENTIALS = "client_credentials";
	inline constexpr std::string_view GRANT_AUTHORIZATION_CODE = "authorization_code";
	inline constexpr std::string_view GRANT_REFRESH = "refresh_token";

	// OAuth parameters
	inline constexpr std::string_view PARAM_CLIENT_ID = "client_id";
	inline constexpr std::string_view PARAM_CLIENT_SECRET = "client_secret";
	inline constexpr std::string_view PARAM_GRANT_TYPE = "grant_type";
	inline constexpr std::string_view PARAM_SCOPE = "scope";
	inline constexpr std::string_view PARAM_CODE = "code";
	inline constexpr std::string_view PARAM_REDIRECT_URI = "redirect_uri";
	inline constexpr std::string_view PARAM_ACCESS_TOKEN = "access_token";
	inline constexpr std::string_view PARAM_TOKEN_TYPE = "token_type";
	inline constexpr std::string_view PARAM_EXPIRES_IN = "expires_in";
	inline constexpr std::string_view PARAM_REFRESH_TOKEN = "refresh_token";
}

// ===== Secrets Management =====
namespace constants::secrets {
	// Secret type names
	inline constexpr std::string_view SECRET_TYPE_SAC = "sac";
	inline constexpr std::string_view SECRET_TYPE_DATASPHERE = "datasphere";
	inline constexpr std::string_view SECRET_TYPE_OAUTH2 = "oauth2";

	// Secret field names
	inline constexpr std::string_view FIELD_TENANT = "tenant";
	inline constexpr std::string_view FIELD_REGION = "region";
	inline constexpr std::string_view FIELD_SECRET_NAME = "secret";
	inline constexpr std::string_view FIELD_USERNAME = "username";
	inline constexpr std::string_view FIELD_PASSWORD = "password";
}

// ===== Datasphere Constants =====
namespace constants::datasphere {
	// Space types
	inline constexpr std::string_view SPACE_TYPE_REPOSITORY = "REPOSITORY";
	inline constexpr std::string_view SPACE_TYPE_TEAM = "TEAM";
	inline constexpr std::string_view SPACE_TYPE_SHARED = "SHARED";

	// Asset types
	inline constexpr std::string_view ASSET_TYPE_TABLE = "TABLE";
	inline constexpr std::string_view ASSET_TYPE_VIEW = "VIEW";
	inline constexpr std::string_view ASSET_TYPE_ANALYTIC = "ANALYTIC";
	inline constexpr std::string_view ASSET_TYPE_PROC = "PROC";

	// Datasphere API fields
	inline constexpr std::string_view FIELD_SPACE_ID = "spaceID";
	inline constexpr std::string_view FIELD_SPACE_NAME = "spaceName";
	inline constexpr std::string_view FIELD_ASSET_ID = "assetID";
	inline constexpr std::string_view FIELD_ASSET_NAME = "assetName";
}

} // namespace erpl_web

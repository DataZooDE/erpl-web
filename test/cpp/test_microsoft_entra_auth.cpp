#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "microsoft_entra_secret.hpp"
#include <iostream>
#include <memory>

using namespace erpl_web;
using namespace std;

// ============================================================================
// Microsoft Entra Token URL Tests
// ============================================================================

TEST_CASE("Test Microsoft Entra Token URL Generation", "[microsoft_entra][basic]")
{
    std::cout << std::endl;

    // Test token URL construction with tenant ID
    auto token_url = MicrosoftEntraTokenManager::GetTokenUrl("my-tenant-id");
    REQUIRE(token_url == "https://login.microsoftonline.com/my-tenant-id/oauth2/v2.0/token");

    // Test with common tenant
    auto common_url = MicrosoftEntraTokenManager::GetTokenUrl("common");
    REQUIRE(common_url == "https://login.microsoftonline.com/common/oauth2/v2.0/token");

    // Test with organizations tenant
    auto org_url = MicrosoftEntraTokenManager::GetTokenUrl("organizations");
    REQUIRE(org_url == "https://login.microsoftonline.com/organizations/oauth2/v2.0/token");
}

TEST_CASE("Test Microsoft Entra Authorization URL Generation", "[microsoft_entra][basic]")
{
    std::cout << std::endl;

    // Test authorization URL construction
    auto auth_url = MicrosoftEntraTokenManager::GetAuthorizationUrl("my-tenant-id");
    REQUIRE(auth_url == "https://login.microsoftonline.com/my-tenant-id/oauth2/v2.0/authorize");

    // Test with consumers tenant (personal Microsoft accounts)
    auto consumer_url = MicrosoftEntraTokenManager::GetAuthorizationUrl("consumers");
    REQUIRE(consumer_url == "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize");
}

// ============================================================================
// Microsoft Entra Secret Data Tests
// ============================================================================

TEST_CASE("Test Microsoft Entra Secret Data Token Validation", "[microsoft_entra][basic]")
{
    std::cout << std::endl;

    MicrosoftEntraSecretData secret_data;

    // Test 1: No token - should not be valid
    REQUIRE(secret_data.HasValidToken() == false);
    REQUIRE(secret_data.IsTokenExpired() == true);

    // Test 2: Token with no expiry - should be expired
    secret_data.access_token = "test-access-token";
    REQUIRE(secret_data.HasValidToken() == false);
    REQUIRE(secret_data.IsTokenExpired() == true);

    // Test 3: Token with future expiry - should be valid
    auto future_time = std::chrono::system_clock::now() + std::chrono::hours(1);
    auto future_timestamp = std::chrono::system_clock::to_time_t(future_time);
    secret_data.expires_at = std::to_string(future_timestamp);
    REQUIRE(secret_data.HasValidToken() == true);
    REQUIRE(secret_data.IsTokenExpired() == false);

    // Test 4: Token with past expiry - should be expired
    auto past_time = std::chrono::system_clock::now() - std::chrono::hours(1);
    auto past_timestamp = std::chrono::system_clock::to_time_t(past_time);
    secret_data.expires_at = std::to_string(past_timestamp);
    REQUIRE(secret_data.HasValidToken() == false);
    REQUIRE(secret_data.IsTokenExpired() == true);
}

TEST_CASE("Test Microsoft Entra Secret Data Expiration Buffer", "[microsoft_entra][basic]")
{
    std::cout << std::endl;

    MicrosoftEntraSecretData secret_data;
    secret_data.access_token = "test-access-token";

    // Token expiring in 3 minutes should be considered expired (5-minute buffer)
    auto near_future = std::chrono::system_clock::now() + std::chrono::minutes(3);
    auto near_timestamp = std::chrono::system_clock::to_time_t(near_future);
    secret_data.expires_at = std::to_string(near_timestamp);

    // Should be expired due to 5-minute buffer
    REQUIRE(secret_data.IsTokenExpired() == true);

    // Token expiring in 10 minutes should be valid
    auto safe_future = std::chrono::system_clock::now() + std::chrono::minutes(10);
    auto safe_timestamp = std::chrono::system_clock::to_time_t(safe_future);
    secret_data.expires_at = std::to_string(safe_timestamp);

    REQUIRE(secret_data.IsTokenExpired() == false);
}

// ============================================================================
// Integration Tests with DuckDB (require mock or skip)
// ============================================================================

TEST_CASE("Test Microsoft Entra Secret Creation in DuckDB", "[microsoft_entra][integration]")
{
    std::cout << std::endl;

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // Load the extension
    auto result = con.Query("LOAD erpl_web;");
    if (result->HasError()) {
        std::cout << "Note: Extension not loaded, skipping integration test" << std::endl;
        return;
    }

    // Create a secret
    result = con.Query(R"(
        CREATE SECRET test_integration_secret (
            TYPE microsoft_entra,
            tenant_id 'test-tenant-guid',
            client_id 'test-client-guid',
            client_secret 'test-client-secret-value',
            scope 'https://graph.microsoft.com/.default'
        )
    )");
    REQUIRE(!result->HasError());

    // Verify the secret exists
    result = con.Query("SELECT name, type, provider FROM duckdb_secrets() WHERE name = 'test_integration_secret'");
    REQUIRE(!result->HasError());

    auto chunk = result->Fetch();
    REQUIRE(chunk);
    REQUIRE(chunk->size() == 1);

    // Verify values
    REQUIRE(chunk->GetValue(0, 0).ToString() == "test_integration_secret");
    REQUIRE(chunk->GetValue(1, 0).ToString() == "microsoft_entra");
    REQUIRE(chunk->GetValue(2, 0).ToString() == "client_credentials");

    // Cleanup
    con.Query("DROP SECRET IF EXISTS test_integration_secret");
}

TEST_CASE("Test Microsoft Entra Config Provider", "[microsoft_entra][integration]")
{
    std::cout << std::endl;

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // Load the extension
    auto result = con.Query("LOAD erpl_web;");
    if (result->HasError()) {
        std::cout << "Note: Extension not loaded, skipping integration test" << std::endl;
        return;
    }

    // Create a config secret with pre-acquired token
    result = con.Query(R"(
        CREATE SECRET test_config_secret (
            TYPE microsoft_entra,
            PROVIDER config,
            tenant_id 'config-tenant',
            client_id 'config-client',
            access_token 'pre-acquired-token-value'
        )
    )");
    REQUIRE(!result->HasError());

    // Verify the secret exists with config provider
    result = con.Query("SELECT name, type, provider FROM duckdb_secrets() WHERE name = 'test_config_secret'");
    REQUIRE(!result->HasError());

    auto chunk = result->Fetch();
    REQUIRE(chunk);
    REQUIRE(chunk->size() == 1);

    REQUIRE(chunk->GetValue(2, 0).ToString() == "config");

    // Cleanup
    con.Query("DROP SECRET IF EXISTS test_config_secret");
}

// ============================================================================
// Scope Configuration Tests
// ============================================================================

TEST_CASE("Test Microsoft Entra Scope Configuration", "[microsoft_entra][basic]")
{
    std::cout << std::endl;

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    auto result = con.Query("LOAD erpl_web;");
    if (result->HasError()) {
        std::cout << "Note: Extension not loaded, skipping scope test" << std::endl;
        return;
    }

    // Test with Graph API scope
    result = con.Query(R"(
        CREATE SECRET graph_scope_secret (
            TYPE microsoft_entra,
            tenant_id 'test-tenant',
            client_id 'test-client',
            client_secret 'test-secret',
            scope 'https://graph.microsoft.com/.default'
        )
    )");
    REQUIRE(!result->HasError());

    // Test with Dynamics 365 scope
    result = con.Query(R"(
        CREATE SECRET dynamics_scope_secret (
            TYPE microsoft_entra,
            tenant_id 'test-tenant',
            client_id 'test-client',
            client_secret 'test-secret',
            scope 'https://org.crm.dynamics.com/.default'
        )
    )");
    REQUIRE(!result->HasError());

    // Test with Business Central scope
    result = con.Query(R"(
        CREATE SECRET bc_scope_secret (
            TYPE microsoft_entra,
            tenant_id 'test-tenant',
            client_id 'test-client',
            client_secret 'test-secret',
            scope 'https://api.businesscentral.dynamics.com/.default'
        )
    )");
    REQUIRE(!result->HasError());

    // Cleanup
    con.Query("DROP SECRET IF EXISTS graph_scope_secret");
    con.Query("DROP SECRET IF EXISTS dynamics_scope_secret");
    con.Query("DROP SECRET IF EXISTS bc_scope_secret");
}

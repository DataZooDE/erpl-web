#include "catch.hpp"
#include "odp_subscription_repository.hpp"
#include "duckdb.hpp"
#include <chrono>
#include <thread>

using namespace erpl_web;
using namespace duckdb;

TEST_CASE("OdpSubscriptionRepository - Basic Operations", "[odp_repository]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    OdpSubscriptionRepository repo(context);
    
    SECTION("Schema and table creation") {
        REQUIRE_NOTHROW(repo.EnsureSchemaExists());
        REQUIRE_NOTHROW(repo.EnsureTablesExist());
        
        // Verify schema exists
        auto result = conn.Query("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'erpl_web'");
        REQUIRE(result->RowCount() == 1);
        
        // Verify tables exist
        auto tables_result = conn.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'erpl_web'");
        REQUIRE(tables_result->RowCount() >= 2); // At least subscriptions and audit tables
    }
    
    SECTION("Subscription creation and retrieval") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest";
        std::string entity_set_name = "EntityOfTest";
        std::string secret_name = "test_secret";
        
        // Create subscription
        std::string subscription_id = repo.CreateSubscription(service_url, entity_set_name, secret_name);
        REQUIRE(!subscription_id.empty());
        
        // Verify subscription ID format (timestamp_cleaned_url)
        REQUIRE(subscription_id.length() > 15); // At least YYYYMMDD_HHMMSS
        REQUIRE(subscription_id.find("_") != std::string::npos);
        
        // Retrieve subscription
        auto subscription = repo.GetSubscription(subscription_id);
        REQUIRE(subscription.has_value());
        REQUIRE(subscription->subscription_id == subscription_id);
        REQUIRE(subscription->service_url == service_url);
        REQUIRE(subscription->entity_set_name == entity_set_name);
        REQUIRE(subscription->secret_name == secret_name);
        REQUIRE(subscription->subscription_status == "active");
        REQUIRE(subscription->preference_applied == false);
    }
    
    SECTION("Find active subscription") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest2";
        std::string entity_set_name = "EntityOfTest2";
        
        // No subscription should exist initially
        auto existing = repo.FindActiveSubscription(service_url, entity_set_name);
        REQUIRE(!existing.has_value());
        
        // Create subscription
        std::string subscription_id = repo.CreateSubscription(service_url, entity_set_name);
        
        // Should find the active subscription
        existing = repo.FindActiveSubscription(service_url, entity_set_name);
        REQUIRE(existing.has_value());
        REQUIRE(existing->subscription_id == subscription_id);
    }
    
    SECTION("Delta token updates") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest3";
        std::string entity_set_name = "EntityOfTest3";
        
        // Create subscription
        std::string subscription_id = repo.CreateSubscription(service_url, entity_set_name);
        
        // Update delta token
        std::string delta_token = "test_delta_token_12345";
        bool success = repo.UpdateDeltaToken(subscription_id, delta_token);
        REQUIRE(success);
        
        // Verify update
        auto subscription = repo.GetSubscription(subscription_id);
        REQUIRE(subscription.has_value());
        REQUIRE(subscription->delta_token == delta_token);
    }
    
    SECTION("Subscription status updates") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest4";
        std::string entity_set_name = "EntityOfTest4";
        
        // Create subscription
        std::string subscription_id = repo.CreateSubscription(service_url, entity_set_name);
        
        // Update status
        bool success = repo.UpdateSubscriptionStatus(subscription_id, "terminated");
        REQUIRE(success);
        
        // Verify update
        auto subscription = repo.GetSubscription(subscription_id);
        REQUIRE(subscription.has_value());
        REQUIRE(subscription->subscription_status == "terminated");
    }
    
    SECTION("List subscriptions") {
        // Create multiple subscriptions
        std::string base_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/";
        
        repo.CreateSubscription(base_url + "EntityOfTest5", "EntityOfTest5");
        repo.CreateSubscription(base_url + "EntityOfTest6", "EntityOfTest6");
        
        // List all subscriptions
        auto subscriptions = repo.ListAllSubscriptions();
        REQUIRE(subscriptions.size() >= 2);
        
        // Verify ordering (should be by created_at DESC due to timestamp prefix)
        if (subscriptions.size() >= 2) {
            REQUIRE(subscriptions[0].created_at >= subscriptions[1].created_at);
        }
    }
    
    SECTION("Audit entry creation") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest7";
        std::string entity_set_name = "EntityOfTest7";
        
        // Create subscription
        std::string subscription_id = repo.CreateSubscription(service_url, entity_set_name);
        
        // Create audit entry
        OdpAuditEntry entry(subscription_id, "initial_load");
        entry.request_url = service_url;
        entry.delta_token_before = "";
        
        int64_t audit_id = repo.CreateAuditEntry(entry);
        REQUIRE(audit_id > 0);
        
        // Update audit entry
        entry.audit_id = audit_id;
        entry.response_timestamp = std::chrono::system_clock::now();
        entry.http_status_code = 200;
        entry.rows_fetched = 100;
        entry.package_size_bytes = 1024;
        entry.delta_token_after = "new_delta_token";
        entry.duration_ms = 500;
        
        bool success = repo.UpdateAuditEntry(entry);
        REQUIRE(success);
    }
    
    SECTION("Remove subscription") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest8";
        std::string entity_set_name = "EntityOfTest8";
        
        // Create subscription
        std::string subscription_id = repo.CreateSubscription(service_url, entity_set_name);
        
        // Verify it exists
        auto subscription = repo.GetSubscription(subscription_id);
        REQUIRE(subscription.has_value());
        
        // Remove subscription
        bool success = repo.RemoveSubscription(subscription_id);
        REQUIRE(success);
        
        // Verify it's gone
        subscription = repo.GetSubscription(subscription_id);
        REQUIRE(!subscription.has_value());
    }
}

TEST_CASE("OdpSubscriptionRepository - Utility Methods", "[odp_repository_utils]") {
    SECTION("Generate subscription ID") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest";
        std::string entity_set_name = "EntityOfTest";
        
        std::string id1 = OdpSubscriptionRepository::GenerateSubscriptionId(service_url, entity_set_name);
        
        // Wait a second to ensure different timestamp
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        std::string id2 = OdpSubscriptionRepository::GenerateSubscriptionId(service_url, entity_set_name);
        
        // IDs should be different due to timestamp
        REQUIRE(id1 != id2);
        
        // Both should contain timestamp pattern
        REQUIRE(id1.find("_") != std::string::npos);
        REQUIRE(id2.find("_") != std::string::npos);
        
        // ID1 should be lexicographically smaller (earlier timestamp)
        REQUIRE(id1 < id2);
    }
    
    SECTION("Clean URL for ID") {
        std::string url1 = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest";
        std::string cleaned1 = OdpSubscriptionRepository::CleanUrlForId(url1);
        
        REQUIRE(cleaned1.find("https") == std::string::npos); // Protocol removed
        REQUIRE(cleaned1.find("://") == std::string::npos);   // Protocol separator removed
        REQUIRE(cleaned1.find("/") == std::string::npos);     // Slashes replaced
        REQUIRE(!cleaned1.empty());
        
        // Should contain underscores instead of special characters
        REQUIRE(cleaned1.find("_") != std::string::npos);
    }
    
    SECTION("Validate ODP URL") {
        // Valid ODP URLs
        REQUIRE(OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/EntityOfTest"));
        REQUIRE(OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/FactsOfTest"));
        REQUIRE(OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfSEPM_ISO"));
        REQUIRE(OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/sap/opu/odata/sap/TEST_SRV/FactsOf0D_NW_C01"));
        
        // Invalid URLs
        REQUIRE(!OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/RegularEntity"));
        REQUIRE(!OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/TestEntity"));
        REQUIRE(!OdpSubscriptionRepository::IsValidOdpUrl("https://test.com/"));
        REQUIRE(!OdpSubscriptionRepository::IsValidOdpUrl(""));
    }
}

TEST_CASE("OdpSubscriptionRepository - Error Handling", "[odp_repository_errors]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    OdpSubscriptionRepository repo(context);
    repo.EnsureTablesExist();
    
    SECTION("Invalid URL creation") {
        REQUIRE_THROWS_AS(
            repo.CreateSubscription("https://invalid.com/RegularEntity", "RegularEntity"),
            InvalidInputException
        );
    }
    
    SECTION("Non-existent subscription operations") {
        std::string fake_id = "20240101_120000_fake_subscription";
        
        // Get non-existent subscription
        auto subscription = repo.GetSubscription(fake_id);
        REQUIRE(!subscription.has_value());
        
        // Update non-existent subscription
        bool success = repo.UpdateDeltaToken(fake_id, "test_token");
        REQUIRE(!success); // Should fail gracefully
        
        success = repo.UpdateSubscriptionStatus(fake_id, "terminated");
        REQUIRE(!success); // Should fail gracefully
        
        // Remove non-existent subscription
        success = repo.RemoveSubscription(fake_id);
        REQUIRE(!success); // Should fail gracefully
    }
    
    SECTION("Duplicate subscription handling") {
        std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfDuplicate";
        std::string entity_set_name = "EntityOfDuplicate";
        
        // Create first subscription
        std::string id1 = repo.CreateSubscription(service_url, entity_set_name);
        REQUIRE(!id1.empty());
        
        // Attempt to create duplicate - should return existing ID
        std::string id2 = repo.CreateSubscription(service_url, entity_set_name);
        REQUIRE(id1 == id2);
    }
}

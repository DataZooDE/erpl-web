#include "catch.hpp"
#include "odp_subscription_state_manager.hpp"
#include "duckdb.hpp"

using namespace erpl_web;
using namespace duckdb;

TEST_CASE("OdpSubscriptionStateManager - Basic State Management", "[odp_state_manager]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest";
    std::string entity_set_name = "EntityOfTest";
    std::string secret_name = "test_secret";
    
    SECTION("Initial state - force full load") {
        OdpSubscriptionStateManager manager(context, service_url, entity_set_name, secret_name, true);
        
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::INITIAL_LOAD);
        REQUIRE(manager.ShouldPerformInitialLoad());
        REQUIRE(!manager.ShouldPerformDeltaFetch());
        REQUIRE(manager.GetCurrentDeltaToken().empty());
        REQUIRE(manager.IsSubscriptionActive());
        REQUIRE(!manager.GetSubscriptionId().empty());
    }
    
    SECTION("Initial state - existing subscription with token") {
        // First create a subscription with a delta token
        {
            OdpSubscriptionStateManager manager1(context, service_url, entity_set_name, secret_name, true);
            manager1.TransitionToDeltaFetch("test_delta_token_123", true);
        }
        
        // Now create a new manager for the same subscription (should find existing)
        OdpSubscriptionStateManager manager2(context, service_url, entity_set_name, secret_name, false);
        
        REQUIRE(manager2.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::DELTA_FETCH);
        REQUIRE(!manager2.ShouldPerformInitialLoad());
        REQUIRE(manager2.ShouldPerformDeltaFetch());
        REQUIRE(manager2.GetCurrentDeltaToken() == "test_delta_token_123");
        REQUIRE(manager2.IsSubscriptionActive());
    }
    
    SECTION("Import delta token") {
        std::string import_token = "imported_delta_token_456";
        OdpSubscriptionStateManager manager(context, service_url + "2", "EntityOfTest2", secret_name, false, import_token);
        
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::DELTA_FETCH);
        REQUIRE(manager.GetCurrentDeltaToken() == import_token);
        REQUIRE(manager.ShouldPerformDeltaFetch());
    }
}

TEST_CASE("OdpSubscriptionStateManager - State Transitions", "[odp_state_transitions]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTransition";
    std::string entity_set_name = "EntityOfTransition";
    
    OdpSubscriptionStateManager manager(context, service_url, entity_set_name, "", true);
    
    SECTION("Initial Load -> Delta Fetch") {
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::INITIAL_LOAD);
        
        // Transition to delta fetch
        std::string delta_token = "delta_token_after_initial_load";
        manager.TransitionToDeltaFetch(delta_token, true);
        
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::DELTA_FETCH);
        REQUIRE(manager.GetCurrentDeltaToken() == delta_token);
        REQUIRE(manager.ShouldPerformDeltaFetch());
        REQUIRE(!manager.ShouldPerformInitialLoad());
        REQUIRE(manager.IsSubscriptionActive());
    }
    
    SECTION("Delta Fetch -> Initial Load (reset)") {
        // First transition to delta fetch
        manager.TransitionToDeltaFetch("some_token", true);
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::DELTA_FETCH);
        
        // Reset to initial load
        manager.TransitionToInitialLoad();
        
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::INITIAL_LOAD);
        REQUIRE(manager.GetCurrentDeltaToken().empty());
        REQUIRE(manager.ShouldPerformInitialLoad());
        REQUIRE(!manager.ShouldPerformDeltaFetch());
        REQUIRE(manager.IsSubscriptionActive());
    }
    
    SECTION("Any State -> Terminated") {
        manager.TransitionToTerminated();
        
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::TERMINATED);
        REQUIRE(!manager.IsSubscriptionActive());
        REQUIRE(!manager.ShouldPerformInitialLoad());
        REQUIRE(!manager.ShouldPerformDeltaFetch());
    }
    
    SECTION("Any State -> Error") {
        std::string error_msg = "Test error message";
        manager.TransitionToError(error_msg);
        
        REQUIRE(manager.GetCurrentPhase() == OdpSubscriptionStateManager::SubscriptionPhase::ERROR_STATE);
        REQUIRE(!manager.IsSubscriptionActive());
        REQUIRE(!manager.ShouldPerformInitialLoad());
        REQUIRE(!manager.ShouldPerformDeltaFetch());
    }
}

TEST_CASE("OdpSubscriptionStateManager - Delta Token Management", "[odp_delta_tokens]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfDelta";
    std::string entity_set_name = "EntityOfDelta";
    
    OdpSubscriptionStateManager manager(context, service_url, entity_set_name, "", true);
    
    SECTION("Update delta token") {
        std::string initial_token = "initial_delta_token";
        std::string updated_token = "updated_delta_token";
        
        // Set initial token
        manager.TransitionToDeltaFetch(initial_token, true);
        REQUIRE(manager.GetCurrentDeltaToken() == initial_token);
        
        // Update token
        manager.UpdateDeltaToken(updated_token);
        REQUIRE(manager.GetCurrentDeltaToken() == updated_token);
        
        // Verify persistence by creating new manager
        OdpSubscriptionStateManager manager2(context, service_url, entity_set_name, "", false);
        REQUIRE(manager2.GetCurrentDeltaToken() == updated_token);
    }
    
    SECTION("Multiple token updates") {
        std::vector<std::string> tokens = {
            "token_001",
            "token_002", 
            "token_003"
        };
        
        for (const auto& token : tokens) {
            manager.UpdateDeltaToken(token);
            REQUIRE(manager.GetCurrentDeltaToken() == token);
        }
        
        // Final token should be persisted
        REQUIRE(manager.GetCurrentDeltaToken() == tokens.back());
    }
}

TEST_CASE("OdpSubscriptionStateManager - Audit Operations", "[odp_audit]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    std::string service_url = "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfAudit";
    std::string entity_set_name = "EntityOfAudit";
    
    OdpSubscriptionStateManager manager(context, service_url, entity_set_name);
    
    SECTION("Create and update audit entry") {
        std::string request_url = service_url + "?$format=json";
        
        // Create audit entry
        int64_t audit_id = manager.CreateAuditEntry("initial_load", request_url);
        REQUIRE(audit_id > 0);
        
        // Update audit entry
        manager.UpdateAuditEntry(audit_id, 200, 150, 2048, "new_delta_token", "", 750);
        
        // Verify the audit entry was created (we can't easily verify the update without direct DB access)
        REQUIRE(audit_id > 0);
    }
    
    SECTION("Multiple audit entries") {
        std::vector<std::string> operations = {"initial_load", "delta_fetch", "delta_fetch"};
        std::vector<int64_t> audit_ids;
        
        for (const auto& operation : operations) {
            int64_t audit_id = manager.CreateAuditEntry(operation, service_url);
            REQUIRE(audit_id > 0);
            audit_ids.push_back(audit_id);
            
            // Update each entry
            manager.UpdateAuditEntry(audit_id, 200, 100, 1024);
        }
        
        // All audit IDs should be unique and increasing
        for (size_t i = 1; i < audit_ids.size(); i++) {
            REQUIRE(audit_ids[i] > audit_ids[i-1]);
        }
    }
}

TEST_CASE("OdpSubscriptionStateManager - Utility Methods", "[odp_state_utils]") {
    SECTION("Phase to string conversion") {
        REQUIRE(OdpSubscriptionStateManager::PhaseToString(
            OdpSubscriptionStateManager::SubscriptionPhase::INITIAL_LOAD) == "INITIAL_LOAD");
        REQUIRE(OdpSubscriptionStateManager::PhaseToString(
            OdpSubscriptionStateManager::SubscriptionPhase::DELTA_FETCH) == "DELTA_FETCH");
        REQUIRE(OdpSubscriptionStateManager::PhaseToString(
            OdpSubscriptionStateManager::SubscriptionPhase::TERMINATED) == "TERMINATED");
        REQUIRE(OdpSubscriptionStateManager::PhaseToString(
            OdpSubscriptionStateManager::SubscriptionPhase::ERROR_STATE) == "ERROR_STATE");
    }
    
    SECTION("Log current state (should not throw)") {
        DuckDB db(nullptr);
        Connection conn(db);
        ClientContext& context = *conn.context;
        
        OdpSubscriptionStateManager manager(context, 
            "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfLog", "EntityOfLog");
        
        REQUIRE_NOTHROW(manager.LogCurrentState());
        
        // Transition to different states and log
        manager.TransitionToDeltaFetch("test_token", true);
        REQUIRE_NOTHROW(manager.LogCurrentState());
        
        manager.TransitionToError("test error");
        REQUIRE_NOTHROW(manager.LogCurrentState());
    }
}

TEST_CASE("OdpSubscriptionStateManager - Error Handling", "[odp_state_errors]") {
    DuckDB db(nullptr);
    Connection conn(db);
    ClientContext& context = *conn.context;
    
    SECTION("Invalid URL validation") {
        REQUIRE_THROWS_AS(
            OdpSubscriptionStateManager(context, "", "EntityOfTest"),
            InvalidInputException
        );
        
        REQUIRE_THROWS_AS(
            OdpSubscriptionStateManager(context, "https://invalid.com/RegularEntity", "RegularEntity"),
            InvalidInputException
        );
    }
    
    SECTION("Empty entity set name") {
        REQUIRE_THROWS_AS(
            OdpSubscriptionStateManager(context, "https://test.com/sap/opu/odata/sap/TEST_SRV/EntityOfTest", ""),
            InvalidInputException
        );
    }
}

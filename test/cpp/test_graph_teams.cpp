#include "catch.hpp"
#include "graph_teams_client.hpp"
#include "duckdb.hpp"

using namespace erpl_web;
using namespace duckdb;

// =============================================================================
// URL Builder Tests
// =============================================================================

TEST_CASE("GraphTeamsUrlBuilder builds correct base URL", "[graph_teams][url]") {
    REQUIRE(GraphTeamsUrlBuilder::GetBaseUrl() == "https://graph.microsoft.com/v1.0");
}

TEST_CASE("GraphTeamsUrlBuilder builds team URLs", "[graph_teams][url]") {
    SECTION("My teams URL") {
        auto url = GraphTeamsUrlBuilder::BuildMyTeamsUrl();
        REQUIRE(url == "https://graph.microsoft.com/v1.0/me/joinedTeams");
    }

    SECTION("Single team URL") {
        auto url = GraphTeamsUrlBuilder::BuildTeamUrl("team-123");
        REQUIRE(url == "https://graph.microsoft.com/v1.0/teams/team-123");
    }
}

TEST_CASE("GraphTeamsUrlBuilder builds channel URLs", "[graph_teams][url]") {
    SECTION("Team channels URL") {
        auto url = GraphTeamsUrlBuilder::BuildTeamChannelsUrl("team-abc");
        REQUIRE(url == "https://graph.microsoft.com/v1.0/teams/team-abc/channels");
    }

    SECTION("Single channel URL") {
        auto url = GraphTeamsUrlBuilder::BuildChannelUrl("team-abc", "channel-xyz");
        REQUIRE(url == "https://graph.microsoft.com/v1.0/teams/team-abc/channels/channel-xyz");
    }
}

TEST_CASE("GraphTeamsUrlBuilder builds member URLs", "[graph_teams][url]") {
    auto url = GraphTeamsUrlBuilder::BuildTeamMembersUrl("team-456");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/teams/team-456/members");
}

TEST_CASE("GraphTeamsUrlBuilder builds message URLs", "[graph_teams][url]") {
    auto url = GraphTeamsUrlBuilder::BuildChannelMessagesUrl("team-abc", "channel-xyz");
    REQUIRE(url == "https://graph.microsoft.com/v1.0/teams/team-abc/channels/channel-xyz/messages");
}

// =============================================================================
// DuckDB Integration Tests
// =============================================================================

TEST_CASE("Graph Teams functions are registered in DuckDB", "[graph_teams][duckdb]") {
    DuckDB db(nullptr);
    Connection con(db);

    // Load extension
    con.Query("LOAD erpl_web");

    SECTION("graph_my_teams function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_my_teams'");
        REQUIRE(result->RowCount() == 1);
    }

    SECTION("graph_team_channels function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_team_channels'");
        REQUIRE(result->RowCount() == 1);
    }

    SECTION("graph_team_members function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_team_members'");
        REQUIRE(result->RowCount() == 1);
    }

    SECTION("graph_channel_messages function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'graph_channel_messages'");
        REQUIRE(result->RowCount() == 1);
    }
}

TEST_CASE("Graph Teams functions require parameters", "[graph_teams][duckdb]") {
    DuckDB db(nullptr);
    Connection con(db);

    con.Query("LOAD erpl_web");

    SECTION("graph_my_teams requires secret") {
        auto result = con.Query("SELECT * FROM graph_my_teams()");
        REQUIRE(result->HasError());
    }

    SECTION("graph_team_channels requires secret and team_id") {
        auto result = con.Query("SELECT * FROM graph_team_channels('secret')");
        REQUIRE(result->HasError());
    }

    SECTION("graph_team_members requires secret and team_id") {
        auto result = con.Query("SELECT * FROM graph_team_members('secret')");
        REQUIRE(result->HasError());
    }

    SECTION("graph_channel_messages requires secret, team_id, and channel_id") {
        auto result = con.Query("SELECT * FROM graph_channel_messages('secret', 'team')");
        REQUIRE(result->HasError());
    }
}

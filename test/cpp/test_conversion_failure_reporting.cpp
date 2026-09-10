#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

#include "conversion_failure_log.hpp"
#include "odata_content.hpp"
#include "odata_read_functions.hpp"

using namespace erpl_web;

namespace {

// A dummy endpoint: port 1 is never listening, so any metadata fetch fails
// immediately instead of hanging. The extractor tests below never need real
// metadata - type resolution falls back and we override the type explicitly.
std::shared_ptr<ODataEntitySetClient> MakeOfflineEntitySetClient()
{
    auto http_client = std::make_shared<HttpClient>();
    auto client = std::make_shared<ODataEntitySetClient>(
        http_client, HttpUrl("http://127.0.0.1:1/service/Things"), nullptr);
    client->SetODataVersionDirectly(ODataVersion::V4);
    return client;
}

} // namespace

// ---------------------------------------------------------------------------
// The reporting mechanism itself.
// ---------------------------------------------------------------------------

TEST_CASE("ConversionFailureLog aggregates per column", "[conversion_failure]")
{
    ConversionFailureLog log;
    REQUIRE_FALSE(log.HasFailures());

    log.RecordFailure("Price", "\"abc\"", "not a number");
    log.RecordFailure("Price", "\"def\"", "also not a number");
    log.RecordFailure("Quantity", "9999999999", "out of range for INTEGER");

    REQUIRE(log.HasFailures());
    REQUIRE(log.TotalFailureCount() == 3);

    auto failures = log.Drain();
    REQUIRE(failures.size() == 2);

    REQUIRE(failures[0].column_name == "Price");
    REQUIRE(failures[0].failure_count == 2);
    // Only the FIRST offending value and message are kept.
    REQUIRE(failures[0].first_offending_value == "\"abc\"");
    REQUIRE(failures[0].first_error_message == "not a number");

    REQUIRE(failures[1].column_name == "Quantity");
    REQUIRE(failures[1].failure_count == 1);

    // Draining clears the log so a scan reports exactly once.
    REQUIRE_FALSE(log.HasFailures());

    const auto summary = ConversionFailureLog::FormatSummary(failures, "odata_read");
    REQUIRE(summary.find("Price") != std::string::npos);
    REQUIRE(summary.find("Quantity") != std::string::npos);
    REQUIRE(summary.find("odata_read") != std::string::npos);
}

TEST_CASE("ConversionFailureLog truncates long offending values", "[conversion_failure]")
{
    ConversionFailureLog log;
    const std::string long_value(ConversionFailureLog::MAX_OFFENDING_VALUE_LENGTH + 50, 'x');
    log.RecordFailure("Blob", long_value, "too long");

    auto failures = log.Drain();
    REQUIRE(failures.size() == 1);
    REQUIRE(failures[0].first_offending_value.size() ==
            ConversionFailureLog::MAX_OFFENDING_VALUE_LENGTH + 3);
}

TEST_CASE("ConversionFailureLog throws under strict typing", "[conversion_failure]")
{
    ConversionFailureLog log;
    log.SetStrictTyping(true);
    REQUIRE(log.IsStrictTyping());

    REQUIRE_THROWS_AS(log.RecordFailure("Price", "\"abc\"", "not a number"), StrictTypingViolation);
    // A refused value is not also recorded - strict typing fails the query.
    REQUIRE_FALSE(log.HasFailures());
}

// ---------------------------------------------------------------------------
// Site 1: ToRows must not turn a conversion failure into a silent NULL.
// ---------------------------------------------------------------------------

TEST_CASE("ToRows reports per-cell conversion failures", "[conversion_failure]")
{
    const std::string json = R"({
        "value": [
            { "Name": "ok",  "Quantity": 7 },
            { "Name": "bad", "Quantity": "not-a-number" },
            { "Name": "bad", "Quantity": "also-bad" }
        ]
    })";

    ODataEntitySetJsonContent content(json);
    auto log = std::make_shared<ConversionFailureLog>();
    content.SetConversionFailureLog(log);

    std::vector<std::string> names = {"Name", "Quantity"};
    std::vector<duckdb::LogicalType> types = {duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR),
                                              duckdb::LogicalType(duckdb::LogicalTypeId::INTEGER)};

    auto rows = content.ToRows(names, types);

    // Row-level resilience is preserved: all three rows are still delivered.
    REQUIRE(rows.size() == 3);
    REQUIRE(rows[0][1].GetValue<int32_t>() == 7);
    REQUIRE(rows[1][1].IsNull());
    REQUIRE(rows[2][1].IsNull());

    // ... but the failures are now visible instead of being indistinguishable
    // from a NULL the server actually sent.
    auto failures = log->Drain();
    REQUIRE(failures.size() == 1);
    REQUIRE(failures[0].column_name == "Quantity");
    REQUIRE(failures[0].failure_count == 2);
    REQUIRE(failures[0].first_offending_value == "\"not-a-number\"");
    REQUIRE_FALSE(failures[0].first_error_message.empty());
}

TEST_CASE("ToRows fails the scan under strict typing", "[conversion_failure]")
{
    const std::string json = R"({
        "value": [ { "Quantity": "not-a-number" } ]
    })";

    ODataEntitySetJsonContent content(json);
    auto log = std::make_shared<ConversionFailureLog>();
    log->SetStrictTyping(true);
    content.SetConversionFailureLog(log);

    std::vector<std::string> names = {"Quantity"};
    std::vector<duckdb::LogicalType> types = {duckdb::LogicalType(duckdb::LogicalTypeId::INTEGER)};

    REQUIRE_THROWS_AS(content.ToRows(names, types), StrictTypingViolation);
}

// ---------------------------------------------------------------------------
// Site 2: a failed list element must not shorten and re-index the list.
// ---------------------------------------------------------------------------

TEST_CASE("Failed list elements become NULL in place", "[conversion_failure]")
{
    const std::string json = R"({
        "value": [ { "Numbers": [1, "bad", 3] } ]
    })";

    ODataEntitySetJsonContent content(json);
    auto log = std::make_shared<ConversionFailureLog>();
    content.SetConversionFailureLog(log);

    std::vector<std::string> names = {"Numbers"};
    std::vector<duckdb::LogicalType> types = {
        duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::INTEGER))};

    auto rows = content.ToRows(names, types);
    REQUIRE(rows.size() == 1);

    auto children = duckdb::ListValue::GetChildren(rows[0][0]);
    // Length and position are preserved: without this, element 3 would be
    // served to the user as element 1.
    REQUIRE(children.size() == 3);
    REQUIRE(children[0].GetValue<int32_t>() == 1);
    REQUIRE(children[1].IsNull());
    REQUIRE(children[2].GetValue<int32_t>() == 3);

    auto failures = log->Drain();
    REQUIRE(failures.size() == 1);
    REQUIRE(failures[0].column_name == "Numbers");
    REQUIRE(failures[0].failure_count == 1);
}

// ---------------------------------------------------------------------------
// Sites 3 and 4: no fabricated scalars, alignment preserved, user told.
// ---------------------------------------------------------------------------

TEST_CASE("Expanded data never fabricates a zero", "[conversion_failure]")
{
    auto client = MakeOfflineEntitySetClient();
    ODataDataExtractor extractor(client);

    auto log = std::make_shared<ConversionFailureLog>();
    extractor.SetConversionFailureLog(log);

    extractor.SetExpandedDataSchema({"Numbers"});
    extractor.UpdateExpandedColumnType(
        0, duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::INTEGER)));

    // Row 0 carries an unconvertible element, row 1 is clean.
    extractor.ExtractExpandedDataFromResponse(R"({
        "value": [
            { "Numbers": [1, "bad", 3] },
            { "Numbers": [4, 5] }
        ]
    })");

    auto row0 = extractor.ExtractExpandedDataForRow("0", "Numbers");
    auto row0_children = duckdb::ListValue::GetChildren(row0);
    REQUIRE(row0_children.size() == 3);
    REQUIRE(row0_children[0].GetValue<int32_t>() == 1);
    // A fabricated 0 here would be indistinguishable from a real 0.
    REQUIRE(row0_children[1].IsNull());
    REQUIRE(row0_children[2].GetValue<int32_t>() == 3);

    // Cache alignment across rows survives the failure: row 1 is still row 1.
    auto row1 = extractor.ExtractExpandedDataForRow("1", "Numbers");
    auto row1_children = duckdb::ListValue::GetChildren(row1);
    REQUIRE(row1_children.size() == 2);
    REQUIRE(row1_children[0].GetValue<int32_t>() == 4);

    auto failures = log->Drain();
    REQUIRE(failures.size() == 1);
    REQUIRE(failures[0].column_name == "Numbers");
    REQUIRE(failures[0].failure_count == 1);
}

TEST_CASE("Out-of-range integers are reported, not zeroed", "[conversion_failure]")
{
    auto client = MakeOfflineEntitySetClient();
    ODataDataExtractor extractor(client);

    auto log = std::make_shared<ConversionFailureLog>();
    extractor.SetConversionFailureLog(log);

    extractor.SetExpandedDataSchema({"Counters"});
    extractor.UpdateExpandedColumnType(
        0, duckdb::LogicalType::LIST(duckdb::LogicalType(duckdb::LogicalTypeId::INTEGER)));

    // 4294967295 does not fit into a signed 32 bit INTEGER.
    extractor.ExtractExpandedDataFromResponse(R"({
        "value": [ { "Counters": [4294967295] } ]
    })");

    auto row0 = extractor.ExtractExpandedDataForRow("0", "Counters");
    auto children = duckdb::ListValue::GetChildren(row0);
    REQUIRE(children.size() == 1);
    REQUIRE(children[0].IsNull());

    auto failures = log->Drain();
    REQUIRE(failures.size() == 1);
    REQUIRE(failures[0].column_name == "Counters");
    REQUIRE(failures[0].first_error_message.find("does not fit") != std::string::npos);
    REQUIRE(failures[0].first_offending_value == "4294967295");
}

TEST_CASE("strict_typing survives the per-scan clone", "[conversion_failure]") {
    // CloneForScan mints a fresh ConversionFailureLog per execution, which is what gives
    // the log the right lifetime - but strict typing lived only inside the log, so the
    // clone silently reset it and the documented strict_typing=true parameter became a
    // no-op for every odata_read() query. Neither #74's nor #75's tests caught it,
    // because no test drove the flag through the table function.
    auto client = MakeOfflineEntitySetClient();
    ODataReadBindData bind_data(client);

    bind_data.SetStrictTyping(true);
    REQUIRE(bind_data.GetConversionFailureLog() != nullptr);
    REQUIRE(bind_data.GetConversionFailureLog()->IsStrictTyping());

    auto clone = bind_data.CloneForScan();
    REQUIRE(clone != nullptr);
    REQUIRE(clone->GetConversionFailureLog() != nullptr);
    REQUIRE(clone->GetConversionFailureLog()->IsStrictTyping());

    // The clone must own a DIFFERENT log, so failures are reported per execution rather
    // than accumulating across re-executions of one bound plan.
    REQUIRE(clone->GetConversionFailureLog() != bind_data.GetConversionFailureLog());
}

TEST_CASE("a non-strict bind data clones as non-strict", "[conversion_failure]") {
    auto client = MakeOfflineEntitySetClient();
    ODataReadBindData bind_data(client);
    bind_data.SetStrictTyping(false);

    auto clone = bind_data.CloneForScan();
    REQUIRE(clone != nullptr);
    REQUIRE(clone->GetConversionFailureLog() != nullptr);
    REQUIRE_FALSE(clone->GetConversionFailureLog()->IsStrictTyping());
}

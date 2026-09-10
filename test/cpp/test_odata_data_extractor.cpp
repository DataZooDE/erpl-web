#include "catch.hpp"
#include "odata_read_functions.hpp"
#include "odata_client.hpp"
#include "odata_edm.hpp"
#include "datazoo/oauth2/http_client.hpp"

#include <cstdint>
#include <string>

using namespace erpl_web;

// Minimal OData V4 EDM: a single entity set "Customers" whose entity type owns a
// navigation property "Orders" of Collection(Test.Order). Exactly one entity set
// is declared so ODataEntitySetClient::GetCurrentEntitySetType() can resolve it
// from metadata without an HTTP round trip.
//
// Order carries one Edm.Int32 and one Edm.Int64 property, which makes the
// expanded column resolve to LIST(STRUCT(id VARCHAR, amount INTEGER,
// total BIGINT)) and lets the tests drive the numeric conversion paths with a
// column type that is fixed by metadata rather than inferred from the payload.
static const std::string EXTRACTOR_EDM_XML = R"XML(<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.String" Nullable="false"/>
        <Property Name="amount" Type="Edm.Int32"/>
        <Property Name="total" Type="Edm.Int64"/>
      </EntityType>
      <EntityType Name="Customer">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Orders" Type="Collection(Test.Order)"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Customers" EntityType="Test.Customer"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>)XML";

// Loopback on a port that is almost certainly closed: any accidental HTTP call
// fails fast with ECONNREFUSED instead of hanging the test binary.
static const std::string EXTRACTOR_URL =
    "http://127.0.0.1:65534/DataExtractorService/Customers";
static const std::string EXTRACTOR_METADATA_URL =
    "http://127.0.0.1:65534/DataExtractorService/$metadata";

static std::shared_ptr<ODataEntitySetClient> MakeExtractorClient() {
    auto edmx = Edmx::FromXml(EXTRACTOR_EDM_XML);
    EdmCache::GetInstance().Set(EXTRACTOR_METADATA_URL, edmx);

    auto http_client = std::make_shared<HttpClient>();
    HttpUrl url(EXTRACTOR_URL);
    auto auth_params = std::make_shared<HttpAuthParams>();
    auto client = std::make_shared<ODataEntitySetClient>(http_client, url, auth_params);
    client->SetODataVersionDirectly(ODataVersion::V4);
    return client;
}

// One OData V4 row whose "Orders" collection holds `order_count` entries.
static std::string BuildExpandedPayload(size_t order_count,
                                        const std::string &amount_literal,
                                        const std::string &total_literal) {
    std::string json = R"({"value":[{"id":"c1","Orders":[)";
    for (size_t i = 0; i < order_count; ++i) {
        if (i > 0) {
            json += ",";
        }
        json += R"({"id":"o)" + std::to_string(i) + R"(","amount":)" + amount_literal +
                R"(,"total":)" + total_literal + "}";
    }
    json += "]}]}";
    return json;
}

// ============================================================================
// Expanded collections must be returned in full (GitHub #81)
// ============================================================================

TEST_CASE("ODataDataExtractor - resolves the expanded column type from metadata") {
    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});

    auto types = extractor.GetExpandedDataTypes();
    REQUIRE(types.size() == 1);
    REQUIRE(types[0].id() == duckdb::LogicalTypeId::LIST);

    auto child_type = duckdb::ListType::GetChildType(types[0]);
    REQUIRE(child_type.id() == duckdb::LogicalTypeId::STRUCT);

    auto &fields = duckdb::StructType::GetChildTypes(child_type);
    REQUIRE(fields.size() == 3);
    REQUIRE(fields[1].first == "amount");
    REQUIRE(fields[1].second.id() == duckdb::LogicalTypeId::INTEGER);
    REQUIRE(fields[2].first == "total");
    REQUIRE(fields[2].second.id() == duckdb::LogicalTypeId::BIGINT);
}

TEST_CASE("ODataDataExtractor - keeps every element of a large expanded collection") {
    // 1500 > the former hard-coded 1000 element cap.
    static constexpr size_t ORDER_COUNT = 1500;

    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(BuildExpandedPayload(ORDER_COUNT, "7", "8"));

    auto orders = extractor.ExtractExpandedDataForRow("0", "Orders");
    REQUIRE_FALSE(orders.IsNull());
    REQUIRE(orders.type().id() == duckdb::LogicalTypeId::LIST);

    auto &children = duckdb::ListValue::GetChildren(orders);
    REQUIRE(children.size() == ORDER_COUNT);

    // The tail must be intact, not just the count: check the very last element.
    auto &last = duckdb::StructValue::GetChildren(children[ORDER_COUNT - 1]);
    REQUIRE(last[0].ToString() == "o" + std::to_string(ORDER_COUNT - 1));
}

TEST_CASE("ODataDataExtractor - collections just under the former cap are unchanged") {
    static constexpr size_t ORDER_COUNT = 999;

    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(BuildExpandedPayload(ORDER_COUNT, "7", "8"));

    auto orders = extractor.ExtractExpandedDataForRow("0", "Orders");
    REQUIRE(duckdb::ListValue::GetChildren(orders).size() == ORDER_COUNT);
}

// ============================================================================
// Out-of-range numbers must fail loudly instead of wrapping (GitHub #81)
// ============================================================================

TEST_CASE("ODataDataExtractor - rejects a value that does not fit an INTEGER column") {
    // 4000000000 > INT32_MAX. The previous implementation handed it to
    // yyjson_get_int(), which truncates to 32 bits and yielded -294967296 as if
    // it were the real value.
    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(BuildExpandedPayload(1, "4000000000", "8"));

    auto orders = extractor.ExtractExpandedDataForRow("0", "Orders");
    REQUIRE(orders.IsNull());
    REQUIRE(extractor.GetLastError().find("4000000000") != std::string::npos);
}

TEST_CASE("ODataDataExtractor - rejects a negative value below the INTEGER range") {
    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(BuildExpandedPayload(1, "-4000000000", "8"));

    auto orders = extractor.ExtractExpandedDataForRow("0", "Orders");
    REQUIRE(orders.IsNull());
    REQUIRE(extractor.GetLastError().find("-4000000000") != std::string::npos);
}

TEST_CASE("ODataDataExtractor - an out-of-range value does not shift later rows") {
    // Row 0 carries an unrepresentable amount, row 1 a valid one. Row 1 must
    // still resolve to its own orders rather than inheriting row 0's slot.
    const std::string json =
        R"({"value":[)"
        R"({"id":"c1","Orders":[{"id":"bad","amount":4000000000,"total":1}]},)"
        R"({"id":"c2","Orders":[{"id":"good","amount":42,"total":2}]}]})";

    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(json);

    REQUIRE(extractor.ExtractExpandedDataForRow("0", "Orders").IsNull());

    auto second = extractor.ExtractExpandedDataForRow("1", "Orders");
    REQUIRE_FALSE(second.IsNull());
    auto &children = duckdb::ListValue::GetChildren(second);
    REQUIRE(children.size() == 1);
    REQUIRE(duckdb::StructValue::GetChildren(children[0])[1].GetValue<int32_t>() == 42);
}

TEST_CASE("ODataDataExtractor - accepts the INTEGER boundary values") {
    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(BuildExpandedPayload(1, "2147483647", "8"));

    auto orders = extractor.ExtractExpandedDataForRow("0", "Orders");
    auto &children = duckdb::ListValue::GetChildren(orders);
    REQUIRE(children.size() == 1);

    auto &fields = duckdb::StructValue::GetChildren(children[0]);
    REQUIRE(fields[1].GetValue<int32_t>() == 2147483647);
}

TEST_CASE("ODataDataExtractor - keeps the full 64-bit width of a BIGINT column") {
    // 9007199254740993 does not survive a 32-bit accessor: the previous
    // implementation read it with yyjson_get_int() and produced 1.
    ODataDataExtractor extractor(MakeExtractorClient());
    extractor.SetExpandedDataSchema({"Orders"});
    extractor.ExtractExpandedDataFromResponse(
        BuildExpandedPayload(1, "7", "9007199254740993"));

    auto orders = extractor.ExtractExpandedDataForRow("0", "Orders");
    auto &children = duckdb::ListValue::GetChildren(orders);
    REQUIRE(children.size() == 1);

    auto &fields = duckdb::StructValue::GetChildren(children[0]);
    REQUIRE(fields[2].GetValue<int64_t>() == INT64_C(9007199254740993));
}

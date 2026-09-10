#include "catch.hpp"

#include "odata_predicate_pushdown_helper.hpp"
#include "odata_read_functions.hpp"

#include <string>
#include <vector>

using namespace erpl_web;

namespace {

duckdb::LogicalType Varchar() {
    return duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
}

} // namespace

// ---------------------------------------------------------------------------
// GitHub #86 - $select pushdown must key off the column's LogicalType, not off
// a hard-coded list of TripPin demo property names matched by prefix.
// ---------------------------------------------------------------------------

TEST_CASE("Scalar column named Features still gets $select pushdown", "[odata][pushdown]")
{
    // A real service with scalar columns whose names happen to collide with the
    // old hard-coded TripPin list. Before the fix, "Features" (exact match) and
    // "HomeAddressLine1" (prefix match on "HomeAddress") each disabled $select
    // entirely, so the scan transferred every column.
    std::vector<std::string> names = {"ID", "Features", "HomeAddressLine1", "Notes"};
    std::vector<duckdb::LogicalType> types = {Varchar(), Varchar(), Varchar(), Varchar()};

    ODataPredicatePushdownHelper helper(names, types);
    helper.ConsumeColumnSelection({0, 1, 2});

    REQUIRE(helper.SelectClause() == "$select=ID,Features,HomeAddressLine1");
}

TEST_CASE("List-typed column disables $select pushdown", "[odata][pushdown]")
{
    std::vector<std::string> names = {"UserName", "Emails", "FirstName"};
    std::vector<duckdb::LogicalType> types = {
        Varchar(), duckdb::LogicalType::LIST(Varchar()), Varchar()};

    ODataPredicatePushdownHelper helper(names, types);
    helper.ConsumeColumnSelection({0, 1});

    REQUIRE(helper.SelectClause().empty());
}

TEST_CASE("Struct-typed column disables $select pushdown", "[odata][pushdown]")
{
    duckdb::child_list_t<duckdb::LogicalType> children;
    children.emplace_back("City", Varchar());

    std::vector<std::string> names = {"UserName", "HomeAddress", "FirstName"};
    std::vector<duckdb::LogicalType> types = {
        Varchar(), duckdb::LogicalType::STRUCT(children), Varchar()};

    ODataPredicatePushdownHelper helper(names, types);
    helper.ConsumeColumnSelection({0, 1});

    REQUIRE(helper.SelectClause().empty());
}

TEST_CASE("Nested column not projected does not disable $select", "[odata][pushdown]")
{
    std::vector<std::string> names = {"UserName", "Emails", "FirstName"};
    std::vector<duckdb::LogicalType> types = {
        Varchar(), duckdb::LogicalType::LIST(Varchar()), Varchar()};

    ODataPredicatePushdownHelper helper(names, types);
    helper.ConsumeColumnSelection({0, 2});

    REQUIRE(helper.SelectClause() == "$select=UserName,FirstName");
}

TEST_CASE("Without column types $select is still built", "[odata][pushdown]")
{
    // Callers that never supply types (unit tests, legacy call sites) keep the
    // pre-existing behaviour: no type information means no nested-column guard.
    std::vector<std::string> names = {"ID", "Features", "Notes"};

    ODataPredicatePushdownHelper helper(names);
    helper.ConsumeColumnSelection({0, 1});

    REQUIRE(helper.SelectClause() == "$select=ID,Features");
}

// ---------------------------------------------------------------------------
// GitHub #88 - the schema order handed to the scan must follow EDMX/metadata,
// because that is the order the DuckDB catalog declares its columns in. JSON
// key order may only contribute names metadata does not know about.
// ---------------------------------------------------------------------------

TEST_CASE("Metadata order wins over JSON key order", "[odata][schema]")
{
    const std::vector<std::string> metadata_names = {"CategoryID", "CategoryName",
                                                     "Description", "Picture"};
    // A V2 payload that serialises its properties in a different order.
    const std::vector<std::string> json_names = {"CategoryName", "Picture",
                                                 "CategoryID", "Description"};

    const auto reconciled =
        ODataReadBindData::ReconcileSchemaOrder(metadata_names, json_names);

    REQUIRE(reconciled == metadata_names);
}

TEST_CASE("Columns absent from the payload keep their metadata slot", "[odata][schema]")
{
    const std::vector<std::string> metadata_names = {"A", "B", "C"};
    const std::vector<std::string> json_names = {"C", "A"};

    const auto reconciled =
        ODataReadBindData::ReconcileSchemaOrder(metadata_names, json_names);

    REQUIRE(reconciled == metadata_names);
}

TEST_CASE("Payload-only columns are appended after the metadata columns", "[odata][schema]")
{
    const std::vector<std::string> metadata_names = {"A", "B"};
    const std::vector<std::string> json_names = {"Extra1", "B", "Extra2", "A"};

    const auto reconciled =
        ODataReadBindData::ReconcileSchemaOrder(metadata_names, json_names);

    const std::vector<std::string> expected = {"A", "B", "Extra1", "Extra2"};
    REQUIRE(reconciled == expected);
}

TEST_CASE("Without metadata the JSON order is used unchanged", "[odata][schema]")
{
    const std::vector<std::string> json_names = {"X", "Y", "Z"};

    const auto reconciled =
        ODataReadBindData::ReconcileSchemaOrder({}, json_names);

    REQUIRE(reconciled == json_names);
}

// ---------------------------------------------------------------------------
// GitHub #75 - the row buffer is per-scan state. A clone of a scan must be able
// to take a copy of the rows buffered during bind without draining the source.
// ---------------------------------------------------------------------------

TEST_CASE("CopyRows snapshots the buffer without consuming it", "[odata][scan_state]")
{
    ODataRowBuffer buffer;
    buffer.AddRows({{duckdb::Value("a")}, {duckdb::Value("b")}});
    buffer.SetHasNextPage(true);

    const auto snapshot = buffer.CopyRows();

    REQUIRE(snapshot.size() == 2);
    REQUIRE(buffer.Size() == 2);
    REQUIRE(buffer.HasMoreRows());

    ODataRowBuffer clone;
    clone.AddRows(snapshot);
    clone.SetHasNextPage(buffer.HasNextPage());

    REQUIRE(clone.Size() == 2);
    REQUIRE(clone.HasNextPage());

    // Draining the clone must leave the source untouched.
    clone.GetNextRow();
    clone.GetNextRow();
    REQUIRE_FALSE(clone.HasMoreRows());
    REQUIRE(buffer.Size() == 2);
}

#include "catch.hpp"
#include "odata_predicate_pushdown_helper.hpp"
#include "odata_url_helpers.hpp"
#include "datazoo/oauth2/http_client.hpp"

#include <optional>
#include <sstream>
#include <string>

using namespace erpl_web;

namespace {

// These tests used to compare `result_url.ToString()` against a full, exact URL. That made
// them break whenever an unrelated but correct change touched the query string (adding the
// mandatory `$format=json`, or reordering parameters - `ApplyFiltersToUrl` rebuilds the query
// from a std::map, so parameters come out in sorted key order). Look a single parameter up by
// name instead, so a test only depends on the thing it is actually about.
std::optional<std::string> RawQueryParam(const HttpUrl &url, const std::string &name)
{
    std::string query = url.Query();
    if (!query.empty() && query.front() == '?') {
        query = query.substr(1);
    }

    std::istringstream stream(query);
    std::string pair;
    while (std::getline(stream, pair, '&')) {
        const auto separator = pair.find('=');
        if (separator == std::string::npos) {
            continue;
        }
        if (pair.substr(0, separator) == name) {
            return pair.substr(separator + 1);
        }
    }
    return std::nullopt;
}

// Percent-decoded value of a query parameter, or an empty string when the parameter is absent.
// Decoding is what lets a test state the expand it asked for, rather than restating the exact
// encoding the sanitizer happens to emit.
std::string QueryParam(const HttpUrl &url, const std::string &name)
{
    const auto raw = RawQueryParam(url, name);
    return raw.has_value() ? ODataUrlCodec::decodeQueryValue(*raw) : std::string();
}

bool HasQueryParam(const HttpUrl &url, const std::string &name)
{
    return RawQueryParam(url, name).has_value();
}

} // namespace

TEST_CASE("OData Predicate Pushdown Helper - Expand Basic Functionality", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Consume expand clause") {
        helper.ConsumeExpand("Category,Orders");

        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }

    SECTION("Consume empty expand clause") {
        helper.ConsumeExpand("");

        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause.empty());
    }

    SECTION("Consume expand clause with whitespace") {
        helper.ConsumeExpand(" Category , Orders ");

        // Regression test for GitHub #115: whitespace around navigation-property segments is
        // trimmed. Strict services (SAP Gateway) treat a leading space as part of the property
        // name and reject "$expand= Category , Orders ".
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }

    SECTION("Consume expand clause multiple times") {
        helper.ConsumeExpand("Category");
        helper.ConsumeExpand("Orders");

        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Orders"); // Last one wins
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand URL Construction", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Apply expand to URL") {
        helper.ConsumeExpand("Category,Orders");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(result_url.Path() == "/service/Customers");
        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
        // Every generated OData request URL carries $format=json; the reader always wants the
        // JSON representation, never the service's default (often XML/Atom for V2).
        REQUIRE(QueryParam(result_url, "$format") == "json");
    }

    SECTION("Apply expand with existing query parameters") {
        helper.ConsumeExpand("Category,Orders");

        HttpUrl base_url("http://host/service/Customers?$select=ID,Name");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        // A $select already present in the base URL is preserved, and the expand is added
        // alongside it.
        REQUIRE(QueryParam(result_url, "$select") == "ID,Name");
        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
    }

    SECTION("Apply expand with other clauses") {
        helper.ConsumeExpand("Category,Orders");
        helper.ConsumeLimit(10);
        helper.ConsumeOffset(20);

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$top") == "10");
        REQUIRE(QueryParam(result_url, "$skip") == "20");
        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
    }

    SECTION("Apply expand with complex syntax") {
        helper.ConsumeExpand("Products($filter=DiscontinuedDate eq null),Category($select=Name)");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand")
                == "Products($filter=DiscontinuedDate eq null),Category($select=Name)");
        // $filter values nested inside $expand options are percent-encoded on the wire; strict
        // services (SAP Gateway) reject the raw form with unescaped spaces. Only the option
        // value is encoded - the expand structure itself stays readable.
        REQUIRE(*RawQueryParam(result_url, "$expand")
                == "Products($filter=DiscontinuedDate%20eq%20null),Category($select=Name)");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand with OData Versions", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Expand clause with OData V2") {
        helper.SetODataVersion(ODataVersion::V2);
        helper.ConsumeExpand("Category,Orders");

        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }

    SECTION("Expand clause with OData V4") {
        helper.SetODataVersion(ODataVersion::V4);
        helper.ConsumeExpand("Category,Orders");

        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Category,Orders");
    }

    SECTION("Expand clause with OData V2 and inline count") {
        helper.SetODataVersion(ODataVersion::V2);
        helper.EnableInlineCount(true);
        helper.ConsumeExpand("Category,Orders");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        // V2 spells inline count "$inlinecount=allpages" (V4 uses "$count=true"); the expand
        // must survive next to it.
        REQUIRE(QueryParam(result_url, "$inlinecount") == "allpages");
        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Complex Scenarios", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Expand with nested paths") {
        helper.ConsumeExpand("Category/Products/Supplier");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand") == "Category/Products/Supplier");
    }

    SECTION("Expand with complex query options") {
        helper.ConsumeExpand("Products($filter=Price gt 100;$select=Name,Price;$top=5;$skip=10)");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand")
                == "Products($filter=Price gt 100;$select=Name,Price;$top=5;$skip=10)");
        // Nested $filter values are percent-encoded ("Price%20gt%20100"), the sibling options
        // are not - they are already URL-safe and stay legible.
        REQUIRE(*RawQueryParam(result_url, "$expand")
                == "Products($filter=Price%20gt%20100;$select=Name,Price;$top=5;$skip=10)");
    }

    SECTION("Expand with multiple complex paths") {
        helper.ConsumeExpand("Category($select=Name)/Products($filter=DiscontinuedDate eq null),Orders($top=10)");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand")
                == "Category($select=Name)/Products($filter=DiscontinuedDate eq null),Orders($top=10)");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Integration with Other Clauses", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name", "CategoryID", "Address", "City"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Expand with select clause") {
        helper.ConsumeColumnSelection({1, 2}); // Name, CategoryID
        helper.ConsumeExpand("Category,Orders");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$select") == "Name,CategoryID");
        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
    }

    SECTION("Expand with filter clause") {
        helper.ConsumeExpand("Category,Orders");
        // No filters were consumed, so no $filter must appear - an expand on its own never
        // synthesises one.

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
        REQUIRE_FALSE(HasQueryParam(result_url, "$filter"));
    }

    SECTION("Expand with all clause types") {
        helper.ConsumeColumnSelection({0, 1}); // ID, Name
        helper.ConsumeExpand("Category,Orders");
        helper.ConsumeLimit(25);
        helper.ConsumeOffset(50);

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$select") == "ID,Name");
        REQUIRE(QueryParam(result_url, "$top") == "25");
        REQUIRE(QueryParam(result_url, "$skip") == "50");
        REQUIRE(QueryParam(result_url, "$expand") == "Category,Orders");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Edge Cases", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Expand with special characters in filter") {
        helper.ConsumeExpand("Products($filter=Name eq 'Product;Name')");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        // The semicolon inside the string literal round-trips: the option splitter treats it as
        // an option separator, so only the first half of the literal gets percent-encoded, but
        // decoding the parameter yields the expand exactly as it was asked for.
        REQUIRE(QueryParam(result_url, "$expand") == "Products($filter=Name eq 'Product;Name')");
    }

    SECTION("Expand with nested parentheses") {
        helper.ConsumeExpand("Products($filter=(Price gt 100) and (CategoryID eq 1))");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand") == "Products($filter=(Price gt 100) and (CategoryID eq 1))");
        // The whole nested filter expression is encoded, parentheses included, so the trailing
        // ')' of the option section is never ambiguous for the service.
        REQUIRE(*RawQueryParam(result_url, "$expand")
                == "Products($filter=%28Price%20gt%20100%29%20and%20%28CategoryID%20eq%201%29)");
    }

    SECTION("Expand with function calls") {
        helper.ConsumeExpand("Products($filter=startswith(Name,'A') eq true)");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        // The comma inside the function call is not mistaken for an option separator.
        REQUIRE(QueryParam(result_url, "$expand") == "Products($filter=startswith(Name,'A') eq true)");
    }

    SECTION("Expand with empty parentheses") {
        helper.ConsumeExpand("Products()");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand") == "Products()");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Real-world Examples", "[odata_expand]") {
    std::vector<std::string> column_names = {"CustomerID", "CompanyName", "ContactName"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("SAP Datasphere example") {
        helper.ConsumeExpand("DefaultSystem($expand=Services())");

        HttpUrl base_url("http://localhost:50000/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(result_url.Path() == "/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups");
        REQUIRE(QueryParam(result_url, "$expand") == "DefaultSystem($expand=Services())");
    }

    SECTION("Northwind example") {
        helper.ConsumeExpand("Orders($filter=Freight gt 100;$select=OrderID,Freight)");

        HttpUrl base_url("http://host/service/Customers");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand") == "Orders($filter=Freight gt 100;$select=OrderID,Freight)");
    }

    SECTION("Complex business scenario") {
        helper.ConsumeExpand("Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)/OrderDetails($select=ProductID,Quantity,UnitPrice)");

        HttpUrl base_url("http://host/service/Invoices");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand")
                == "Customer($select=CustomerID,CompanyName)/Orders($filter=OrderDate gt 2023-01-01;$top=10)"
                   "/OrderDetails($select=ProductID,Quantity,UnitPrice)");
    }
}

TEST_CASE("OData Predicate Pushdown Helper - Expand Performance and Robustness", "[odata_expand]") {
    std::vector<std::string> column_names = {"ID", "Name"};
    ODataPredicatePushdownHelper helper(column_names);

    SECTION("Large expand clause") {
        std::string large_expand = "Path1,Path2,Path3,Path4,Path5,Path6,Path7,Path8,Path9,Path10";
        helper.ConsumeExpand(large_expand);

        HttpUrl base_url("http://host/service/Entity");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        REQUIRE(QueryParam(result_url, "$expand") == large_expand);
    }

    SECTION("Very long filter in expand") {
        std::string long_filter = std::string(1000, 'a') + " eq 'test'";
        std::string expand_with_long_filter = "Products($filter=" + long_filter + ")";
        helper.ConsumeExpand(expand_with_long_filter);

        HttpUrl base_url("http://host/service/Entity");
        auto result_url = helper.ApplyFiltersToUrl(base_url);

        // The nested $filter is percent-encoded, so the raw parameter is longer than the input,
        // but it decodes back to exactly what was requested - nothing is truncated.
        REQUIRE(QueryParam(result_url, "$expand") == expand_with_long_filter);
        REQUIRE(RawQueryParam(result_url, "$expand")->size() > expand_with_long_filter.size());
    }

    SECTION("Multiple expand operations") {
        for (int i = 0; i < 100; ++i) {
            helper.ConsumeExpand("Path" + std::to_string(i));
        }

        // Last one should be the final result
        auto expand_clause = helper.ExpandClause();
        REQUIRE(expand_clause == "$expand=Path99");
    }
}

#pragma once

#include <string>
#include <vector>

namespace erpl_web {

/**
 * OData Query Builder and Value Encoder
 *
 * Provides safe construction of OData query strings with proper encoding of values.
 * Prevents OData injection attacks by encoding special characters in filter values.
 *
 * Security: Encodes single quotes and backslashes in OData filter values
 * Pattern: OData v4 string literal encoding (ISO/IEC 20802-1:2016)
 *
 * Example:
 *   auto builder = ODataQueryBuilder();
 *   builder.AddFilter("spaceName", "my'space"); // Encodes single quote
 *   // Result: $filter=spaceName eq 'my''space'
 */
class ODataQueryBuilder {
public:
    /**
     * Create a new OData query builder
     */
    ODataQueryBuilder() = default;

    /**
     * Add a filter condition to the query
     * Encodes the value to prevent OData injection
     *
     * @param field_name The field to filter (e.g., "spaceName", "assetId")
     * @param value The value to match (will be encoded)
     * @param operator_str The comparison operator (default: "eq")
     * @return Reference to this builder (for chaining)
     *
     * Example:
     *   builder.AddFilter("spaceName", "test space")
     *         .AddFilter("status", "ACTIVE");
     */
    ODataQueryBuilder& AddFilter(
        const std::string& field_name,
        const std::string& value,
        const std::string& operator_str = "eq");

    /**
     * Add a $select clause to limit returned columns
     *
     * @param fields Vector of field names to select
     * @return Reference to this builder (for chaining)
     *
     * Example:
     *   builder.AddSelect({"id", "name", "description"});
     */
    ODataQueryBuilder& AddSelect(const std::vector<std::string>& fields);

    /**
     * Add a $top clause to limit number of results
     *
     * @param count Maximum number of results to return
     * @return Reference to this builder (for chaining)
     */
    ODataQueryBuilder& AddTop(size_t count);

    /**
     * Add a $skip clause to skip initial results
     *
     * @param count Number of results to skip
     * @return Reference to this builder (for chaining)
     */
    ODataQueryBuilder& AddSkip(size_t count);

    /**
     * Add an $orderby clause for sorting
     *
     * @param field_name The field to sort by
     * @param descending If true, use descending order (default: ascending)
     * @return Reference to this builder (for chaining)
     */
    ODataQueryBuilder& AddOrderBy(
        const std::string& field_name,
        bool descending = false);

    /**
     * Build the complete query string
     *
     * @return Query string (e.g., "$filter=...&$select=...&$top=10")
     *         Returns empty string if no clauses added
     *
     * Example:
     *   auto query = builder.AddFilter("status", "ACTIVE")
     *                        .AddTop(50)
     *                        .Build();
     *   // Result: "$filter=status eq 'ACTIVE'&$top=50"
     */
    std::string Build() const;

    /**
     * Reset the builder to initial state
     * Clears all filters, select, top, skip, and orderby clauses
     */
    void Clear();

private:
    std::vector<std::string> filters_;
    std::vector<std::string> select_fields_;
    size_t top_ = 0;
    size_t skip_ = 0;
    std::string order_by_;
};

/**
 * OData Value Encoder
 *
 * Encodes values for safe use in OData filter expressions.
 * Follows OData v4 string literal encoding standard.
 *
 * Security: Prevents OData injection by escaping special characters
 * Pattern: Single quotes doubled (''), backslashes doubled (\\)
 *
 * Example:
 *   auto encoded = ODataValueEncoder::Encode("test'value");
 *   // Result: "test''value"
 */
class ODataValueEncoder {
public:
    /**
     * Encode a value for safe use in OData filter expressions
     *
     * Encodes:
     * - Single quotes (') → '' (doubled)
     * - Backslashes (\) → \\ (doubled)
     *
     * @param value The raw value to encode
     * @return Encoded value safe for use in OData filters
     *
     * Examples:
     *   Encode("simple") → "simple"
     *   Encode("test'value") → "test''value"
     *   Encode("path\\to\\file") → "path\\\\to\\\\file"
     *   Encode("O'Brien") → "O''Brien"
     *   Encode("'; DROP TABLE--") → "''; DROP TABLE--"  (now safe!)
     */
    static std::string Encode(const std::string& value);

    /**
     * Check if a value needs encoding
     *
     * @param value The value to check
     * @return true if value contains special characters needing encoding
     */
    static bool NeedsEncoding(const std::string& value);
};

} // namespace erpl_web

#include "odata_query_builder.hpp"
#include <algorithm>

namespace erpl_web {

// ===== ODataValueEncoder =====

std::string ODataValueEncoder::Encode(const std::string& value) {
    std::string result;
    result.reserve(value.size() * 1.2);  // Pre-allocate with some buffer

    for (char ch : value) {
        if (ch == '\'') {
            // Single quote → doubled single quote
            result += "''";
        } else if (ch == '\\') {
            // Backslash → doubled backslash
            result += "\\\\";
        } else {
            result += ch;
        }
    }

    return result;
}

bool ODataValueEncoder::NeedsEncoding(const std::string& value) {
    return std::any_of(value.begin(), value.end(), [](char ch) {
        return ch == '\'' || ch == '\\';
    });
}

// ===== ODataQueryBuilder =====

ODataQueryBuilder& ODataQueryBuilder::AddFilter(
    const std::string& field_name,
    const std::string& value,
    const std::string& operator_str) {

    // Optimize: Pre-allocate to reduce temporary string allocations
    std::string filter;
    filter.reserve(field_name.size() + operator_str.size() + value.size() * 1.3 + 5);
    filter.append(field_name);
    filter.append(" ");
    filter.append(operator_str);
    filter.append(" '");

    // Inline encoding to avoid temporary encoded_value string
    for (char ch : value) {
        if (ch == '\'') {
            filter.append("''");
        } else if (ch == '\\') {
            filter.append("\\\\");
        } else {
            filter.push_back(ch);
        }
    }

    filter.append("'");
    filters_.push_back(std::move(filter));
    return *this;
}

ODataQueryBuilder& ODataQueryBuilder::AddSelect(const std::vector<std::string>& fields) {
    select_fields_ = fields;
    return *this;
}

ODataQueryBuilder& ODataQueryBuilder::AddTop(size_t count) {
    top_ = count;
    return *this;
}

ODataQueryBuilder& ODataQueryBuilder::AddSkip(size_t count) {
    skip_ = count;
    return *this;
}

ODataQueryBuilder& ODataQueryBuilder::AddOrderBy(
    const std::string& field_name,
    bool descending) {

    order_by_ = field_name;
    if (descending) {
        order_by_ += " desc";
    }
    return *this;
}

std::string ODataQueryBuilder::Build() const {
    // Estimate final size to avoid reallocations
    size_t estimated_size = 0;
    if (!filters_.empty()) {
        estimated_size += 8;  // "$filter="
        for (const auto& f : filters_) {
            estimated_size += f.size() + 5;  // filter + " and "
        }
    }
    if (!select_fields_.empty()) {
        estimated_size += 8;  // "$select="
        for (const auto& f : select_fields_) {
            estimated_size += f.size() + 1;  // field + ","
        }
        if (!filters_.empty()) estimated_size += 1;  // "&"
    }
    if (top_ > 0) {
        estimated_size += 20;  // "$top=" + number
        if (!filters_.empty() || !select_fields_.empty()) estimated_size += 1;  // "&"
    }
    if (skip_ > 0) {
        estimated_size += 20;  // "$skip=" + number
        if (!filters_.empty() || !select_fields_.empty() || top_ > 0) estimated_size += 1;  // "&"
    }
    if (!order_by_.empty()) {
        estimated_size += order_by_.size() + 9;  // "$orderby="
        if (!filters_.empty() || !select_fields_.empty() || top_ > 0 || skip_ > 0) estimated_size += 1;  // "&"
    }

    std::string result;
    result.reserve(estimated_size);
    bool first = true;

    // Add $filter clause
    if (!filters_.empty()) {
        result.append("$filter=");
        for (size_t i = 0; i < filters_.size(); ++i) {
            if (i > 0) {
                result.append(" and ");
            }
            result.append(filters_[i]);
        }
        first = false;
    }

    // Add $select clause
    if (!select_fields_.empty()) {
        if (!first) {
            result.append("&");
        }
        result.append("$select=");
        for (size_t i = 0; i < select_fields_.size(); ++i) {
            if (i > 0) {
                result.append(",");
            }
            result.append(select_fields_[i]);
        }
        first = false;
    }

    // Add $top clause
    if (top_ > 0) {
        if (!first) {
            result.append("&");
        }
        result.append("$top=");
        result.append(std::to_string(top_));
        first = false;
    }

    // Add $skip clause
    if (skip_ > 0) {
        if (!first) {
            result.append("&");
        }
        result.append("$skip=");
        result.append(std::to_string(skip_));
        first = false;
    }

    // Add $orderby clause
    if (!order_by_.empty()) {
        if (!first) {
            result.append("&");
        }
        result.append("$orderby=");
        result.append(order_by_);
    }

    return result;
}

void ODataQueryBuilder::Clear() {
    filters_.clear();
    select_fields_.clear();
    top_ = 0;
    skip_ = 0;
    order_by_.clear();
}

} // namespace erpl_web

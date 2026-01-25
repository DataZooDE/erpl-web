#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace erpl_web {

// Microsoft Graph Outlook table functions
class GraphOutlookFunctions {
public:
    static void Register(duckdb::ExtensionLoader &loader);

private:
    // graph_calendar_events(secret_name) - List calendar events
    static duckdb::unique_ptr<duckdb::FunctionData> CalendarEventsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void CalendarEventsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_contacts(secret_name) - List contacts
    static duckdb::unique_ptr<duckdb::FunctionData> ContactsBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void ContactsScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);

    // graph_messages(secret_name) - List email messages (metadata only)
    static duckdb::unique_ptr<duckdb::FunctionData> MessagesBind(
        duckdb::ClientContext &context,
        duckdb::TableFunctionBindInput &input,
        duckdb::vector<duckdb::LogicalType> &return_types,
        duckdb::vector<std::string> &names);

    static void MessagesScan(
        duckdb::ClientContext &context,
        duckdb::TableFunctionInput &data,
        duckdb::DataChunk &output);
};

} // namespace erpl_web

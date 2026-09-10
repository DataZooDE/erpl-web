#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>

namespace odp_test {

// ODP delta state now refuses to live in an in-memory catalog -- tokens stored
// there vanish at exit, which turns every delta read into a silent full
// extraction (GitHub #92). Every ODP state test therefore needs a real database
// file; this owns one for the duration of a test and deletes it afterwards.
class TempDatabase {
public:
    explicit TempDatabase(const std::string& name_hint = "odp_state") {
        path = MakeUniquePath(name_hint);
        duckdb::DBConfig config;
        config.SetOption("allocator_background_threads", duckdb::Value::BOOLEAN(true));
        database = duckdb::make_uniq<duckdb::DuckDB>(path.string(), &config);
        connection = duckdb::make_uniq<duckdb::Connection>(*database);
    }

    ~TempDatabase() {
        connection.reset();
        database.reset();
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(path.string() + ".wal", ignored);
    }

    TempDatabase(const TempDatabase&) = delete;
    TempDatabase& operator=(const TempDatabase&) = delete;

    duckdb::Connection& Conn() { return *connection; }
    duckdb::ClientContext& Context() { return *connection->context; }
    const std::filesystem::path& Path() const { return path; }

    // A second, independent connection -- two connections stand in for two
    // sessions racing on the same ODP subscription.
    duckdb::unique_ptr<duckdb::Connection> NewConnection() {
        return duckdb::make_uniq<duckdb::Connection>(*database);
    }

    static std::filesystem::path MakeUniquePath(const std::string& name_hint) {
        static std::atomic<uint64_t> counter {0};
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto file = name_hint + "_" + std::to_string(stamp) + "_" + std::to_string(counter++) + ".db";
        return std::filesystem::temp_directory_path() / file;
    }

private:
    std::filesystem::path path;
    duckdb::unique_ptr<duckdb::DuckDB> database;
    duckdb::unique_ptr<duckdb::Connection> connection;
};

} // namespace odp_test

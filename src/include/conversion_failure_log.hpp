#pragma once

#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace erpl_web {

// A value the server sent cannot be represented in the column's declared type
// because it lies outside that type's range. Distinct from a type mismatch:
// the JSON type was right, the magnitude was not.
class NumericRangeError : public std::out_of_range {
public:
    explicit NumericRangeError(const std::string &message) : std::out_of_range(message) { }
};

// Raised by ConversionFailureLog::RecordFailure when strict typing is enabled.
// Callers that swallow std::exception to keep a scan alive must catch this type
// first and rethrow it, otherwise strict typing silently degrades to lenient.
class StrictTypingViolation : public std::runtime_error {
public:
    explicit StrictTypingViolation(const std::string &message) : std::runtime_error(message) { }
};

// One column's worth of aggregated conversion failures.
struct ConversionFailure {
    std::string column_name;
    uint64_t failure_count = 0;
    std::string first_offending_value;
    std::string first_error_message;
};

// Collects per-column value-conversion failures over the lifetime of a single
// scan and surfaces them once, instead of turning each failure into an
// indistinguishable NULL.
//
// Ownership: one log per scan, held by the bind data. It deliberately outlives
// the individual page responses and the row buffer, both of which are replaced
// during pagination, so that the summary covers the whole scan.
//
// Thread safety: DuckDB may drive a scan from several threads, so every
// mutating operation takes the mutex.
class ConversionFailureLog {
public:
    // Longest offending value kept for the report; longer values are truncated.
    static constexpr size_t MAX_OFFENDING_VALUE_LENGTH = 120;

    ConversionFailureLog() = default;

    // When enabled, RecordFailure throws StrictTypingViolation instead of
    // recording, turning a silently-NULLed cell into a failed query.
    void SetStrictTyping(bool strict);
    bool IsStrictTyping() const;

    // Record one failed conversion. The first offending value and error message
    // per column are kept; later ones only bump the count.
    void RecordFailure(const std::string &column_name,
                       const std::string &offending_value,
                       const std::string &error_message);

    bool HasFailures() const;
    uint64_t TotalFailureCount() const;

    // Return the accumulated failures and clear the log.
    std::vector<ConversionFailure> Drain();

    // Human-readable summary of the given failures, or an empty string when
    // there are none.
    static std::string FormatSummary(const std::vector<ConversionFailure> &failures,
                                     const std::string &source);

    // Drain the log and, if anything was recorded, emit the summary once as a
    // warning on stderr and into the trace. Safe to call when empty.
    void ReportAndDrain(const std::string &source);

private:
    static std::string Truncate(const std::string &value);

    mutable std::mutex mutex_;
    std::vector<ConversionFailure> failures_;
    std::unordered_map<std::string, size_t> column_index_;
    bool strict_typing_ = false;
};

} // namespace erpl_web

#pragma once

#include <string>
#include <memory>
#include <fstream>
#include <mutex>

namespace erpl_web {

enum class TraceLevel {
    NONE = 0,
    ERROR = 1,
    WARN = 2,
    INFO = 3,
    DEBUG_LEVEL = 4,
    TRACE = 5
};

class ErplTracer {
public:
    static ErplTracer& Instance();
    
    void SetEnabled(bool enabled);
    void SetLevel(TraceLevel level);
    void SetTraceDirectory(const std::string& directory);
    void SetOutputMode(const std::string& output_mode);
    void SetMaxFileSize(int64_t max_size);
    void SetRotation(bool rotation);
    
    bool IsEnabled() const { return enabled; }
    TraceLevel GetLevel() const { return level; }
    std::string GetOutputMode() const { return output_mode; }
    int64_t GetMaxFileSize() const { return max_file_size; }
    bool GetRotation() const { return rotation_enabled; }
    
    void Trace(TraceLevel msg_level, const std::string& component, const std::string& message);
    void Trace(TraceLevel msg_level, const std::string& component, const std::string& message, const std::string& data);
    
    // Convenience methods for different trace levels
    void Error(const std::string& component, const std::string& message);
    void Error(const std::string& component, const std::string& message, const std::string& data);
    void Warn(const std::string& component, const std::string& message);
    void Warn(const std::string& component, const std::string& message, const std::string& data);
    void Info(const std::string& component, const std::string& message);
    void Info(const std::string& component, const std::string& message, const std::string& data);
    void Debug(const std::string& component, const std::string& message);
    void Debug(const std::string& component, const std::string& message, const std::string& data);
    void Trace(const std::string& component, const std::string& message);
    void Trace(const std::string& component, const std::string& message, const std::string& data);

private:
    ErplTracer() = default;
    ~ErplTracer() = default;
    ErplTracer(const ErplTracer&) = delete;
    ErplTracer& operator=(const ErplTracer&) = delete;
    
    void WriteToFile(const std::string& message);
    std::string GetTimestamp();
    std::string LevelToString(TraceLevel level);
    
    bool enabled = false;
    TraceLevel level = TraceLevel::INFO;
    std::string trace_directory = ".";
    std::string output_mode = "console";
    int64_t max_file_size = 10485760; // 10MB default
    bool rotation_enabled = true;
    std::unique_ptr<std::ofstream> trace_file;
    std::mutex trace_mutex;
};

} // namespace erpl_web

// Lightweight C-style tracing wrappers to avoid dependency on class name in macros
void erpl_trace_error(const std::string &component, const std::string &message);
void erpl_trace_error_data(const std::string &component, const std::string &message, const std::string &data);
void erpl_trace_warn(const std::string &component, const std::string &message);
void erpl_trace_warn_data(const std::string &component, const std::string &message, const std::string &data);
void erpl_trace_info(const std::string &component, const std::string &message);
void erpl_trace_info_data(const std::string &component, const std::string &message, const std::string &data);
void erpl_trace_debug(const std::string &component, const std::string &message);
void erpl_trace_debug_data(const std::string &component, const std::string &message, const std::string &data);
void erpl_trace_trace(const std::string &component, const std::string &message);
void erpl_trace_trace_data(const std::string &component, const std::string &message, const std::string &data);

// Convenience macros for tracing (defined outside namespace)
#define ERPL_TRACE_ERROR(component, message) \
    ::erpl_trace_error(component, message)

#define ERPL_TRACE_ERROR_DATA(component, message, data) \
    ::erpl_trace_error_data(component, message, data)

#define ERPL_TRACE_WARN(component, message) \
    ::erpl_trace_warn(component, message)

#define ERPL_TRACE_WARN_DATA(component, message, data) \
    ::erpl_trace_warn_data(component, message, data)

#define ERPL_TRACE_INFO(component, message) \
    ::erpl_trace_info(component, message)

#define ERPL_TRACE_INFO_DATA(component, message, data) \
    ::erpl_trace_info_data(component, message, data)

#define ERPL_TRACE_DEBUG(component, message) \
    ::erpl_trace_debug(component, message)

#define ERPL_TRACE_DEBUG_DATA(component, message, data) \
    ::erpl_trace_debug_data(component, message, data)

#define ERPL_TRACE_TRACE(component, message) \
    ::erpl_trace_trace(component, message)

#define ERPL_TRACE_TRACE_DATA(component, message, data) \
    ::erpl_trace_trace_data(component, message, data)

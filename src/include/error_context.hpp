#pragma once

#include <string>
#include <map>
#include <memory>

namespace erpl_web {

/**
 * Error Context Helper
 *
 * Provides structured error context for consistent error handling across modules.
 * Helps with debugging by capturing contextual information when errors occur.
 *
 * Usage:
 *   ErrorContext ctx;
 *   ctx.Set("operation", "fetch_models")
 *      .Set("tenant", tenant_id)
 *      .Set("region", region)
 *      .ThrowIfFailed("Failed to fetch models");
 */
class ErrorContext {
public:
    /**
     * Create a new error context
     */
    ErrorContext() = default;

    /**
     * Set a context variable
     *
     * @param key The context key
     * @param value The context value
     * @return Reference to this context (for chaining)
     */
    ErrorContext& Set(const std::string& key, const std::string& value);

    /**
     * Get a context variable
     *
     * @param key The context key
     * @return The context value, or empty string if not set
     */
    std::string Get(const std::string& key) const;

    /**
     * Build a formatted error message with context
     *
     * @param base_message The base error message
     * @return Formatted message with context appended
     *
     * Example:
     *   auto ctx = ErrorContext();
     *   ctx.Set("operation", "query").Set("tenant", "test");
     *   auto msg = ctx.Format("Operation failed");
     *   // Returns: "Operation failed [operation: query, tenant: test]"
     */
    std::string Format(const std::string& base_message) const;

    /**
     * Clear all context variables
     */
    void Clear();

    /**
     * Check if context is empty
     *
     * @return true if no context variables set, false otherwise
     */
    bool IsEmpty() const;

private:
    std::map<std::string, std::string> context_;
};

/**
 * Scoped Error Context
 *
 * RAII wrapper for automatic error context cleanup.
 * Automatically clears context when scope exits.
 *
 * Usage:
 *   {
 *       ScopedErrorContext ctx;
 *       ctx.Set("operation", "fetch")
 *          .Set("tenant", tenant_id);
 *       // Use ctx for error handling
 *       // Context automatically cleared when scope exits
 *   }
 */
class ScopedErrorContext : public ErrorContext {
public:
    /**
     * Create a new scoped error context
     */
    ScopedErrorContext() = default;

    /**
     * Destructor clears context on scope exit
     */
    ~ScopedErrorContext();

    // Explicitly deleted copy operations (RAII pattern)
    ScopedErrorContext(const ScopedErrorContext&) = delete;
    ScopedErrorContext& operator=(const ScopedErrorContext&) = delete;

    // Allow move operations
    ScopedErrorContext(ScopedErrorContext&&) = default;
    ScopedErrorContext& operator=(ScopedErrorContext&&) = default;
};

} // namespace erpl_web

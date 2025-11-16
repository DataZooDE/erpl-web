#include "error_context.hpp"
#include <sstream>

namespace erpl_web {

ErrorContext& ErrorContext::Set(const std::string& key, const std::string& value) {
    context_[key] = value;
    return *this;
}

std::string ErrorContext::Get(const std::string& key) const {
    auto it = context_.find(key);
    if (it != context_.end()) {
        return it->second;
    }
    return "";
}

std::string ErrorContext::Format(const std::string& base_message) const {
    if (context_.empty()) {
        return base_message;
    }

    std::ostringstream result;
    result << base_message << " [";

    bool first = true;
    for (const auto& [key, value] : context_) {
        if (!first) {
            result << ", ";
        }
        result << key << ": " << value;
        first = false;
    }

    result << "]";
    return result.str();
}

void ErrorContext::Clear() {
    context_.clear();
}

bool ErrorContext::IsEmpty() const {
    return context_.empty();
}

// ===== ScopedErrorContext =====

ScopedErrorContext::~ScopedErrorContext() {
    Clear();
}

} // namespace erpl_web

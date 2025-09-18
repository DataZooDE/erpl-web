#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ErplWebExtension {
public:
	static void Load(ExtensionLoader &loader);
	static std::string Name();
	static std::string Version();
};

} // namespace duckdb

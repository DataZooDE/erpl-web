#pragma once

#include <string>
#include <vector>
#include <optional>
#include <regex>

namespace erpl_web {

/**
 * Pattern Matcher Utility
 *
 * Consolidates pattern matching, content type detection, and format detection
 * across the extension. Reduces code duplication and provides consistent
 * pattern validation interfaces.
 *
 * Patterns supported:
 * - Binary content detection (images, archives, etc.)
 * - JSON/XML format detection
 * - OData version detection (V2 vs V4)
 * - Content-Type header parsing
 *
 * Usage:
 *   auto content_type = "application/json; charset=utf-8";
 *   if (PatternMatcher::IsJson(content_type)) {
 *       // Handle JSON response
 *   }
 *
 *   std::vector<uint8_t> content = {0xFF, 0xD8, 0xFF, ...};
 *   if (PatternMatcher::IsBinaryContent(content)) {
 *       // Handle binary data
 *   }
 */
class PatternMatcher {
public:
	/**
	 * Detect if content type is JSON
	 *
	 * Matches: application/json, application/ *+json, text/json, etc.
	 *
	 * @param content_type The Content-Type header value
	 * @return true if content type indicates JSON, false otherwise
	 */
	static bool IsJson(const std::string& content_type);

	/**
	 * Detect if content type is XML
	 *
	 * Matches: application/xml, text/xml, application/ *+xml, etc.
	 *
	 * @param content_type The Content-Type header value
	 * @return true if content type indicates XML, false otherwise
	 */
	static bool IsXml(const std::string& content_type);

	/**
	 * Detect if content is binary (non-text) data
	 *
	 * Checks for binary file signatures (magic bytes):
	 * - Images: PNG, JPEG, GIF, BMP, WebP
	 * - Archives: ZIP, GZIP, TAR, RAR, 7z, Brotli, Zstd
	 * - Documents: PDF, MS Office, OpenDocument
	 * - Audio/Video: MP3, MP4, WebM, Ogg, FLAC, WAV
	 * - Executables: ELF, Mach-O, PE
	 *
	 * @param content Binary data to check
	 * @return true if content appears to be binary, false if text
	 */
	static bool IsBinaryContent(const std::vector<uint8_t>& content);

	/**
	 * Detect OData version from response content
	 *
	 * Analyzes JSON/XML structure to determine if response is OData v2 or v4:
	 * - v4: Uses "value" array with "@odata.context" or "@odata.type"
	 * - v2: Uses "d" wrapper with "__metadata" or "results" array
	 *
	 * @param content Response content (JSON or XML)
	 * @param content_type Content-Type header for format hint
	 * @return OData version (v2, v4) or empty if not OData
	 */
	static std::optional<std::string> DetectODataVersion(
		const std::string& content,
		const std::string& content_type);

	/**
	 * Parse Content-Type header and extract media type
	 *
	 * Handles: "application/json; charset=utf-8" → "application/json"
	 * Also extracts charset if present.
	 *
	 * @param content_type Full Content-Type header
	 * @return Pair of (media_type, charset) or empty charset if not present
	 *
	 * Example:
	 *   auto [media_type, charset] = PatternMatcher::ParseContentType(header).value();
	 *   // media_type: "application/json", charset: "utf-8"
	 */
	static std::optional<std::pair<std::string, std::string>> ParseContentType(
		const std::string& content_type);

	/**
	 * Detect charset from Content-Type header
	 *
	 * Extracts charset parameter from Content-Type header.
	 * Returns UTF-8 if not specified (default for web).
	 *
	 * @param content_type Content-Type header value
	 * @return Detected or default charset
	 *
	 * Example:
	 *   auto charset = PatternMatcher::DetectCharset("application/json; charset=iso-8859-1");
	 *   // Returns: "iso-8859-1"
	 */
	static std::string DetectCharset(const std::string& content_type);

	/**
	 * Check if content looks like JSON (heuristic check)
	 *
	 * Performs basic validation: starts with { or [ and contains valid JSON structure.
	 * Does not fully parse JSON (use yyjson for that).
	 *
	 * @param content Text content to check
	 * @return true if content looks like JSON, false otherwise
	 */
	static bool LooksLikeJson(const std::string& content);

	/**
	 * Check if content looks like XML (heuristic check)
	 *
	 * Performs basic validation: starts with < and looks like XML.
	 * Does not fully parse XML (use tinyxml2 for that).
	 *
	 * @param content Text content to check
	 * @return true if content looks like XML, false otherwise
	 */
	static bool LooksLikeXml(const std::string& content);

private:
	// Binary content magic bytes for common file formats
	static const std::vector<std::vector<uint8_t>> BINARY_SIGNATURES;

	// Common binary content type patterns
	static const std::vector<std::string> BINARY_CONTENT_TYPES;

	/**
	 * Check if content starts with any of the known binary signatures
	 *
	 * @param content Content to check
	 * @return true if signature matches
	 */
	static bool MatchesBinarySignature(const std::vector<uint8_t>& content);

	/**
	 * Check if content type indicates binary data
	 *
	 * @param content_type Content-Type header
	 * @return true if content type pattern matches binary
	 */
	static bool MatchesBinaryContentType(const std::string& content_type);
};

} // namespace erpl_web

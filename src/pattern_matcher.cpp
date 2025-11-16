#include "pattern_matcher.hpp"
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <sstream>

namespace erpl_web {

// Binary file signatures (magic bytes)
// Ordered by frequency: most common formats first
const std::vector<std::vector<uint8_t>> PatternMatcher::BINARY_SIGNATURES = {
	// Images
	{0xFF, 0xD8, 0xFF}, // JPEG
	{0x89, 0x50, 0x4E, 0x47}, // PNG
	{0x47, 0x49, 0x46}, // GIF
	{0x42, 0x4D}, // BMP
	{0x52, 0x49, 0x46, 0x46}, // WebP/WAV

	// Archives and compression
	{0x50, 0x4B, 0x03, 0x04}, // ZIP
	{0x1F, 0x8B}, // GZIP
	{0x7B, 0x5A}, // Bzip2
	{0x28, 0xB5, 0x2F, 0xFD}, // Zstd
	{0xCE, 0xB2, 0xCF, 0x81}, // Brotli
	{0x75, 0x73, 0x74, 0x61, 0x72}, // TAR (at offset 257)
	{0x52, 0x61, 0x72, 0x21}, // RAR
	{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}, // 7z

	// Documents
	{0x25, 0x50, 0x44, 0x46}, // PDF
	{0xD0, 0xCF, 0x11, 0xE0}, // MS Office 97-2003
	{0x50, 0x4B, 0x03, 0x04}, // Office Open XML

	// Audio/Video
	{0xFF, 0xFB}, // MP3
	{0xFF, 0xFA}, // MP3 MPEG2
	{0x49, 0x44, 0x33}, // ID3 tag
	{0x00, 0x00, 0x00, 0x20, 0x66, 0x74, 0x79, 0x70}, // MP4
	{0x1A, 0x45, 0xDF, 0xA3}, // WebM/Matroska
	{0x4F, 0x67, 0x67, 0x53}, // Ogg
	{0x66, 0x4C, 0x61, 0x43}, // FLAC
	{0x52, 0x49, 0x46, 0x46}, // WAV

	// Executables
	{0x7F, 0x45, 0x4C, 0x46}, // ELF
	{0xFE, 0xED, 0xFA}, // Mach-O
	{0x4D, 0x5A}, // PE (Windows)

	// Other formats
	{0xAC, 0xED}, // Java serialized object
	{0xCA, 0xFE, 0xBA, 0xBE}, // Java class file
};

// Common binary content type patterns
const std::vector<std::string> PatternMatcher::BINARY_CONTENT_TYPES = {
	"image/",
	"audio/",
	"video/",
	"application/octet-stream",
	"application/pdf",
	"application/zip",
	"application/x-zip",
	"application/x-gzip",
	"application/x-rar",
	"application/x-7z",
	"application/x-bzip2",
	"application/x-tar",
	"application/vnd.ms-excel",
	"application/vnd.openxmlformats",
	"application/vnd.ms-word",
	"application/vnd.ms-powerpoint",
};

bool PatternMatcher::IsJson(const std::string& content_type) {
	if (content_type.empty()) {
		return false;
	}

	std::string ct_lower = content_type;
	std::transform(ct_lower.begin(), ct_lower.end(), ct_lower.begin(),
				   [](unsigned char c) { return std::tolower(c); });

	// Check for JSON content types
	return ct_lower.find("application/json") != std::string::npos ||
		   ct_lower.find("text/json") != std::string::npos ||
		   ct_lower.find("+json") != std::string::npos;
}

bool PatternMatcher::IsXml(const std::string& content_type) {
	if (content_type.empty()) {
		return false;
	}

	std::string ct_lower = content_type;
	std::transform(ct_lower.begin(), ct_lower.end(), ct_lower.begin(),
				   [](unsigned char c) { return std::tolower(c); });

	// Check for XML content types
	return ct_lower.find("application/xml") != std::string::npos ||
		   ct_lower.find("text/xml") != std::string::npos ||
		   ct_lower.find("+xml") != std::string::npos;
}

bool PatternMatcher::MatchesBinarySignature(const std::vector<uint8_t>& content) {
	if (content.empty()) {
		return false;
	}

	// Check against known binary signatures
	for (const auto& signature : BINARY_SIGNATURES) {
		if (content.size() >= signature.size()) {
			bool matches = true;
			for (size_t i = 0; i < signature.size(); ++i) {
				if (content[i] != signature[i]) {
					matches = false;
					break;
				}
			}
			if (matches) {
				return true;
			}
		}
	}

	return false;
}

bool PatternMatcher::MatchesBinaryContentType(const std::string& content_type) {
	if (content_type.empty()) {
		return false;
	}

	std::string ct_lower = content_type;
	std::transform(ct_lower.begin(), ct_lower.end(), ct_lower.begin(),
				   [](unsigned char c) { return std::tolower(c); });

	for (const auto& binary_ct : BINARY_CONTENT_TYPES) {
		if (ct_lower.find(binary_ct) != std::string::npos) {
			return true;
		}
	}

	return false;
}

bool PatternMatcher::IsBinaryContent(const std::vector<uint8_t>& content) {
	// First check magic bytes (most reliable)
	if (MatchesBinarySignature(content)) {
		return true;
	}

	// Heuristic: if content is very large and has low text ratio, likely binary
	if (content.size() > 1024) {
		int printable_chars = 0;
		for (uint8_t byte : content) {
			if ((byte >= 32 && byte <= 126) || byte == '\t' || byte == '\n' || byte == '\r') {
				printable_chars++;
			}
		}
		// If less than 25% printable, likely binary
		if (printable_chars < content.size() / 4) {
			return true;
		}
	}

	return false;
}

std::optional<std::string> PatternMatcher::DetectODataVersion(
	const std::string& content,
	const std::string& content_type) {

	if (content.empty()) {
		return std::nullopt;
	}

	// Quick lowercase check
	std::string content_type_lower = content_type;
	std::transform(content_type_lower.begin(), content_type_lower.end(),
				   content_type_lower.begin(),
				   [](unsigned char c) { return std::tolower(c); });

	bool is_json = content_type_lower.find("json") != std::string::npos ||
				   (LooksLikeJson(content) && !LooksLikeXml(content));
	bool is_xml = content_type_lower.find("xml") != std::string::npos ||
				  (LooksLikeXml(content) && !LooksLikeJson(content));

	if (is_json) {
		// OData v4 JSON uses "value" array with "@odata.context" or "@odata.type"
		if (content.find("\"@odata.context\"") != std::string::npos ||
			content.find("\"@odata.type\"") != std::string::npos ||
			content.find("\"value\"") != std::string::npos) {
			return "v4";
		}
		// OData v2 JSON uses "d" wrapper with "__metadata"
		if (content.find("\"d\"") != std::string::npos ||
			content.find("\"__metadata\"") != std::string::npos) {
			return "v2";
		}
	}

	if (is_xml) {
		// OData v4 XML namespace
		if (content.find("http://docs.oasis-open.org/odata/ns/") != std::string::npos) {
			return "v4";
		}
		// OData v2 XML namespace
		if (content.find("http://schemas.microsoft.com/ado/2007/08/dataservices") != std::string::npos) {
			return "v2";
		}
	}

	return std::nullopt;
}

std::optional<std::pair<std::string, std::string>> PatternMatcher::ParseContentType(
	const std::string& content_type) {

	if (content_type.empty()) {
		return std::nullopt;
	}

	// Find semicolon separator
	size_t semicolon_pos = content_type.find(';');
	std::string media_type = content_type.substr(0, semicolon_pos);

	// Trim whitespace
	media_type.erase(0, media_type.find_first_not_of(" \t"));
	media_type.erase(media_type.find_last_not_of(" \t") + 1);

	std::string charset = "utf-8"; // Default

	if (semicolon_pos != std::string::npos) {
		// Extract charset if present
		std::string params = content_type.substr(semicolon_pos + 1);
		size_t charset_pos = params.find("charset");
		if (charset_pos != std::string::npos) {
			size_t equals_pos = params.find('=', charset_pos);
			if (equals_pos != std::string::npos) {
				charset = params.substr(equals_pos + 1);
				// Trim quotes and whitespace
				charset.erase(0, charset.find_first_not_of(" \t\"'"));
				charset.erase(charset.find_last_not_of(" \t\"'") + 1);
			}
		}
	}

	return std::make_pair(media_type, charset);
}

std::string PatternMatcher::DetectCharset(const std::string& content_type) {
	auto parsed = ParseContentType(content_type);
	if (parsed) {
		return parsed->second;
	}
	return "utf-8"; // Default
}

bool PatternMatcher::LooksLikeJson(const std::string& content) {
	if (content.empty()) {
		return false;
	}

	// Find first non-whitespace character
	size_t start = content.find_first_not_of(" \t\n\r");
	if (start == std::string::npos) {
		return false;
	}

	// JSON must start with { or [
	char first_char = content[start];
	if (first_char != '{' && first_char != '[') {
		return false;
	}

	// Check for basic JSON structure
	return content.find(':') != std::string::npos || content.find('"') != std::string::npos;
}

bool PatternMatcher::LooksLikeXml(const std::string& content) {
	if (content.empty()) {
		return false;
	}

	// Find first non-whitespace character
	size_t start = content.find_first_not_of(" \t\n\r");
	if (start == std::string::npos) {
		return false;
	}

	// XML must start with <
	if (content[start] != '<') {
		return false;
	}

	// Check for XML declaration or element tag
	if (start + 1 < content.length()) {
		char next_char = content[start + 1];
		return next_char == '?' || next_char == '!'; // XML declaration or DOCTYPE
	}

	return content.find("<?xml") != std::string::npos ||
		   content.find("<!DOCTYPE") != std::string::npos;
}

} // namespace erpl_web

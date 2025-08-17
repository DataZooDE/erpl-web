#pragma once
#include <string>

namespace erpl_web {

// Browser helper for opening URLs (cross-platform)
class DatasphereBrowserHelper {
public:
    // Open URL in default browser
    static void OpenUrl(const std::string& url);
    
    // Find available port for local server
    static int FindAvailablePort(int start_port = 65000);
    
    // Check if a port is available
    static bool IsPortAvailable(int port);
    
    // Platform-specific browser detection
    static std::string GetDefaultBrowser();

private:
    // Platform-specific browser opening
    static void OpenUrlWindows(const std::string& url);
    static void OpenUrlMacOS(const std::string& url);
    static void OpenUrlLinux(const std::string& url);
    
    // Platform-specific port checking
    static bool IsPortAvailableWindows(int port);
    static bool IsPortAvailableMacOS(int port);
    static bool IsPortAvailableLinux(int port);
};

} // namespace erpl_web

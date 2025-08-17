#include "erpl_datasphere_browser.hpp"
#include <cstdlib>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#include <shellapi.h>
#elif defined(__APPLE__)
#include <CoreFoundation/CoreFoundation.h>
#include <ApplicationServices/ApplicationServices.h>
#else
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace erpl_web {

void DatasphereBrowserHelper::OpenUrl(const std::string& url) {
#ifdef _WIN32
    OpenUrlWindows(url);
#elif defined(__APPLE__)
    OpenUrlMacOS(url);
#else
    OpenUrlLinux(url);
#endif
}

int DatasphereBrowserHelper::FindAvailablePort(int start_port) {
    for (int port = start_port; port < start_port + 100; ++port) {
        if (IsPortAvailable(port)) {
            return port;
        }
    }
    throw std::runtime_error("No available ports found in range " + std::to_string(start_port) + "-" + std::to_string(start_port + 99));
}

bool DatasphereBrowserHelper::IsPortAvailable(int port) {
#ifdef _WIN32
    return IsPortAvailableWindows(port);
#elif defined(__APPLE__)
    return IsPortAvailableMacOS(port);
#else
    return IsPortAvailableLinux(port);
#endif
}

std::string DatasphereBrowserHelper::GetDefaultBrowser() {
#ifdef _WIN32
    return "default"; // Windows will use default browser
#elif defined(__APPLE__)
    return "open"; // macOS open command
#else
    return "xdg-open"; // Linux xdg-open command
#endif
}

void DatasphereBrowserHelper::OpenUrlWindows(const std::string& url) {
#ifdef _WIN32
    HINSTANCE result = ShellExecuteA(NULL, "open", url.c_str(), NULL, NULL, SW_SHOWNORMAL);
    if (result <= (HINSTANCE)32) {
        throw std::runtime_error("Failed to open browser on Windows");
    }
#else
    // Not Windows - throw error or use fallback
    throw std::runtime_error("Windows-specific browser opening not available on this platform");
#endif
}

void DatasphereBrowserHelper::OpenUrlMacOS(const std::string& url) {
#ifdef __APPLE__
    CFStringRef urlString = CFStringCreateWithCString(NULL, url.c_str(), kCFStringEncodingUTF8);
    CFURLRef urlRef = CFURLCreateWithString(NULL, urlString, NULL);
    
    if (urlRef) {
        LSOpenCFURLRef(urlRef, NULL);
        CFRelease(urlRef);
    }
    
    CFRelease(urlString);
#else
    // Not macOS - throw error or use fallback
    throw std::runtime_error("macOS-specific browser opening not available on this platform");
#endif
}

void DatasphereBrowserHelper::OpenUrlLinux(const std::string& url) {
#ifndef _WIN32
#ifndef __APPLE__
    pid_t pid = fork();
    if (pid == 0) {
        // Child process
        execlp("xdg-open", "xdg-open", url.c_str(), NULL);
        exit(1); // If execlp fails
    } else if (pid > 0) {
        // Parent process - wait for child to start
        int status;
        waitpid(pid, &status, WNOHANG);
    } else {
        throw std::runtime_error("Failed to fork process for opening browser");
    }
#else
    throw std::runtime_error("Linux-specific browser opening not available on this platform");
#endif
#else
    throw std::runtime_error("Linux-specific browser opening not available on this platform");
#endif
}

bool DatasphereBrowserHelper::IsPortAvailableWindows(int port) {
    // Windows implementation would use WinSock to check port availability
    // For now, return true to avoid blocking
    return true;
}

bool DatasphereBrowserHelper::IsPortAvailableMacOS(int port) {
    // macOS implementation would use socket to check port availability
    // For now, return true to avoid blocking
    return true;
}

bool DatasphereBrowserHelper::IsPortAvailableLinux(int port) {
    // Linux implementation would use socket to check port availability
    // For now, return true to avoid blocking
    return true;
}

} // namespace erpl_web

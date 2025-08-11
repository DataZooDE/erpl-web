#include "catch.hpp"
#include "erpl_tracing.hpp"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>

using namespace erpl_web;

TEST_CASE("ErplTracer Singleton Pattern", "[tracing]") {
    SECTION("Instance returns same reference") {
        auto& instance1 = ErplTracer::Instance();
        auto& instance2 = ErplTracer::Instance();
        REQUIRE(&instance1 == &instance2);
    }
}

TEST_CASE("ErplTracer Basic Functionality", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    // Reset to known state
    tracer.SetEnabled(false);
    tracer.SetLevel(TraceLevel::INFO);
    
    SECTION("Default state") {
        REQUIRE_FALSE(tracer.IsEnabled());
        REQUIRE(tracer.GetLevel() == TraceLevel::INFO);
    }
    
    SECTION("Enable/Disable tracing") {
        tracer.SetEnabled(true);
        REQUIRE(tracer.IsEnabled());
        
        tracer.SetEnabled(false);
        REQUIRE_FALSE(tracer.IsEnabled());
    }
    
    SECTION("Set trace level") {
        tracer.SetLevel(TraceLevel::DEBUG_LEVEL);
        REQUIRE(tracer.GetLevel() == TraceLevel::DEBUG_LEVEL);
        
        tracer.SetLevel(TraceLevel::ERROR);
        REQUIRE(tracer.GetLevel() == TraceLevel::ERROR);
        
        tracer.SetLevel(TraceLevel::TRACE);
        REQUIRE(tracer.GetLevel() == TraceLevel::TRACE);
    }
}

TEST_CASE("ErplTracer Level Filtering", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    // Enable tracing at INFO level
    tracer.SetEnabled(true);
    tracer.SetLevel(TraceLevel::INFO);
    
    SECTION("Messages at or below current level should be logged") {
        // Capture cout output
        std::stringstream buffer;
        std::streambuf* old_cout = std::cout.rdbuf(buffer.rdbuf());
        
        tracer.Error("TEST", "Error message");
        tracer.Warn("TEST", "Warning message");
        tracer.Info("TEST", "Info message");
        
        std::cout.rdbuf(old_cout);
        
        std::string output = buffer.str();
        REQUIRE(output.find("ERROR") != std::string::npos);
        REQUIRE(output.find("WARN") != std::string::npos);
        REQUIRE(output.find("INFO") != std::string::npos);
    }
    
    SECTION("Messages above current level should not be logged") {
        std::stringstream buffer;
        std::streambuf* old_cout = std::cout.rdbuf(buffer.rdbuf());
        
        tracer.Debug("TEST", "Debug message");
        tracer.Trace("TEST", "Trace message");
        
        std::cout.rdbuf(old_cout);
        
        std::string output = buffer.str();
        REQUIRE(output.find("DEBUG") == std::string::npos);
        REQUIRE(output.find("TRACE") == std::string::npos);
    }
}

TEST_CASE("ErplTracer File Output", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    // Create temporary directory for testing
    std::string test_dir = "./test_trace_output";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    
    tracer.SetTraceDirectory(test_dir);
    tracer.SetEnabled(true);
    tracer.SetLevel(TraceLevel::DEBUG_LEVEL);
    
    SECTION("Trace messages are written to file") {
        std::string test_message = "Test trace message";
        tracer.Info("TEST", test_message);
        
        // Check if file was created
        std::filesystem::path trace_file_path = test_dir / "erpl_web_trace.log";
        REQUIRE(std::filesystem::exists(trace_file_path));
        
        // Read file content
        std::ifstream file(trace_file_path);
        REQUIRE(file.is_open());
        
        std::string content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
        file.close();
        
        REQUIRE(content.find(test_message) != std::string::npos);
        REQUIRE(content.find("INFO") != std::string::npos);
        REQUIRE(content.find("TEST") != std::string::npos);
    }
    
    // Cleanup
    tracer.SetEnabled(false);
    std::filesystem::remove_all(test_dir);
}

TEST_CASE("ErplTracer Thread Safety", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    tracer.SetEnabled(true);
    tracer.SetLevel(TraceLevel::DEBUG_LEVEL);
    
    SECTION("Multiple threads can trace simultaneously") {
        const int num_threads = 10;
        const int messages_per_thread = 100;
        
        std::vector<std::thread> threads;
        std::atomic<int> total_messages(0);
        
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&tracer, i, messages_per_thread, &total_messages]() {
                for (int j = 0; j < messages_per_thread; ++j) {
                    tracer.Info("THREAD_" + std::to_string(i), 
                               "Message " + std::to_string(j));
                    total_messages++;
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        REQUIRE(total_messages == num_threads * messages_per_thread);
    }
}

TEST_CASE("ErplTracer Data Messages", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    tracer.SetEnabled(true);
    tracer.SetLevel(TraceLevel::DEBUG_LEVEL);
    
    SECTION("Messages with data are properly formatted") {
        std::stringstream buffer;
        std::streambuf* old_cout = std::cout.rdbuf(buffer.rdbuf());
        
        std::string test_data = "{\"key\": \"value\", \"number\": 42}";
        tracer.Info("TEST", "JSON data received", test_data);
        
        std::cout.rdbuf(old_cout);
        
        std::string output = buffer.str();
        REQUIRE(output.find("JSON data received") != std::string::npos);
        REQUIRE(output.find("Data: " + test_data) != std::string::npos);
    }
}

TEST_CASE("ErplTracer Timestamp Format", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    tracer.SetEnabled(true);
    tracer.SetLevel(TraceLevel::INFO);
    
    SECTION("Timestamps are properly formatted") {
        std::stringstream buffer;
        std::streambuf* old_cout = std::cout.rdbuf(buffer.rdbuf());
        
        tracer.Info("TEST", "Timestamp test");
        
        std::cout.rdbuf(old_cout);
        
        std::string output = buffer.str();
        
        // Check for timestamp format: YYYY-MM-DD HH:MM:SS.mmm
        REQUIRE(output.find("20") != std::string::npos); // Year
        REQUIRE(output.find("-") != std::string::npos);  // Date separator
        REQUIRE(output.find(":") != std::string::npos);  // Time separator
        REQUIRE(output.find(".") != std::string::npos);  // Millisecond separator
    }
}

TEST_CASE("ErplTracer Level String Conversion", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    SECTION("All trace levels have proper string representations") {
        std::stringstream buffer;
        std::streambuf* old_cout = std::cout.rdbuf(buffer.rdbuf());
        
        tracer.SetLevel(TraceLevel::NONE);
        tracer.SetLevel(TraceLevel::ERROR);
        tracer.SetLevel(TraceLevel::WARN);
        tracer.SetLevel(TraceLevel::INFO);
        tracer.SetLevel(TraceLevel::DEBUG_LEVEL);
        tracer.SetLevel(TraceLevel::TRACE);
        
        std::cout.rdbuf(old_cout);
        
        std::string output = buffer.str();
        REQUIRE(output.find("NONE") != std::string::npos);
        REQUIRE(output.find("ERROR") != std::string::npos);
        REQUIRE(output.find("WARN") != std::string::npos);
        REQUIRE(output.find("INFO") != std::string::npos);
        REQUIRE(output.find("DEBUG") != std::string::npos);
        REQUIRE(output.find("TRACE") != std::string::npos);
    }
}

TEST_CASE("ErplTracer Directory Creation", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    // Test with non-existent directory
    std::string test_dir = "./non_existent_trace_dir";
    std::filesystem::remove_all(test_dir);
    
    REQUIRE_FALSE(std::filesystem::exists(test_dir));
    
    tracer.SetTraceDirectory(test_dir);
    REQUIRE(std::filesystem::exists(test_dir));
    
    // Cleanup
    std::filesystem::remove_all(test_dir);
}

TEST_CASE("ErplTracer File Reopening", "[tracing]") {
    auto& tracer = ErplTracer::Instance();
    
    std::string test_dir = "./test_reopen";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    
    tracer.SetTraceDirectory(test_dir);
    tracer.SetEnabled(true);
    tracer.SetLevel(TraceLevel::INFO);
    
    // Write a message
    tracer.Info("TEST", "First message");
    
    // Change directory
    std::string new_dir = "./test_reopen_new";
    std::filesystem::remove_all(new_dir);
    std::filesystem::create_directories(new_dir);
    
    tracer.SetTraceDirectory(new_dir);
    
    // Write another message
    tracer.Info("TEST", "Second message");
    
    // Check both files exist
    std::filesystem::path old_file = test_dir / "erpl_web_trace.log";
    std::filesystem::path new_file = new_dir / "erpl_web_trace.log";
    
    REQUIRE(std::filesystem::exists(old_file));
    REQUIRE(std::filesystem::exists(new_file));
    
    // Cleanup
    tracer.SetEnabled(false);
    std::filesystem::remove_all(test_dir);
    std::filesystem::remove_all(new_dir);
}

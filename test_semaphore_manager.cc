#include "src/core/ext/transport/shmem/shmem_transport.h"
#include "src/core/ext/transport/shmem/shmem_semaphore.h"
#include <iostream>

// Simple test to check semaphore functionality
int main() {
    std::cout << "Testing semaphore manager creation..." << std::endl;
    
    try {
        // Test creating a simple CrossProcessSemaphore 
        grpc_shmem::CrossProcessSemaphore sem;
        std::cout << "CrossProcessSemaphore created successfully" << std::endl;
        
        // Test SemaphoreManager creation
        grpc_shmem::SemaphoreManager mgr;
        std::cout << "SemaphoreManager created successfully" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
        return 1;
    }
}
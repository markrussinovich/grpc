// Simple demo gRPC server using the shared-memory transport
#include <iostream>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "src/core/ext/transport/shmem/shmem_transport.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// Define a simple Echo service
class EchoServiceImpl final : public grpc::Service {
 public:
  grpc::Status Echo(ServerContext* context, const grpc::ByteBuffer* request,
                    grpc::ByteBuffer* response) override {
    std::string msg;
    grpc::Slice slice;
    if (request->Dump(&slice, 1) > 0) {
      msg = std::string(reinterpret_cast<const char*>(slice.begin()), slice.size());
    }
    std::cout << "[Server] Received: " << msg << std::endl;
    *response = *request;
    return Status::OK;
  }
};

int main(int argc, char** argv) {
  std::string server_address("shmem://demo");
  EchoServiceImpl service;

  ServerBuilder builder;
  // Register the shared memory transport
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "[Server] Listening on " << server_address << std::endl;
  server->Wait();
  return 0;
}

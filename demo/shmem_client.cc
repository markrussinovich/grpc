// Simple demo gRPC client using the shared-memory transport
#include <iostream>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "src/core/ext/transport/shmem/shmem_transport.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

int main(int argc, char** argv) {
  std::string server_address("shmem://demo");
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());

  // Prepare a generic stub for demonstration
  std::unique_ptr<grpc::GenericStub> stub(new grpc::GenericStub(channel));
  ClientContext context;
  grpc::ByteBuffer request;
  std::string msg = "Hello from shmem client!";
  grpc::Slice slice(msg);
  request = grpc::ByteBuffer(&slice, 1);
  grpc::ByteBuffer response;

  Status status = stub->Call(&context, "/Echo/Echo", request, &response);
  if (status.ok()) {
    grpc::Slice resp_slice;
    if (response.Dump(&resp_slice, 1) > 0) {
      std::string resp_msg(reinterpret_cast<const char*>(resp_slice.begin()), resp_slice.size());
      std::cout << "[Client] Received: " << resp_msg << std::endl;
    }
  } else {
    std::cout << "[Client] RPC failed: " << status.error_message() << std::endl;
  }
  return 0;
}

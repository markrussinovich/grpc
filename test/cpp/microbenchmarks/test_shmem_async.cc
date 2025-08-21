#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <thread>
#include <chrono>

#include "src/proto/grpc/testing/echo.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::ServerAsyncResponseWriter;
using grpc::Status;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::ClientAsyncResponseReader;
using grpc::Channel;

using grpc::testing::EchoTestService;
using grpc::testing::EchoRequest;
using grpc::testing::EchoResponse;

int main() {
    // Create server
    EchoTestService::AsyncService service;
    ServerBuilder builder;
    const std::string server_address = "shmem://test-async";
    
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    auto cq = builder.AddCompletionQueue();
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    
    printf("Server started on %s\n", server_address.c_str());
    
    // Set up server-side request handler
    ServerContext server_ctx;
    EchoRequest server_request;
    ServerAsyncResponseWriter<EchoResponse> responder(&server_ctx);
    
    // This should complete with tag (void*)1 when a request arrives
    service.RequestEcho(&server_ctx, &server_request, &responder, 
                       cq.get(), cq.get(), (void*)1);
    printf("RequestEcho registered with tag 1\n");
    
    // Create client  
    auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    auto stub = EchoTestService::NewStub(channel);
    
    // Send async request
    ClientContext client_ctx;
    EchoRequest client_request;
    client_request.set_message("test");
    EchoResponse client_response;
    
    grpc::Status status;
    auto rpc = stub->AsyncEcho(&client_ctx, client_request, cq.get());
    rpc->Finish(&client_response, &status, (void*)2);
    printf("AsyncEcho called with tag 2\n");
    
    // Wait for completions
    void* tag;
    bool ok;
    
    printf("Waiting for first completion...\n");
    if (cq->Next(&tag, &ok)) {
        printf("Got completion: tag=%ld, ok=%s\n", 
               (long)tag, ok ? "true" : "false");
        
        if (tag == (void*)1) {
            printf("Server RequestEcho completed - request arrived!\n");
            printf("Request message: '%s'\n", server_request.message().c_str());
            
            // Send response
            EchoResponse response;
            response.set_message("Server response: " + server_request.message());
            responder.Finish(response, Status::OK, (void*)3);
            printf("Response sent with tag 3\n");
        } else if (tag == (void*)2) {
            printf("Client AsyncEcho completed - got response!\n");
            printf("Response message: '%s'\n", client_response.message().c_str());
        } else {
            printf("Unexpected tag!\n");
        }
    }
    
    printf("Waiting for more completions...\n");
    while (cq->Next(&tag, &ok)) {
        printf("Got completion: tag=%ld, ok=%s\n", 
               (long)tag, ok ? "true" : "false");
        
        if (tag == (void*)2) {
            printf("Client AsyncEcho completed - got response!\n");
            printf("Response message: '%s'\n", client_response.message().c_str());
            break;
        } else if (tag == (void*)3) {
            printf("Server response send completed\n");
        }
    }
    
    printf("Test completed\n");
    server->Shutdown();
    cq->Shutdown();
    
    return 0;
}
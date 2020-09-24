#include "extensions/grpc_stream_demuxer/grpc_stream_demuxer.h"

namespace Envoy {
namespace Extensions {
namespace GrpcStreamDemuxer {

ReceivedMessageServiceClient::ReceivedMessageServiceClient(std::shared_ptr<Channel> channel)
  : stub_(ReceivedMessageService::NewStub(channel)) {}

// Assembles the client's payload, sends it and presents the response back
// from the server.
Envoy::StatusOr<std::string> ReceivedMessageServiceClient::SendReceivedMessage(const ReceivedMessage &request) {
  Ack reply;
  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context;

  // The actual RPC.
  Status status = stub_->SendReceivedMessage(&context, request, &reply);

  // Act upon its status.
  if (status.ok()) {
    return reply.ack_id();
  } else {
    std::cout << status.error_code() << ": " << status.error_message() << std::endl;
    return absl::Status(absl::StatusCode::kAborted, status.error_message());
  }
}
	
GrpcStreamDemuxer::GrpcStreamDemuxer(const std::string& subscription, const std::string& address, int port, Event::Dispatcher& dispatcher) 
  : subscription_(subscription), address_(address), port_(port) {   	
  interval_timer_ = dispatcher.createTimer([this]() -> void { start(); });
  interval_timer_->enableTimer(std::chrono::milliseconds(5000));
}

void GrpcStreamDemuxer::start() {
  auto creds = grpc::GoogleDefaultCredentials();
  auto stub = Subscriber::NewStub(
    grpc::CreateChannel("pubsub.googleapis.com", creds));      

  // Open up the stream.
  // CompletionQueue cq;
  ClientContext ctx;
  unsigned int client_connection_timeout = 2;
  std::chrono::system_clock::time_point deadline = 
	  std::chrono::system_clock::now() + std::chrono::seconds(client_connection_timeout);
  ctx.set_deadline(deadline);
  std::unique_ptr<ClientReaderWriter
    <StreamingPullRequest, StreamingPullResponse>> 
      stream(stub->StreamingPull(&ctx));
  
  // Send initial message.
  StreamingPullRequest request;
  request.set_subscription(subscription_);
  request.set_stream_ack_deadline_seconds(10);
  stream->Write(request);
  // Receive messages.
  StreamingPullResponse response;
  while (stream->Read(&response)) {
    // Ack messages.
    StreamingPullRequest ack_request;
    for (const auto &message : response.received_messages()) {
      // Print the data from the message.
      ENVOY_LOG(info, "Pubsub message data: {}", message.message().data());
      // Send the message using a unary grpc request.
      std::string target_uri = address_ + ":" + std::to_string(port_);
      ReceivedMessageServiceClient client(grpc::CreateChannel(target_uri, grpc::InsecureChannelCredentials()));
      Envoy::StatusOr<std::string> reply = client.SendReceivedMessage(message);
      if (reply.ok()){
        ack_request.add_ack_ids(reply.value());
      } else {
        ENVOY_LOG(info, "Unary request response: {}", reply.status());
      }
    }
    stream->Write(ack_request);
  }  
  ENVOY_LOG(debug, "Address: {}", address_);
  ENVOY_LOG(debug, "Port: {}", port_);
  interval_timer_->enableTimer(std::chrono::milliseconds(5000));
}

} // namespace GrpcStreamDemuxer
} // namespace Extensions
} // namespace Envoy

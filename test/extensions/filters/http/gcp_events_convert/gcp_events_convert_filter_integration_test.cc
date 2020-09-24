#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.h"
#include "envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.pb.h"

#include "common/protobuf/utility.h"

#include "test/integration/http_integration.h"
#include "test/integration/http_protocol_integration.h"

#include "google/pubsub/v1/pubsub.pb.h"

namespace Envoy {
namespace {

using google::pubsub::v1::PubsubMessage;
using google::pubsub::v1::ReceivedMessage;

/**
 * Tests a downstream client sendig HTTP requests that containing Cloud Event in PubsubMessage binding
 * that are converted to Cloud Event in HTTP binding. Both Binding is binary format. 
 */
class GcpEventsConvertIntegrationTest : public HttpIntegrationTest,
                                        public testing::TestWithParam<Network::Address::IpVersion> {
public:
  GcpEventsConvertIntegrationTest()
      : HttpIntegrationTest(Http::CodecClient::Type::HTTP1, GetParam()) {}
  /**
   * Initializer for an individual integration test.
   */
  void SetUp() override { initialize(); }

  void initialize() override {
    config_helper_.addFilter(
        "{ name: envoy.filters.http.gcp_events_convert , typed_config: { \"@type\": "
        "type.googleapis.com/envoy.extensions.filters.http.gcp_events_convert.v3.GcpEventsConvert, "
        "content_type: application/grpc+cloudevent+json} }");
    HttpIntegrationTest::initialize();
  }
};

INSTANTIATE_TEST_SUITE_P(IpVersions, GcpEventsConvertIntegrationTest,
                         testing::ValuesIn(TestEnvironment::getIpVersionsForTest()));

/**
 * Normal cases that will convert Pubsub Binding to HTTP Binding
 */
TEST_P(GcpEventsConvertIntegrationTest, CloudEventNormalRequest) {
  Http::TestRequestHeaderMapImpl headers{{":method", "POST"},
                                         {":path", "/"},
                                         {":authority", "host"},
                                         {"content-type", "application/grpc+cloudevent+json"}};

  IntegrationCodecClientPtr codec_client;
  FakeHttpConnectionPtr fake_upstream_connection;
  FakeStreamPtr request_stream;

  codec_client = makeHttpConnection(lookupPort("http"));
  auto encoder_decoder = codec_client->startRequest(headers);
  request_encoder_ = &encoder_decoder.first;
  IntegrationStreamDecoderPtr response = std::move(encoder_decoder.second);

  // create a received message proto object
  ReceivedMessage received_message;
  received_message.set_ack_id("random ack id");
  received_message.set_delivery_attempt(3);
  PubsubMessage& pubsub_message = *received_message.mutable_message();
  google::protobuf::Map<std::string, std::string>& attributes =
      *pubsub_message.mutable_attributes();
  attributes["ce-specversion"] = "1.0";
  attributes["ce-type"] = "com.example.some_event";
  attributes["ce-time"] = "2020-03-10T03:56:24Z";
  attributes["ce-id"] = "1234-1234-1234";
  attributes["ce-source"] = "/mycontext/subcontext";
  attributes["ce-datacontenttype"] = "application/text; charset=utf-8";
  pubsub_message.set_data("cloud event data payload");

  // create a proto string of received message
  std::string proto_string;
  bool status = received_message.SerializeToString(&proto_string);
  ASSERT_TRUE(status);

  // send proto string in multilple bodies @end_stream = false
  for (size_t index = 0; index < proto_string.size(); index += 10) {
    size_t length = (proto_string.size() - index) < 10 ? (proto_string.size() - index) : 10;
    Buffer::OwnedImpl data(proto_string.substr(index, length));
    codec_client->sendData(*request_encoder_, data, false);
  }
  // send the last piece of proto string  @end_stream = true
  Buffer::OwnedImpl data;
  codec_client->sendData(*request_encoder_, data, true);

  ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection));
  ASSERT_TRUE(fake_upstream_connection->waitForNewStream(*dispatcher_, request_stream));
  ASSERT_TRUE(request_stream->waitForEndStream(*dispatcher_));
  response->waitForEndStream();
  // filter should replace body with given string
  EXPECT_EQ("cloud event data payload", request_stream->body().toString());
  auto& request_headers = request_stream->headers();
  // filter should replace headers content-type with `ce-datecontenttype`
  EXPECT_EQ("application/text; charset=utf-8", request_headers.getContentTypeValue());
  // filter should insert ce attribute into header (except for `ce-datacontenttype`)
  EXPECT_THAT(request_headers.get(Http::LowerCaseString("ce-datacontenttype")), testing::IsNull());
  EXPECT_EQ("1.0",
            request_headers.get(Http::LowerCaseString("ce-specversion"))->value().getStringView());
  EXPECT_EQ("com.example.some_event",
            request_headers.get(Http::LowerCaseString("ce-type"))->value().getStringView());
  EXPECT_EQ("2020-03-10T03:56:24Z",
            request_headers.get(Http::LowerCaseString("ce-time"))->value().getStringView());
  EXPECT_EQ("1234-1234-1234",
            request_headers.get(Http::LowerCaseString("ce-id"))->value().getStringView());
  EXPECT_EQ("/mycontext/subcontext",
            request_headers.get(Http::LowerCaseString("ce-source"))->value().getStringView());
  codec_client->close();
}

/**
 * Edges cases that miss part of body, pass through
 */
TEST_P(GcpEventsConvertIntegrationTest, CloudEventPartialMissingRequest) {
  Http::TestRequestHeaderMapImpl headers{{":method", "POST"},
                                         {":path", "/"},
                                         {":authority", "host"},
                                         {"content-type", "application/grpc+cloudevent+json"}};

  IntegrationCodecClientPtr codec_client;
  FakeHttpConnectionPtr fake_upstream_connection;
  FakeStreamPtr request_stream;

  codec_client = makeHttpConnection(lookupPort("http"));
  auto encoder_decoder = codec_client->startRequest(headers);
  request_encoder_ = &encoder_decoder.first;
  IntegrationStreamDecoderPtr response = std::move(encoder_decoder.second);

  // create a received message proto object
  ReceivedMessage received_message;
  received_message.set_ack_id("random ack id");
  received_message.set_delivery_attempt(3);
  PubsubMessage& pubsub_message = *received_message.mutable_message();
  google::protobuf::Map<std::string, std::string>& attributes =
      *pubsub_message.mutable_attributes();
  attributes["ce-specversion"] = "1.0";
  attributes["ce-type"] = "com.example.some_event";
  attributes["ce-time"] = "2020-03-10T03:56:24Z";
  attributes["ce-id"] = "1234-1234-1234";
  attributes["ce-source"] = "/mycontext/subcontext";
  attributes["ce-datacontenttype"] = "application/text; charset=utf-8";
  pubsub_message.set_data("cloud event data payload");

  // create a proto string of received message
  std::string full_proto_string;
  bool status = received_message.SerializeToString(&full_proto_string);
  ASSERT_TRUE(status);

  // another string missing the last 10 characters
  std::string partial_proto_string = full_proto_string.substr(0, full_proto_string.size() - 10);

  // send proto string in multilple bodies @end_stream = false
  for (size_t index = 0; index < partial_proto_string.size(); index += 10) {
    size_t length =
        (partial_proto_string.size() - index) < 10 ? (partial_proto_string.size() - index) : 10;
    Buffer::OwnedImpl data(partial_proto_string.substr(index, length));
    codec_client->sendData(*request_encoder_, data, false);
  }
  // send the last piece of proto string  @end_stream = true
  Buffer::OwnedImpl data;
  codec_client->sendData(*request_encoder_, data, true);

  ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection));
  ASSERT_TRUE(fake_upstream_connection->waitForNewStream(*dispatcher_, request_stream));
  ASSERT_TRUE(request_stream->waitForEndStream(*dispatcher_));
  response->waitForEndStream();
  // filter should be pass through since filter can not convert partial proto string to proto object
  EXPECT_EQ(request_stream->body().toString(), partial_proto_string);
  codec_client->close();
}

/**
 * Unrelated cases, pass through
 */
TEST_P(GcpEventsConvertIntegrationTest, RandomRequest) {
  Http::TestRequestHeaderMapImpl headers{{":method", "POST"},
                                         {":path", "/"},
                                         {":authority", "host"},
                                         {"content-type", "application/text"}};

  IntegrationCodecClientPtr codec_client;
  FakeHttpConnectionPtr fake_upstream_connection;
  FakeStreamPtr request_stream;

  codec_client = makeHttpConnection(lookupPort("http"));
  auto encoder_decoder = codec_client->startRequest(headers);
  request_encoder_ = &encoder_decoder.first;
  IntegrationStreamDecoderPtr response = std::move(encoder_decoder.second);

  Buffer::OwnedImpl data1("hello ");
  codec_client->sendData(*request_encoder_, data1, false);

  Buffer::OwnedImpl data2(" world!");
  codec_client->sendData(*request_encoder_, data2, false);

  Buffer::OwnedImpl data3;
  codec_client->sendData(*request_encoder_, data3, true);

  ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection));
  ASSERT_TRUE(fake_upstream_connection->waitForNewStream(*dispatcher_, request_stream));
  ASSERT_TRUE(request_stream->waitForEndStream(*dispatcher_));
  response->waitForEndStream();
  // filter should be pass through
  EXPECT_EQ(request_stream->body().toString(), "hello  world!");
  codec_client->close();
}

/**
 * Unrelated cases, pass through
 */
TEST_P(GcpEventsConvertIntegrationTest, HeaderOnlyRequest) {
  Http::TestRequestHeaderMapImpl headers{{":method", "GET"},
                                         {":path", "/"},
                                         {":authority", "host"}};

  IntegrationCodecClientPtr codec_client;
  FakeHttpConnectionPtr fake_upstream_connection;
  FakeStreamPtr request_stream;

  codec_client = makeHttpConnection(lookupPort("http"));
  auto response = codec_client->makeHeaderOnlyRequest(headers);

  ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection));
  ASSERT_TRUE(fake_upstream_connection->waitForNewStream(*dispatcher_, request_stream));
  ASSERT_TRUE(request_stream->waitForEndStream(*dispatcher_));

  EXPECT_EQ(request_stream->body().toString(), "");
  response->waitForEndStream();
  codec_client->close();
}

} // namespace
} // namespace Envoy

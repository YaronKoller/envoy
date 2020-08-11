#include <memory>

#include "envoy/event/dispatcher.h"
#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.h"

#include "common/http/header_map_impl.h"
#include "common/protobuf/utility.h"
#include "common/runtime/runtime_impl.h"

#include "google/pubsub/v1/pubsub.pb.h"

#include "extensions/filters/http/gcp_events_convert/gcp_events_convert_filter.h"
#include "extensions/filters/http/well_known_names.h"

#include "test/mocks/buffer/mocks.h"
#include "test/mocks/http/mocks.h"
#include "test/test_common/printers.h"
#include "test/test_common/test_runtime.h"

#include "gmock/gmock.h"
#include "google/pubsub/v1/pubsub.pb.h"
#include "gtest/gtest.h"

using google::pubsub::v1::PubsubMessage;
using google::pubsub::v1::ReceivedMessage;
using testing::NiceMock;

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GcpEventsConvert {

// Unit test for Decode Headers
TEST(GcpEventsConvertFilterUnitTest, DecoderHeaderWithCloudEventBody) {
  envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert config;
  config.set_content_type("application/grpc+cloudevent+json");
  GcpEventsConvertFilter filter(std::make_shared<GcpEventsConvertFilterConfig>(config));
  Http::TestRequestHeaderMapImpl headers(
      {{"content-type", "application/grpc+cloudevent+json"}, {"content-length", "100"}});

  EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter.decodeHeaders(headers, false));
}

TEST(GcpEventsConvertFilterUnitTest, DecodeHeaderWithCloudEventNoBody) {
  envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert config;
  config.set_content_type("application/grpc+cloudevent+json");
  GcpEventsConvertFilter filter(std::make_shared<GcpEventsConvertFilterConfig>(config));
  Http::TestRequestHeaderMapImpl headers(
      {{"content-type", "application/grpc+cloudevent+json"}, {"content-length", "0"}});

  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter.decodeHeaders(headers, true));
}

TEST(GcpEventsConvertFilterUnitTest, DecodeHeaderWithRandomContent) {
  envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert config;
  config.set_content_type("application/grpc+cloudevent+json");
  GcpEventsConvertFilter filter(std::make_shared<GcpEventsConvertFilterConfig>(config));
  Http::TestRequestHeaderMapImpl headers(
      {{"content-type", "application/json"}, {"content-length", "100"}});

  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter.decodeHeaders(headers, false));
}

// Unit test for Decode Data
TEST(GcpEventsConvertFilterUnitTest, DecodeDataWithCloudEvent) {
  envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert proto_config;
  proto_config.set_content_type("application/grpc+cloudevent+json");
  GcpEventsConvertFilter filter(std::make_shared<GcpEventsConvertFilterConfig>(proto_config), true);
  NiceMock<Http::MockStreamDecoderFilterCallbacks> callbacks;
  filter.setDecoderFilterCallbacks(callbacks);

  // create a received message proto object
  ReceivedMessage received_message;
  received_message.set_ack_id("random ack id");
  received_message.set_delivery_attempt(3);
  PubsubMessage& pubsub_message = *received_message.mutable_message();
  auto attributes = *pubsub_message.mutable_attributes();
  attributes["ce-specversion"] = "1.0";
  attributes["ce-type"] = "com.example.some_event";
  attributes["ce-time"] = "2020-03-10T03:56:24Z";
  attributes["ce-id"] = "1234-1234-1234";
  attributes["ce-source"] = "/mycontext/subcontext";
  attributes["ce-datacontenttype"] = "application/text; charset=utf-8";
  pubsub_message.set_data("cloud event data payload");
  pubsub_message.set_message_id("136969346945");
  pubsub_message.mutable_publish_time()->ParseFromString("2014-10-02T15:01:23Z");
  pubsub_message.set_ordering_key("");

  // create a json string of received message
  std::string json_string;
  auto status = Envoy::ProtobufUtil::MessageToJsonString(received_message, &json_string);
  ASSERT_TRUE(status.ok());

  // separate string into multilple decodeData call
  for (size_t index = 0; index < json_string.size(); index += 10) {
    size_t length = (json_string.size() - index) < 10 ? (json_string.size() - index) : 10;
    Buffer::OwnedImpl data(json_string.substr(index, length));
    EXPECT_EQ(Http::FilterDataStatus::StopIterationAndBuffer, filter.decodeData(data, false));
  }

  EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter.decodeHeaders(headers, false));
}

TEST(GcpEventsConvertFilterUnitTest, DecodeDataWithRandomBody) {
  envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert proto_config;
  proto_config.set_content_type("application/grpc+cloudevent+json");
  GcpEventsConvertFilter filter(std::make_shared<GcpEventsConvertFilterConfig>(proto_config));
  NiceMock<Http::MockStreamDecoderFilterCallbacks> callbacks;
  filter.setDecoderFilterCallbacks(callbacks);

  Buffer::OwnedImpl data1("Hello");
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter.decodeData(data1, false));

  Buffer::OwnedImpl data2;
  EXPECT_EQ(Http::FilterDataStatus::Continue, filter.decodeData(data2, true));
}

} // namespace GcpEventsConvert
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy

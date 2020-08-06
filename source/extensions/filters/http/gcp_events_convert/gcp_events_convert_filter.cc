#include "extensions/filters/http/gcp_events_convert/gcp_events_convert_filter.h"

#include <string>

#include "envoy/common/exception.h"
#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.h"
#include "envoy/http/filter.h"
#include "envoy/server/filter_config.h"

#include "common/buffer/buffer_impl.h"
#include "common/common/cleanup.h"
#include "common/common/enum_to_int.h"
#include "common/common/utility.h"
#include "common/grpc/common.h"
#include "common/http/codes.h"
#include "common/http/utility.h"
#include "common/protobuf/utility.h"

#include "google/pubsub/v1/pubsub.pb.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GcpEventsConvert {

using google::pubsub::v1::PubsubMessage;
using google::pubsub::v1::ReceivedMessage;

GcpEventsConvertFilterConfig::GcpEventsConvertFilterConfig(
    const envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert& proto_config)
    : content_type_(proto_config.content_type()) {}

GcpEventsConvertFilter::GcpEventsConvertFilter(GcpEventsConvertFilterConfigSharedPtr config)
    : has_cloud_event_(false), config_(config) {}

GcpEventsConvertFilter::GcpEventsConvertFilter(GcpEventsConvertFilterConfigSharedPtr config, bool has_cloud_event)
    : has_cloud_event_(has_cloud_event), config_(config) {}

void GcpEventsConvertFilter::onDestroy() {}

Http::FilterHeadersStatus GcpEventsConvertFilter::decodeHeaders(Http::RequestHeaderMap& headers,
                                                                bool end_stream) {
  if (end_stream || !isCloudEvent(headers)) {
    // if this is a header-only request or it's not a request containing cloud event
    // we don't need to do any buffering
    return Http::FilterHeadersStatus::Continue;
  }

  has_cloud_event_ = true;
  // store the current header for future usage
  request_headers_ = &headers;
  return Http::FilterHeadersStatus::StopIteration;
}

<<<<<<< HEAD
Http::FilterDataStatus GcpEventsConvertFilter::decodeData(Buffer::Instance&, bool end_stream) {
  // for any requst body that is not related to cloud event. Pass through
  if (!has_cloud_event_)
    return Http::FilterDataStatus::Continue;

  // For any request body that is not the end of HTTP request and not empty
  // Buffer the current HTTP request's body
  if (!end_stream) 
    return Http::FilterDataStatus::StopIterationAndBuffer;

  if (decoder_callbacks_ == nullptr) {
    return Http::FilterDataStatus::Continue;
  }

  const Buffer::Instance* buffered = decoder_callbacks_->decodingBuffer();

  if (buffered == nullptr) {
    // nothing got buffered, Continue
    return Http::FilterDataStatus::Continue;
  }

  ReceivedMessage received_message;
  Envoy::ProtobufUtil::JsonParseOptions parse_option;
  auto status = Envoy::ProtobufUtil::JsonStringToMessage(buffered->toString(), &received_message,
                                                          parse_option);

  if (!status.ok()) {
    // buffered data didn't successfully converted to proto. Continue
    ENVOY_LOG(warn, "Gcp Events Convert Filter log: fail to convert from body to proto object");
    return Http::FilterDataStatus::Continue;
  }

  // TODO(#3): Use Cloud Event SDK to convert Pubsub Message to HTTP Binding
  absl::Status update_status = updateHeader();
  if (!update_status.ok()) {
    ENVOY_LOG(warn, "Gcp Events Convert Filter log: update header {}", update_status.ToString());
    return Http::FilterDataStatus::Continue;
  }

  update_status = updateBody();
  if (!update_status.ok()) {
    ENVOY_LOG(warn, "Gcp Events Convert Filter log: update body {}", update_status.ToString());
    return Http::FilterDataStatus::Continue;
  }

  return Http::FilterDataStatus::Continue;
=======
Http::FilterHeadersStatus GcpEventsConvertFilter::decodeHeaders(Http::RequestHeaderMap& headers, bool end_stream) {
  if (end_stream || !isCloudEvent(headers)) {
    // if this is a header-only request or it's not a request containing cloud event
    // we don't need to do any buffering
    return Http::FilterHeadersStatus::Continue;
  }

  has_cloud_event_ = true;
  // store the current header for future usage
  request_headers_ = &headers;
  return Http::FilterHeadersStatus::StopIteration;
}

Http::FilterDataStatus GcpEventsConvertFilter::decodeData(Buffer::Instance&, bool end_stream) {
  // for any requst body that is not related to cloud event. Pass through
  if (!has_cloud_event_) return Http::FilterDataStatus::Continue;

  // wait for all the body has arrived.
  if (end_stream) {
    const Buffer::Instance* buffered = decoder_callbacks_->decodingBuffer();
    
    if (buffered == nullptr) {
      // nothing got buffered, Continue
      return Http::FilterDataStatus::Continue;
    }

    ReceivedMessage received_message;
    Envoy::ProtobufUtil::JsonParseOptions parse_option;
    auto status = Envoy::ProtobufUtil::JsonStringToMessage(buffered->toString(), &received_message, parse_option);
    
    if (!status.ok()) {
      // buffered data didn't successfully converted to proto. Continue
      ENVOY_LOG(
          debug, 
          "Gcp Events Convert Filter log: fail to convert from body to proto object");
      return Http::FilterDataStatus::Continue;
    }

    // const PubsubMessage& pubsub_message = received_message.message();

    // TODO(h9jiang): Use Cloud Event SDK to convert Pubsub Message to HTTP Binding
    absl::Status update_status = updateHeader();
    if (!update_status.ok()){
      // something wrong while update header. Continue
      ENVOY_LOG(
          debug, 
          "Gcp Events Convert Filter log: update header {}", 
          absl::StatusCodeToString(update_status));
      return Http::FilterDataStatus::Continue;
    }

    update_status = updateBody();
    if (!update_status.ok()){
      // something wrong while rewrite the body. Continue
      ENVOY_LOG(
          debug, 
          "Gcp Events Convert Filter log: update body {}", 
          absl::StatusCodeToString(update_status));
      return Http::FilterDataStatus::Continue;
    }

    ENVOY_LOG(
        debug,
        "after rewrite the buffered data : {}", 
        decoder_callbacks_->decodingBuffer()->toString());
    return Http::FilterDataStatus::Continue;
  }
  
  // for any request body that is not the end of HTTP request and not empty
  // Buffer the current HTTP request's body
  return Http::FilterDataStatus::StopIterationAndBuffer;
>>>>>>> 94456147a... rebase logic with master
}

Http::FilterTrailersStatus GcpEventsConvertFilter::decodeTrailers(Http::RequestTrailerMap&) {
  return Http::FilterTrailersStatus::Continue;
}

void GcpEventsConvertFilter::setDecoderFilterCallbacks(
    Http::StreamDecoderFilterCallbacks& callbacks) {
  decoder_callbacks_ = &callbacks;
}

<<<<<<< HEAD
bool GcpEventsConvertFilter::isCloudEvent(const Http::RequestHeaderMap& headers) const {
  return headers.getContentTypeValue() == config_->content_type_;
}

absl::Status GcpEventsConvertFilter::updateHeader() {
  // TODO(#3): implement detail logic for update Header
=======
bool GcpEventsConvertFilter::isCloudEvent(const Http::RequestHeaderMap& headers) {
  absl::string_view  content_type = headers.getContentTypeValue();
  return content_type == "application/grcp+json+cloudevent";
}

absl::Status GcpEventsConvertFilter::updateHeader() {
  request_headers_->addCopy(headerKey(), headerValue());
>>>>>>> 94456147a... rebase logic with master
  return absl::OkStatus();
}

absl::Status GcpEventsConvertFilter::updateBody() {
<<<<<<< HEAD
  decoder_callbacks_->modifyDecodingBuffer([](Buffer::Instance& buffered) {
    // TODO(#3): implement detail logic for update Body
    // drain the current buffered instance
    buffered.drain(buffered.length());
    // replace the current buffered instance with the new body
    buffered.add("This is a example body");
  });
  return absl::OkStatus();
=======
    decoder_callbacks_->modifyDecodingBuffer([](Buffer::Instance& buffered) {
      // create a new buffer instance  
      Buffer::OwnedImpl new_buffer;
      new_buffer.add("This is a example body");
      // drain the current buffered instance
      buffered.drain(buffered.length());
      // replace the current buffered instance with buffer instance just created
      buffered.move(new_buffer);
    });
    return absl::OkStatus();
>>>>>>> 94456147a... rebase logic with master
}

} // namespace GcpEventsConvert
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy

#pragma once

#include <boost/beast/http.hpp>
#include <memory>
#include <string>

#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.h"
#include "envoy/server/filter_config.h"

#include "common/common/logger.h"

#include "extensions/filters/http/common/pass_through_filter.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GcpEventsConvert {

struct GcpEventsConvertFilterConfig : public Router::RouteSpecificFilterConfig {
  GcpEventsConvertFilterConfig(
      const envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert&
          proto_config);

  const std::string content_type_;
};

using GcpEventsConvertFilterConfigSharedPtr = std::shared_ptr<GcpEventsConvertFilterConfig>;

/**
 * The filter instance for convert Cloud Event Pubsub Binding to HTTP binding
 */
class GcpEventsConvertFilter : public Envoy::Http::PassThroughFilter,
                               public Logger::Loggable<Logger::Id::filter> {
public:
  // normal constructor
  GcpEventsConvertFilter(GcpEventsConvertFilterConfigSharedPtr config);
  // special constructor for Unit Test ONLY
  GcpEventsConvertFilter(GcpEventsConvertFilterConfigSharedPtr config, 
                         bool has_cloud_event,
                         Http::RequestHeaderMap* headers);
  GcpEventsConvertFilter(GcpEventsConvertFilterConfigSharedPtr config, 
                         bool has_cloud_event,
                         std::string ack_id,
                         bool acked);
  // Http::StreamFilterBase
  void onDestroy() override;

  // Http::StreamDecoderFilter
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers,
                                          bool end_stream) override;
  Http::FilterDataStatus decodeData(Buffer::Instance& data, bool end_stream) override;
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap& trailers) override;
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override;

  // Http::StreamEncoderFilter
  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers,
                                          bool end_stream) override;
  Http::FilterDataStatus encodeData(Buffer::Instance& buffer, bool end_stream) override;
  Http::FilterTrailersStatus encodeTrailers(Http::ResponseTrailerMap& trailers) override;
  void setEncoderFilterCallbacks(Http::StreamEncoderFilterCallbacks& callbacks) override;

private:
  using HttpRequest = boost::beast::http::request<boost::beast::http::string_body>;

  // build body with both buffered data and the incoming buffer data
  std::string buildBody(const Buffer::Instance* buffered, const Buffer::Instance& last);

  bool isCloudEvent(const Http::RequestHeaderMap& headers) const;

  // modify the data of HTTP request
  // 1. drain buffered data
  // 2. write cloud event data
  void updateBody(const HttpRequest& http_req, Buffer::Instance& buffer);

  // modify the header of HTTP request
  // 1. replace header's content type with ce-datacontenttype
  // 2. add cloud event information, ce-version, ce-type...... (except ce's data)
  void updateHeader(const HttpRequest& request);

  std::string ack_id_;
  bool acked_ = false;
  Http::RequestHeaderMap* request_headers_ = nullptr;
  bool has_cloud_event_ = false;
  const GcpEventsConvertFilterConfigSharedPtr config_;
  Http::StreamEncoderFilterCallbacks* encoder_callbacks_ = nullptr;
  Http::StreamDecoderFilterCallbacks* decoder_callbacks_ = nullptr;
};

} // namespace GcpEventsConvert
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy

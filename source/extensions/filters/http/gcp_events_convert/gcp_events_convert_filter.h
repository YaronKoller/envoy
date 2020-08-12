#pragma once

#include <string>

#include "envoy/server/filter_config.h"

#include "common/common/logger.h"

#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GcpEventsConvert {

struct GcpEventsConvertFilterConfig {
  GcpEventsConvertFilterConfig(const envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert& proto_config);

  const std::string key_;
  const std::string val_;
};

using GcpEventsConvertFilterConfigSharedPtr = std::shared_ptr<GcpEventsConvertFilterConfig>;

/**
 * The filter instance for convert Cloud Event Pubsub Binding to HTTP binding
 */
class GcpEventsConvertFilter : public Http::StreamDecoderFilter, public Logger::Loggable<Logger::Id::filter> {
public:
  GcpEventsConvertFilter(GcpEventsConvertFilterConfigSharedPtr config);

  // Http::StreamFilterBase
  void onDestroy() override;

  // Http::StreamDecoderFilter
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers, 
                                          bool end_stream) override;
  Http::FilterDataStatus decodeData(Buffer::Instance& data, bool end_stream) override;
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap& trailers) override;
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override;

private:
  const Http::LowerCaseString headerKey() const;
  const std::string headerValue() const;
  static bool isCloudEvent(const Http::RequestHeaderMap&);

  // modify the data of HTTP request
  // 1. drain buffered data
  // 2. write cloud event data
  absl::Status updateBody();

  // modify the header of HTTP request
  // 1. replace header's content type with ce-datacontenttype
  // 2. add cloud event information, ce-version, ce-type...... (except ce's data)
  // 3. [TBD] add Ack ID into header 
  absl::Status updateHeader();
  
  Http::RequestHeaderMap* request_headers_ = nullptr;
  bool has_cloud_event_ = false;
  const GcpEventsConvertFilterConfigSharedPtr config_;
  Http::StreamDecoderFilterCallbacks* decoder_callbacks_ = nullptr;
};

} // namespace GcpEventsConvert
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
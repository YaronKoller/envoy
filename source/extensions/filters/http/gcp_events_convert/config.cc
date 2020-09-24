#include "extensions/filters/http/gcp_events_convert/config.h"

#include <memory>
#include <string>

#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.h"
#include "envoy/extensions/filters/http/gcp_events_convert/v3/gcp_events_convert.pb.validate.h"
#include "envoy/registry/registry.h"

#include "common/protobuf/utility.h"

#include "extensions/filters/http/gcp_events_convert/gcp_events_convert_filter.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace GcpEventsConvert {

Http::FilterFactoryCb GcpEventsConvertFilterFactory::createFilterFactoryFromProtoTyped(
    const envoy::extensions::filters::http::gcp_events_convert::v3::GcpEventsConvert& proto_config,
    const std::string&, Server::Configuration::FactoryContext&) {
  GcpEventsConvertFilterConfigSharedPtr config =
      std::make_shared<GcpEventsConvertFilterConfig>(proto_config);

  return [config](Http::FilterChainFactoryCallbacks& callbacks) -> void {
    callbacks.addStreamFilter(std::make_shared<GcpEventsConvertFilter>(config));
  };
}

/**
 * Static registration for the Gcp Events Convert filter. @see RegisterFactory.
 */
REGISTER_FACTORY(GcpEventsConvertFilterFactory,
                 Server::Configuration::NamedHttpFilterConfigFactory){"envoy.gcp_events_convert"};

} // namespace GcpEventsConvert
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy

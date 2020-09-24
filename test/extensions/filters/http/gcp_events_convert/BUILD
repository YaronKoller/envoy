load(
    "//bazel:envoy_build_system.bzl",
    "envoy_package",
)
load(
    "//test/extensions:extensions_build_system.bzl",
    "envoy_extension_cc_test",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_extension_cc_test(
    name = "gcp_events_convert_filter_test",
    srcs = ["gcp_events_convert_filter_test.cc"],
    extension_name = "envoy.filters.http.gcp_events_convert",
    deps = [
        "//include/envoy/event:dispatcher_interface",
        "//source/common/http:header_map_lib",
        "//source/common/stats:stats_lib",
        "//source/extensions/filters/http/gcp_events_convert:gcp_events_convert_filter_lib",
        "//test/mocks/buffer:buffer_mocks",
        "//test/mocks/http:http_mocks",
        "//test/mocks/init:init_mocks",
        "//test/mocks/local_info:local_info_mocks",
        "//test/mocks/protobuf:protobuf_mocks",
        "//test/mocks/thread_local:thread_local_mocks",
        "//test/mocks/upstream:upstream_mocks",
        "//test/test_common:test_runtime_lib",
        "@com_google_googleapis//google/pubsub/v1:pubsub_cc_proto",
        "@envoy_api//envoy/extensions/filters/http/gcp_events_convert/v3:pkg_cc_proto",
        "@envoy_api//envoy/service/pubsub/v3alpha:pkg_cc_grpc",
    ],
)

envoy_extension_cc_test(
    name = "gcp_events_convert_filter_integration_test",
    srcs = ["gcp_events_convert_filter_integration_test.cc"],
    extension_name = "envoy.filters.http.gcp_events_convert",
    tags = ["fails_on_windows"],
    deps = [
        "//source/extensions/filters/http/buffer:config",
        "//source/extensions/filters/http/gcp_events_convert:config",
        "//test/integration:http_integration_lib",
        "//test/integration:http_protocol_integration_lib",
        "//test/test_common:utility_lib",
        "@com_google_googleapis//google/pubsub/v1:pubsub_cc_proto",
        "@envoy_api//envoy/extensions/filters/http/gcp_events_convert/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/filters/network/http_connection_manager/v3:pkg_cc_proto",
    ],
)

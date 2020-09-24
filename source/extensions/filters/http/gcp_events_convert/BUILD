load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
)

licenses(["notice"])  # Apache 2

envoy_extension_package()

envoy_cc_library(
    name = "gcp_events_convert_filter_lib",
    srcs = ["gcp_events_convert_filter.cc"],
    hdrs = ["gcp_events_convert_filter.h"],
    deps = [
        "//include/envoy/http:filter_interface",
        "//include/envoy/runtime:runtime_interface",
        "//source/common/common:assert_lib",
        "//source/common/common:cleanup_lib",
        "//source/common/http:codes_lib",
        "//source/common/http:header_map_lib",
        "//source/common/http:headers_lib",
        "//source/common/network:lc_trie_lib",
        "//source/common/runtime:runtime_lib",
        "//source/common/stats:symbol_table_lib",
        "//source/extensions/filters/http:well_known_names",
        "//source/extensions/filters/http/common:pass_through_filter_lib",
        "@boost//:beast",
        "@com_github_cloudevents_sdk//v1/protocol_binding:http_binder_lib",
        "@com_github_cloudevents_sdk//v1/protocol_binding:pubsub_binder_lib",
        "@com_google_googleapis//google/pubsub/v1:pubsub_cc_proto",
        "@envoy_api//envoy/extensions/filters/http/gcp_events_convert/v3:pkg_cc_proto",
        "@envoy_api//envoy/service/pubsub/v3alpha:pkg_cc_grpc",
    ],
)

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    security_posture = "robust_to_untrusted_downstream",
    deps = [
        "//include/envoy/registry",
        "//source/common/common:enum_to_int",
        "//source/common/protobuf:utility_lib",
        "//source/extensions/filters/http:well_known_names",
        "//source/extensions/filters/http/common:factory_base_lib",
        "//source/extensions/filters/http/gcp_events_convert:gcp_events_convert_filter_lib",
        "@envoy_api//envoy/extensions/filters/http/gcp_events_convert/v3:pkg_cc_proto",
    ],
)

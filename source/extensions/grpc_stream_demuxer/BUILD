load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_library(
    name = "grpc_stream_demuxer",
    srcs = ["grpc_stream_demuxer.cc"],
    hdrs = ["grpc_stream_demuxer.h"],
    deps = [
	    "//include/envoy/event:dispatcher_interface",
        "//source/common/common:minimal_logger_lib",
        "//source/common/common:statusor_lib",
        "@com_google_googleapis//google/pubsub/v1:pubsub_cc_grpc",
        "@com_github_grpc_grpc//:grpc++",
        "@envoy_api//envoy/service/pubsub/v3alpha:pkg_cc_grpc",
    ],
)

envoy_cc_library(
    name = "grpc_stream_demuxer_factory_lib",
    hdrs = ["grpc_stream_demuxer_factory.h"],
    deps = [
        ":grpc_stream_demuxer",
        "//include/envoy/config:typed_config_interface",
        "@envoy_api//envoy/extensions/grpc_stream_demuxer/v3alpha:pkg_cc_proto",
    ],
)

envoy_cc_extension(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    security_posture = "unknown",
    status = "wip",
    deps = [
        ":grpc_stream_demuxer_factory_lib",
        "//include/envoy/registry",
    ],
)

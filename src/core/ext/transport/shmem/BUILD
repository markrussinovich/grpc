load("//bazel:grpc_build_system.bzl", "grpc_cc_library")

package(default_visibility = ["//visibility:public"])

config_setting(
    name = "linux",
    constraint_values = ["@platforms//os:linux"],
)

grpc_cc_library(
    name = "shmem_transport",
    srcs = [
        "shmem_transport.cc",
        "shmem_framer.cc",
        "shmem_queue.cc",
        "shmem_segment.cc",
        "shmem_channel.cc",
        "shmem_legacy_transport.cc",
    ] + select({
        "//conditions:default": ["shmem_transport_stub.cc"],
        ":linux": [],
    }),
    hdrs = [
        "shmem_transport.h",
        "shmem_framer.h",
        "shmem_queue.h",
        "shmem_segment.h",
        "shmem_protocol.h",
        "shmem_memory.h",
        "shmem_legacy_transport.h",
        "shmem_semaphore.h",
        "shmem_lockfree_queue.h",
    ],
    copts = select({
        "//conditions:default": [],
        ":linux": ["-DGRPC_SHMEM_LINUX"],
    }),
    deps = [
        "//:gpr",
        "//src/core:grpc_base",
        "//src/core:grpc_transport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)


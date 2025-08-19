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
        "shmem_memory.cc",
        "shmem_resolver.cc",
        "shmem_plugin.cc",
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
        "shmem_semaphore.h",
        "shmem_lockfree_queue.h",
        "shmem_resolver.h",
    ],
    defines = select({
        "//conditions:default": [],
        ":linux": ["GRPC_SHMEM_LINUX"],
    }),
    deps = [
        "//:gpr",
        "//:grpc_base",
        "//src/core:slice",
        "//src/core:context",
        "//src/core:channel_args",
        "//src/core:memory_quota",
        "//src/core:direct_channel",
        "//src/core:grpc_transport_inproc",
        "//:uri",
        "//:server",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)


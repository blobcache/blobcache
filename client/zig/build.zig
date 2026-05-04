const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const blobcache_mod = b.addModule("blobcache", .{
        .root_source_file = b.path("lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_test_mod = b.createModule(.{
        .root_source_file = b.path("lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{
        .root_module = unit_test_mod,
    });
    const integration_test_mod = b.createModule(.{
        .root_source_file = b.path("tests/daemon_ephemeral.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_test_mod.addImport("blobcache", blobcache_mod);
    const integration_tests = b.addTest(.{
        .root_module = integration_test_mod,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_integration_tests.step);
}

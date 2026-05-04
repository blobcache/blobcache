const std = @import("std");
const blobcache = @import("blobcache");

var socket_counter: u64 = 0;

fn nextSocketPath(allocator: std.mem.Allocator) ![]u8 {
    socket_counter += 1;
    return std.fmt.allocPrint(allocator, "/tmp/blobcache-zig-itest-{d}.sock", .{socket_counter});
}

fn waitForSocket(io: std.Io, path: []const u8) !void {
    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        if (std.Io.Dir.cwd().access(io, path, .{})) |_| return else |_| {}
        try std.Io.sleep(io, std.Io.Duration.fromMilliseconds(50), .awake);
    }
    return error.TestTimeout;
}

test "daemon ephemeral create volume and edit transaction" {
    var da = std.heap.DebugAllocator(.{}){};
    defer _ = da.deinit();
    const allocator = da.allocator();
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const daemon_path = "../../build/out/blobcache";
    try std.Io.Dir.cwd().access(io, daemon_path, .{});

    const socket_path = try nextSocketPath(allocator);
    defer allocator.free(socket_path);

    std.Io.Dir.cwd().deleteFile(io, socket_path) catch {};

    const argv: []const []const u8 = &.{
        daemon_path,
        "daemon-ephemeral",
        "--serve-ipc",
        socket_path,
        "--net",
        "127.0.0.1:0",
    };
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    defer {
        child.kill(io);
        std.Io.Dir.cwd().deleteFile(io, socket_path) catch {};
    }

    try waitForSocket(io, socket_path);

    var client = try blobcache.UnixClient.init(allocator, socket_path);
    defer client.deinit();

    const volume_spec_json =
        \\{"local":{"schema":{"name":"","params":null},"hash_algo":"blake3-256","max_size":1048576,"salted":false}}
    ;
    const volume = try client.createVolume(io, null, volume_spec_json);

    const tx = try client.beginTx(io, volume, .{
        .modify = true,
        .gc_blobs = false,
        .gc_links = false,
    });

    try client.save(io, tx, "hello from zig");
    const loaded = try client.load(io, tx);
    defer allocator.free(loaded);
    try std.testing.expectEqualStrings("hello from zig", loaded);

    try client.commit(io, tx);
}

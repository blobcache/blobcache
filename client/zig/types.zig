const std = @import("std");
const builtin = @import("builtin");
const protocol = @import("protocol.zig");

pub const ENV_BLOBCACHE_API = "BLOBCACHE_API";
pub const ENV_BLOBCACHE_NS_ROOT = "BLOBCACHE_NS_ROOT";

pub const DEFAULT_ENDPOINT = switch (builtin.os.tag) {
    .macos => "unix:///var/run/blobcache/blobcache.sock",
    else => "unix:///run/blobcache/blobcache.sock",
};

pub const oid_size: usize = 16;
pub const cid_size: usize = 32;
pub const handle_size: usize = oid_size + 16;
pub const link_token_size: usize = 16 + 8 + 24;
pub const node_id_size: usize = 32;

pub const ActionSet = u64;

pub const OID = struct {
    bytes: [oid_size]u8,

    pub fn fromBytes(data: []const u8) !OID {
        if (data.len != oid_size) return error.InvalidOid;
        var out: [oid_size]u8 = undefined;
        @memcpy(&out, data[0..oid_size]);
        return .{ .bytes = out };
    }

    pub fn asBytes(self: *const OID) *const [oid_size]u8 {
        return &self.bytes;
    }
};

pub const CID = struct {
    bytes: [cid_size]u8,

    pub fn fromBytes(data: []const u8) !CID {
        if (data.len != cid_size) return error.InvalidCid;
        var out: [cid_size]u8 = undefined;
        @memcpy(&out, data[0..cid_size]);
        return .{ .bytes = out };
    }

    pub fn asBytes(self: *const CID) *const [cid_size]u8 {
        return &self.bytes;
    }
};

pub const NodeID = struct {
    bytes: [node_id_size]u8,
};

pub const Handle = struct {
    oid: OID,
    secret: [16]u8,

    pub fn marshal(self: *const Handle, out: *std.array_list.Managed(u8)) !void {
        try out.appendSlice(self.oid.asBytes());
        try out.appendSlice(&self.secret);
    }

    pub fn unmarshal(data: []const u8) !Handle {
        if (data.len < handle_size) return error.InvalidHandle;
        const oid = try OID.fromBytes(data[0..oid_size]);
        var secret: [16]u8 = undefined;
        @memcpy(&secret, data[oid_size..handle_size]);
        return .{ .oid = oid, .secret = secret };
    }
};

pub const LinkToken = struct {
    target: OID,
    rights: ActionSet,
    secret: [24]u8,

    pub fn marshal(self: *const LinkToken, out: *std.array_list.Managed(u8)) !void {
        try out.appendSlice(self.target.asBytes());
        try protocol.appendInt(out, u64, self.rights, .little);
        try out.appendSlice(&self.secret);
    }

    pub fn unmarshal(data: []const u8) !LinkToken {
        if (data.len < link_token_size) return error.InvalidLinkToken;
        const target = try OID.fromBytes(data[0..16]);
        const rights = std.mem.readInt(u64, data[16..24], .little);
        var secret: [24]u8 = undefined;
        @memcpy(&secret, data[24..48]);
        return .{ .target = target, .rights = rights, .secret = secret };
    }
};

pub const Endpoint = struct {
    node: NodeID,
    ip_port: []const u8,

    pub fn deinit(self: *Endpoint, allocator: std.mem.Allocator) void {
        allocator.free(self.ip_port);
        self.ip_port = "";
    }

    pub fn fromBcpBytes(allocator: std.mem.Allocator, data: []const u8) !Endpoint {
        if (data.len < node_id_size) return error.InvalidEndpoint;

        var peer: [node_id_size]u8 = undefined;
        @memcpy(&peer, data[0..node_id_size]);
        const locator = data[node_id_size..];

        const ip_port = try formatLocator(allocator, locator);
        return .{ .node = .{ .bytes = peer }, .ip_port = ip_port };
    }

    fn formatLocator(allocator: std.mem.Allocator, locator: []const u8) ![]const u8 {
        if (locator.len == 0) return allocator.dupe(u8, "");

        if (locator.len == 6) {
            const port = std.mem.readInt(u16, locator[4..6], .little);
            return std.fmt.allocPrint(allocator, "{d}.{d}.{d}.{d}:{d}", .{ locator[0], locator[1], locator[2], locator[3], port });
        }

        if (locator.len < 18) return error.InvalidEndpoint;
        const port = std.mem.readInt(u16, locator[locator.len - 2 ..], .little);
        const addr = locator[0 .. locator.len - 2];
        if (addr.len < 16) return error.InvalidEndpoint;

        var octets: [16]u8 = undefined;
        @memcpy(&octets, addr[0..16]);
        const ipv6 = std.net.Address.initIp6(octets, port, 0, 0);

        if (addr.len == 16) return std.fmt.allocPrint(allocator, "{any}", .{ipv6});

        const zone = addr[16..];
        const base = try std.fmt.allocPrint(allocator, "{any}", .{ipv6});
        defer allocator.free(base);
        return std.fmt.allocPrint(allocator, "{s}%{s}", .{ base, zone });
    }

    pub fn marshalBcp(self: *const Endpoint, out: *std.array_list.Managed(u8)) !void {
        try out.appendSlice(&self.node.bytes);
        if (self.ip_port.len == 0) return;

        const ip = try std.Io.net.IpAddress.parseLiteral(self.ip_port);
        switch (ip) {
            .ip4 => |ip4| {
                try out.appendSlice(&ip4.bytes);
                try protocol.appendInt(out, u16, ip4.port, .little);
            },
            .ip6 => |ip6| {
                try out.appendSlice(&ip6.bytes);
                try protocol.appendInt(out, u16, ip6.port, .little);
            },
        }
    }
};

pub const TxParams = struct {
    modify: bool = false,
    gc_blobs: bool = false,
    gc_links: bool = false,

    pub fn marshalBcp(self: TxParams, out: *std.array_list.Managed(u8)) !void {
        var flags: u32 = 0;
        if (self.modify) flags |= 1 << 0;
        if (self.gc_blobs) flags |= 1 << 1;
        if (self.gc_links) flags |= 1 << 2;
        try protocol.appendInt(out, u32, flags, .little);
    }
};

pub const PostOpts = struct { salt: ?CID = null };

pub const GetOpts = struct {
    salt: ?CID = null,
    skip_verify: bool = false,
};

pub const DequeueOpts = struct {
    min: u32,
    leave_in: bool,
    skip: u32,
    max_wait: ?i64,
};

pub const Message = struct {
    handles: []Handle,
    bytes: []u8,

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        allocator.free(self.handles);
        allocator.free(self.bytes);
    }

    pub fn marshal(self: *const Message, out: *std.array_list.Managed(u8)) !void {
        try protocol.appendInt(out, u32, @intCast(self.handles.len), .little);
        for (self.handles) |h| try h.marshal(out);
        try out.appendSlice(self.bytes);
    }

    pub fn unmarshal(allocator: std.mem.Allocator, data: []const u8) !Message {
        if (data.len < 4) return error.InvalidMessage;

        const count: usize = @intCast(std.mem.readInt(u32, data[0..4], .little));
        var idx: usize = 4;

        const handles = try allocator.alloc(Handle, count);
        errdefer allocator.free(handles);

        for (handles) |*h| {
            if (idx + handle_size > data.len) return error.InvalidMessage;
            h.* = try Handle.unmarshal(data[idx .. idx + handle_size]);
            idx += handle_size;
        }

        const bytes = try allocator.dupe(u8, data[idx..]);
        return .{ .handles = handles, .bytes = bytes };
    }
};

pub const InsertResp = struct {
    success: u32,
};

test "message marshal and unmarshal" {
    var da = std.heap.DebugAllocator(.{}){};
    defer _ = da.deinit();
    const allocator = da.allocator();

    const msg = Message{ .handles = &.{}, .bytes = @constCast("abc") };

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();
    try msg.marshal(&out);

    var parsed = try Message.unmarshal(allocator, out.items);
    defer parsed.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), parsed.handles.len);
    try std.testing.expectEqualStrings("abc", parsed.bytes);
}

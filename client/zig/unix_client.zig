const std = @import("std");
const protocol = @import("protocol.zig");
const types = @import("types.zig");

pub const UnixClient = struct {
    allocator: std.mem.Allocator,
    socket_path: []const u8,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !UnixClient {
        return .{
            .allocator = allocator,
            .socket_path = try allocator.dupe(u8, path),
        };
    }

    pub fn deinit(self: *UnixClient) void {
        self.allocator.free(self.socket_path);
    }

    fn ask(self: *const UnixClient, code: protocol.MessageCode, body: []const u8) ![]u8 {
        var conn = try std.net.connectUnixSocket(self.socket_path);
        defer conn.close();

        try protocol.writeMessage(conn.writer(), code, body);
        const resp = try protocol.readMessageAlloc(self.allocator, conn.reader());
        errdefer self.allocator.free(resp.body);

        try protocol.checkOk(resp.code);
        return resp.body;
    }

    fn askVoid(self: *const UnixClient, code: protocol.MessageCode, body: []const u8) !void {
        const resp = try self.ask(code, body);
        self.allocator.free(resp);
    }

    fn requestBuffer(self: *const UnixClient) std.array_list.Managed(u8) {
        return std.array_list.Managed(u8).init(self.allocator);
    }

    fn encodeHost(self: *const UnixClient, host: ?types.Endpoint) !std.array_list.Managed(u8) {
        var host_data = std.array_list.Managed(u8).init(self.allocator);
        errdefer host_data.deinit();

        if (host) |h| {
            try h.marshalBcp(&host_data);
        } else {
            try host_data.appendNTimes(0, types.node_id_size);
        }
        return host_data;
    }

    pub fn endpoint(self: *const UnixClient) !types.Endpoint {
        const body = try self.ask(protocol.mt_endpoint, "");
        defer self.allocator.free(body);
        return types.Endpoint.fromBcpBytes(self.allocator, body);
    }

    pub fn inspectHandle(self: *const UnixClient, handle: types.Handle) !struct {
        oid: types.OID,
        rights: types.ActionSet,
        created_at: [8]u8,
        expires_at: [8]u8,
    } {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);

        const body = try self.ask(protocol.mt_handle_inspect, req.items);
        defer self.allocator.free(body);
        if (body.len < 40) return error.InvalidMessage;

        var created_at: [8]u8 = undefined;
        var expires_at: [8]u8 = undefined;
        @memcpy(&created_at, body[24..32]);
        @memcpy(&expires_at, body[32..40]);

        return .{
            .oid = try types.OID.fromBytes(body[0..16]),
            .rights = std.mem.readInt(u64, body[16..24], .little),
            .created_at = created_at,
            .expires_at = expires_at,
        };
    }

    pub fn dropHandle(self: *const UnixClient, handle: types.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);
        try self.askVoid(protocol.mt_handle_drop, req.items);
    }

    pub fn keepAlive(self: *const UnixClient, handles: []const types.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        for (handles) |h| try h.marshal(&req);
        try self.askVoid(protocol.mt_handle_keep_alive, req.items);
    }

    pub fn shareOut(self: *const UnixClient, handle: types.Handle, to: types.NodeID, mask: types.ActionSet) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);
        try req.appendSlice(&to.bytes);
        try protocol.appendInt(&req, u64, mask, .big);

        const body = try self.ask(protocol.mt_handle_share_out, req.items);
        defer self.allocator.free(body);
        return types.Handle.unmarshal(body);
    }

    pub fn shareIn(self: *const UnixClient, host: types.NodeID, handle: types.Handle) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try req.appendSlice(&host.bytes);
        try handle.marshal(&req);

        const body = try self.ask(protocol.mt_handle_share_in, req.items);
        defer self.allocator.free(body);
        return types.Handle.unmarshal(body);
    }

    pub fn inspect(self: *const UnixClient, handle: types.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);

        const body = try self.ask(protocol.mt_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn openFiat(self: *const UnixClient, target: types.OID, mask: types.ActionSet) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try req.appendSlice(target.asBytes());
        try protocol.appendInt(&req, u64, mask, .big);

        const body = try self.ask(protocol.mt_open_fiat, req.items);
        defer self.allocator.free(body);
        if (body.len < types.handle_size) return error.InvalidMessage;
        return types.Handle.unmarshal(body[0..types.handle_size]);
    }

    pub fn openFrom(self: *const UnixClient, base: types.Handle, token: types.LinkToken, mask: types.ActionSet) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try base.marshal(&req);
        try token.marshal(&req);
        try protocol.appendInt(&req, u64, mask, .big);

        const body = try self.ask(protocol.mt_open_from, req.items);
        defer self.allocator.free(body);
        if (body.len < types.handle_size) return error.InvalidMessage;
        return types.Handle.unmarshal(body[0..types.handle_size]);
    }

    pub fn createVolume(self: *const UnixClient, host: ?types.Endpoint, spec_json: []const u8) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();

        var host_data = try self.encodeHost(host);
        defer host_data.deinit();

        if (host_data.items.len > std.math.maxInt(u16)) return error.InvalidEndpoint;
        try protocol.appendInt(&req, u16, @intCast(host_data.items.len), .little);
        try req.appendSlice(host_data.items);

        if (spec_json.len > std.math.maxInt(u16)) return error.InvalidMessage;
        try protocol.appendInt(&req, u16, @intCast(spec_json.len), .little);
        try req.appendSlice(spec_json);

        const body = try self.ask(protocol.mt_create_volume, req.items);
        defer self.allocator.free(body);
        if (body.len < types.handle_size) return error.InvalidMessage;
        return types.Handle.unmarshal(body[0..types.handle_size]);
    }

    pub fn cloneVolume(_: *const UnixClient, _: ?types.NodeID, _: types.Handle) !types.Handle {
        return error.UnsupportedOperation;
    }

    pub fn inspectVolume(self: *const UnixClient, volume: types.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try volume.marshal(&req);

        const body = try self.ask(protocol.mt_volume_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn beginTx(self: *const UnixClient, volume: types.Handle, params: types.TxParams) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try volume.marshal(&req);
        try params.marshalBcp(&req);

        const body = try self.ask(protocol.mt_volume_begin_tx, req.items);
        defer self.allocator.free(body);
        if (body.len < types.handle_size) return error.InvalidMessage;
        return types.Handle.unmarshal(body[0..types.handle_size]);
    }

    pub fn inspectTx(self: *const UnixClient, tx: types.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);

        const body = try self.ask(protocol.mt_tx_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn commit(self: *const UnixClient, tx: types.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try self.askVoid(protocol.mt_tx_commit, req.items);
    }

    pub fn abort(self: *const UnixClient, tx: types.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try self.askVoid(protocol.mt_tx_abort, req.items);
    }

    pub fn save(self: *const UnixClient, tx: types.Handle, root: []const u8) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try req.appendSlice(root);
        try self.askVoid(protocol.mt_tx_save, req.items);
    }

    pub fn load(self: *const UnixClient, tx: types.Handle) ![]u8 {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        return self.ask(protocol.mt_tx_load, req.items);
    }

    pub fn post(self: *const UnixClient, tx: types.Handle, data: []const u8, opts: types.PostOpts) !types.CID {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);

        const code = if (opts.salt) |salt| blk: {
            try req.appendSlice(salt.asBytes());
            break :blk protocol.mt_tx_post_salt;
        } else protocol.mt_tx_post;

        try req.appendSlice(data);
        const body = try self.ask(code, req.items);
        defer self.allocator.free(body);
        return types.CID.fromBytes(body);
    }

    pub fn get(self: *const UnixClient, tx: types.Handle, cid: types.CID, buffer: []u8, opts: types.GetOpts) !usize {
        var req = self.requestBuffer();
        defer req.deinit();
        _ = opts.skip_verify;
        try tx.marshal(&req);

        const code = if (opts.salt) |salt| blk: {
            try req.appendSlice(salt.asBytes());
            break :blk protocol.mt_tx_get_salt;
        } else protocol.mt_tx_get;

        try req.appendSlice(cid.asBytes());
        const body = try self.ask(code, req.items);
        defer self.allocator.free(body);

        if (body.len > buffer.len) return error.BufferTooShort;
        @memcpy(buffer[0..body.len], body);
        return body.len;
    }

    pub fn exists(self: *const UnixClient, tx: types.Handle, cids: []const types.CID) ![]bool {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        for (cids) |cid| try req.appendSlice(cid.asBytes());

        const body = try self.ask(protocol.mt_tx_exists, req.items);
        defer self.allocator.free(body);
        return protocol.decodeBoolBitset(self.allocator, body, cids.len);
    }

    pub fn delete(self: *const UnixClient, tx: types.Handle, cids: []const types.CID) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try protocol.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());
        try self.askVoid(protocol.mt_tx_delete, req.items);
    }

    pub fn copy(self: *const UnixClient, tx: types.Handle, src_txs: []const types.Handle, cids: []const types.CID) ![]bool {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try protocol.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());
        try protocol.writeUvarint(&req, src_txs.len);
        for (src_txs) |src| try src.marshal(&req);

        const body = try self.ask(protocol.mt_tx_copy, req.items);
        defer self.allocator.free(body);
        return protocol.decodeBoolBitset(self.allocator, body, cids.len);
    }

    pub fn visit(self: *const UnixClient, tx: types.Handle, cids: []const types.CID) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try protocol.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());
        try self.askVoid(protocol.mt_tx_visit, req.items);
    }

    pub fn isVisited(self: *const UnixClient, tx: types.Handle, cids: []const types.CID) ![]bool {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try protocol.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());

        const body = try self.ask(protocol.mt_tx_is_visited, req.items);
        defer self.allocator.free(body);
        return protocol.decodeBoolBitset(self.allocator, body, cids.len);
    }

    pub fn link(self: *const UnixClient, tx: types.Handle, target: types.Handle, mask: types.ActionSet) !types.LinkToken {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try target.marshal(&req);
        try protocol.appendInt(&req, u64, mask, .big);

        const body = try self.ask(protocol.mt_tx_link, req.items);
        defer self.allocator.free(body);
        return types.LinkToken.unmarshal(body);
    }

    pub fn unlink(self: *const UnixClient, tx: types.Handle, targets: []const types.LinkToken) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try protocol.writeUvarint(&req, targets.len);
        for (targets) |token| try token.marshal(&req);
        try self.askVoid(protocol.mt_tx_unlink, req.items);
    }

    pub fn visitLinks(self: *const UnixClient, tx: types.Handle, targets: []const types.LinkToken) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try protocol.writeUvarint(&req, targets.len);
        for (targets) |token| try token.marshal(&req);
        try self.askVoid(protocol.mt_tx_visit_links, req.items);
    }

    pub fn createQueue(self: *const UnixClient, host: ?types.Endpoint, spec_json: []const u8) !types.Handle {
        var req = self.requestBuffer();
        defer req.deinit();

        var host_data = try self.encodeHost(host);
        defer host_data.deinit();

        if (host_data.items.len > std.math.maxInt(u16)) return error.InvalidEndpoint;
        try protocol.appendInt(&req, u16, @intCast(host_data.items.len), .little);
        try req.appendSlice(host_data.items);

        if (spec_json.len > std.math.maxInt(u16)) return error.InvalidMessage;
        try protocol.appendInt(&req, u16, @intCast(spec_json.len), .little);
        try req.appendSlice(spec_json);

        const body = try self.ask(protocol.mt_queue_create, req.items);
        defer self.allocator.free(body);
        return types.Handle.unmarshal(body);
    }

    pub fn inspectQueue(self: *const UnixClient, queue: types.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);

        const body = try self.ask(protocol.mt_queue_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn dequeue(self: *const UnixClient, queue: types.Handle, max: usize, opts: types.DequeueOpts) ![]types.Message {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);
        try protocol.appendInt(&req, u32, @intCast(max), .little);
        try std.json.stringify(opts, .{}, req.writer());

        const body = try self.ask(protocol.mt_queue_dequeue, req.items);
        defer self.allocator.free(body);
        if (body.len < 4) return error.InvalidMessage;

        const count: usize = @intCast(std.mem.readInt(u32, body[0..4], .little));
        const out = try self.allocator.alloc(types.Message, count);
        errdefer {
            for (out) |*message| message.deinit(self.allocator);
            self.allocator.free(out);
        }

        var rest = body[4..];
        for (out) |*message| {
            const lp = try protocol.readLp(rest);
            message.* = try types.Message.unmarshal(self.allocator, lp.payload);
            rest = lp.rest;
        }

        return out;
    }

    pub fn enqueue(self: *const UnixClient, queue: types.Handle, messages: []const types.Message) !types.InsertResp {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);
        try protocol.appendInt(&req, u32, @intCast(messages.len), .little);

        var encoded = self.requestBuffer();
        defer encoded.deinit();

        for (messages) |msg| {
            encoded.clearRetainingCapacity();
            try msg.marshal(&encoded);
            try protocol.appendLp(&req, encoded.items);
        }

        const body = try self.ask(protocol.mt_queue_enqueue, req.items);
        defer self.allocator.free(body);
        if (body.len < 4) return error.InvalidMessage;

        return .{ .success = std.mem.readInt(u32, body[0..4], .little) };
    }

    pub fn subToVolume(self: *const UnixClient, queue: types.Handle, volume: types.Handle, spec_json: []const u8) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);
        try volume.marshal(&req);
        try protocol.appendLp(&req, spec_json);
        try self.askVoid(protocol.mt_queue_sub_to_volume, req.items);
    }
};

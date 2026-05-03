const std = @import("std");
const bcp = @import("bcp.zig");
const blobcache = @import("blobcache.zig");

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

    fn ask(self: *const UnixClient, io: std.Io, code: bcp.MessageCode, body: []const u8) ![]u8 {
        const ua = try std.Io.net.UnixAddress.init(self.socket_path);
        const stream = try ua.connect(io);
        defer stream.close(io);

        var write_buf: [1024]u8 = undefined;
        var writer = stream.writer(io, &write_buf);
        try bcp.writeMessage(&writer.interface, code, body);
        try writer.interface.flush();

        var read_buf: [1024]u8 = undefined;
        var reader = stream.reader(io, &read_buf);
        const resp = try bcp.readMessageAlloc(self.allocator, &reader.interface);
        errdefer self.allocator.free(resp.body);

        try bcp.checkOk(resp.code);
        return resp.body;
    }

    fn askVoid(self: *const UnixClient, io: std.Io, code: bcp.MessageCode, body: []const u8) !void {
        const resp = try self.ask(io, code, body);
        self.allocator.free(resp);
    }

    fn requestBuffer(self: *const UnixClient) std.array_list.Managed(u8) {
        return std.array_list.Managed(u8).init(self.allocator);
    }

    fn encodeHost(self: *const UnixClient, host: ?blobcache.Endpoint) !std.array_list.Managed(u8) {
        var host_data = std.array_list.Managed(u8).init(self.allocator);
        errdefer host_data.deinit();

        if (host) |h| {
            try h.marshalBcp(&host_data);
        } else {
            try host_data.appendNTimes(0, blobcache.node_id_size);
        }
        return host_data;
    }

    pub fn endpoint(self: *const UnixClient, io: std.Io) !blobcache.Endpoint {
        const body = try self.ask(io, bcp.mt_endpoint, "");
        defer self.allocator.free(body);
        return blobcache.Endpoint.fromBcpBytes(self.allocator, body);
    }

    pub fn inspectHandle(self: *const UnixClient, io: std.Io, handle: blobcache.Handle) !struct {
        oid: blobcache.OID,
        rights: blobcache.ActionSet,
        created_at: [8]u8,
        expires_at: [8]u8,
    } {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);

        const body = try self.ask(io, bcp.mt_handle_inspect, req.items);
        defer self.allocator.free(body);
        if (body.len < 40) return error.InvalidMessage;

        var created_at: [8]u8 = undefined;
        var expires_at: [8]u8 = undefined;
        @memcpy(&created_at, body[24..32]);
        @memcpy(&expires_at, body[32..40]);

        return .{
            .oid = try blobcache.OID.fromBytes(body[0..16]),
            .rights = std.mem.readInt(u64, body[16..24], .little),
            .created_at = created_at,
            .expires_at = expires_at,
        };
    }

    pub fn dropHandle(self: *const UnixClient, io: std.Io, handle: blobcache.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);
        try self.askVoid(io, bcp.mt_handle_drop, req.items);
    }

    pub fn keepAlive(self: *const UnixClient, io: std.Io, handles: []const blobcache.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        for (handles) |h| try h.marshal(&req);
        try self.askVoid(io, bcp.mt_handle_keep_alive, req.items);
    }

    pub fn shareOut(self: *const UnixClient, io: std.Io, handle: blobcache.Handle, to: blobcache.NodeID, mask: blobcache.ActionSet) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);
        try req.appendSlice(&to.bytes);
        try bcp.appendInt(&req, u64, mask, .big);

        const body = try self.ask(io, bcp.mt_handle_share_out, req.items);
        defer self.allocator.free(body);
        return blobcache.Handle.unmarshal(body);
    }

    pub fn shareIn(self: *const UnixClient, io: std.Io, host: blobcache.NodeID, handle: blobcache.Handle) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try req.appendSlice(&host.bytes);
        try handle.marshal(&req);

        const body = try self.ask(io, bcp.mt_handle_share_in, req.items);
        defer self.allocator.free(body);
        return blobcache.Handle.unmarshal(body);
    }

    pub fn inspect(self: *const UnixClient, io: std.Io, handle: blobcache.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try handle.marshal(&req);

        const body = try self.ask(io, bcp.mt_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn openFiat(self: *const UnixClient, io: std.Io, target: blobcache.OID, mask: blobcache.ActionSet) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try req.appendSlice(target.asBytes());
        try bcp.appendInt(&req, u64, mask, .big);

        const body = try self.ask(io, bcp.mt_open_fiat, req.items);
        defer self.allocator.free(body);
        if (body.len < blobcache.handle_size) return error.InvalidMessage;
        return blobcache.Handle.unmarshal(body[0..blobcache.handle_size]);
    }

    pub fn openFrom(self: *const UnixClient, io: std.Io, base: blobcache.Handle, token: blobcache.LinkToken, mask: blobcache.ActionSet) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try base.marshal(&req);
        try token.marshal(&req);
        try bcp.appendInt(&req, u64, mask, .big);

        const body = try self.ask(io, bcp.mt_open_from, req.items);
        defer self.allocator.free(body);
        if (body.len < blobcache.handle_size) return error.InvalidMessage;
        return blobcache.Handle.unmarshal(body[0..blobcache.handle_size]);
    }

    pub fn createVolume(self: *const UnixClient, io: std.Io, host: ?blobcache.Endpoint, spec_json: []const u8) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();

        var host_data = try self.encodeHost(host);
        defer host_data.deinit();

        if (host_data.items.len > std.math.maxInt(u16)) return error.InvalidEndpoint;
        try bcp.appendInt(&req, u16, @intCast(host_data.items.len), .little);
        try req.appendSlice(host_data.items);

        if (spec_json.len > std.math.maxInt(u16)) return error.InvalidMessage;
        try bcp.appendInt(&req, u16, @intCast(spec_json.len), .little);
        try req.appendSlice(spec_json);

        const body = try self.ask(io, bcp.mt_create_volume, req.items);
        defer self.allocator.free(body);
        if (body.len < blobcache.handle_size) return error.InvalidMessage;
        return blobcache.Handle.unmarshal(body[0..blobcache.handle_size]);
    }

    pub fn cloneVolume(_: *const UnixClient, _: std.Io, _: ?blobcache.NodeID, _: blobcache.Handle) !blobcache.Handle {
        return error.UnsupportedOperation;
    }

    pub fn inspectVolume(self: *const UnixClient, io: std.Io, volume: blobcache.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try volume.marshal(&req);

        const body = try self.ask(io, bcp.mt_volume_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn beginTx(self: *const UnixClient, io: std.Io, volume: blobcache.Handle, params: blobcache.TxParams) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();
        try volume.marshal(&req);
        try params.marshalBcp(&req);

        const body = try self.ask(io, bcp.mt_volume_begin_tx, req.items);
        defer self.allocator.free(body);
        if (body.len < blobcache.handle_size) return error.InvalidMessage;
        return blobcache.Handle.unmarshal(body[0..blobcache.handle_size]);
    }

    pub fn inspectTx(self: *const UnixClient, io: std.Io, tx: blobcache.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);

        const body = try self.ask(io, bcp.mt_tx_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn commit(self: *const UnixClient, io: std.Io, tx: blobcache.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try self.askVoid(io, bcp.mt_tx_commit, req.items);
    }

    pub fn abort(self: *const UnixClient, io: std.Io, tx: blobcache.Handle) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try self.askVoid(io, bcp.mt_tx_abort, req.items);
    }

    pub fn save(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, root: []const u8) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try req.appendSlice(root);
        try self.askVoid(io, bcp.mt_tx_save, req.items);
    }

    pub fn load(self: *const UnixClient, io: std.Io, tx: blobcache.Handle) ![]u8 {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        return self.ask(io, bcp.mt_tx_load, req.items);
    }

    pub fn post(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, data: []const u8, opts: blobcache.PostOpts) !blobcache.CID {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);

        const code = if (opts.salt) |salt| blk: {
            try req.appendSlice(salt.asBytes());
            break :blk bcp.mt_tx_post_salt;
        } else bcp.mt_tx_post;

        try req.appendSlice(data);
        const body = try self.ask(io, code, req.items);
        defer self.allocator.free(body);
        return blobcache.CID.fromBytes(body);
    }

    pub fn get(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, cid: blobcache.CID, buffer: []u8, opts: blobcache.GetOpts) !usize {
        var req = self.requestBuffer();
        defer req.deinit();
        _ = opts.skip_verify;
        try tx.marshal(&req);

        const code = if (opts.salt) |salt| blk: {
            try req.appendSlice(salt.asBytes());
            break :blk bcp.mt_tx_get_salt;
        } else bcp.mt_tx_get;

        try req.appendSlice(cid.asBytes());
        const body = try self.ask(io, code, req.items);
        defer self.allocator.free(body);

        if (body.len > buffer.len) return error.BufferTooShort;
        @memcpy(buffer[0..body.len], body);
        return body.len;
    }

    pub fn exists(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, cids: []const blobcache.CID) ![]bool {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        for (cids) |cid| try req.appendSlice(cid.asBytes());

        const body = try self.ask(io, bcp.mt_tx_exists, req.items);
        defer self.allocator.free(body);
        return bcp.decodeBoolBitset(self.allocator, body, cids.len);
    }

    pub fn delete(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, cids: []const blobcache.CID) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try bcp.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());
        try self.askVoid(io, bcp.mt_tx_delete, req.items);
    }

    pub fn copy(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, src_txs: []const blobcache.Handle, cids: []const blobcache.CID) ![]bool {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try bcp.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());
        try bcp.writeUvarint(&req, src_txs.len);
        for (src_txs) |src| try src.marshal(&req);

        const body = try self.ask(io, bcp.mt_tx_copy, req.items);
        defer self.allocator.free(body);
        return bcp.decodeBoolBitset(self.allocator, body, cids.len);
    }

    pub fn visit(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, cids: []const blobcache.CID) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try bcp.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());
        try self.askVoid(io, bcp.mt_tx_visit, req.items);
    }

    pub fn isVisited(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, cids: []const blobcache.CID) ![]bool {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try bcp.writeUvarint(&req, cids.len);
        for (cids) |cid| try req.appendSlice(cid.asBytes());

        const body = try self.ask(io, bcp.mt_tx_is_visited, req.items);
        defer self.allocator.free(body);
        return bcp.decodeBoolBitset(self.allocator, body, cids.len);
    }

    pub fn link(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, target: blobcache.Handle, mask: blobcache.ActionSet) !blobcache.LinkToken {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try target.marshal(&req);
        try bcp.appendInt(&req, u64, mask, .big);

        const body = try self.ask(io, bcp.mt_tx_link, req.items);
        defer self.allocator.free(body);
        return blobcache.LinkToken.unmarshal(body);
    }

    pub fn unlink(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, targets: []const blobcache.LinkToken) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try bcp.writeUvarint(&req, targets.len);
        for (targets) |token| try token.marshal(&req);
        try self.askVoid(io, bcp.mt_tx_unlink, req.items);
    }

    pub fn visitLinks(self: *const UnixClient, io: std.Io, tx: blobcache.Handle, targets: []const blobcache.LinkToken) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try tx.marshal(&req);
        try bcp.writeUvarint(&req, targets.len);
        for (targets) |token| try token.marshal(&req);
        try self.askVoid(io, bcp.mt_tx_visit_links, req.items);
    }

    pub fn createQueue(self: *const UnixClient, io: std.Io, host: ?blobcache.Endpoint, spec_json: []const u8) !blobcache.Handle {
        var req = self.requestBuffer();
        defer req.deinit();

        var host_data = try self.encodeHost(host);
        defer host_data.deinit();

        if (host_data.items.len > std.math.maxInt(u16)) return error.InvalidEndpoint;
        try bcp.appendInt(&req, u16, @intCast(host_data.items.len), .little);
        try req.appendSlice(host_data.items);

        if (spec_json.len > std.math.maxInt(u16)) return error.InvalidMessage;
        try bcp.appendInt(&req, u16, @intCast(spec_json.len), .little);
        try req.appendSlice(spec_json);

        const body = try self.ask(io, bcp.mt_queue_create, req.items);
        defer self.allocator.free(body);
        return blobcache.Handle.unmarshal(body);
    }

    pub fn inspectQueue(self: *const UnixClient, io: std.Io, queue: blobcache.Handle) !std.json.Parsed(std.json.Value) {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);

        const body = try self.ask(io, bcp.mt_queue_inspect, req.items);
        defer self.allocator.free(body);
        return std.json.parseFromSlice(std.json.Value, self.allocator, body, .{});
    }

    pub fn dequeue(self: *const UnixClient, io: std.Io, queue: blobcache.Handle, max: usize, opts: blobcache.DequeueOpts) ![]blobcache.Message {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);
        try bcp.appendInt(&req, u32, @intCast(max), .little);
        try std.json.stringify(opts, .{}, req.writer());

        const body = try self.ask(io, bcp.mt_queue_dequeue, req.items);
        defer self.allocator.free(body);
        if (body.len < 4) return error.InvalidMessage;

        const count: usize = @intCast(std.mem.readInt(u32, body[0..4], .little));
        const out = try self.allocator.alloc(blobcache.Message, count);
        errdefer {
            for (out) |*message| message.deinit(self.allocator);
            self.allocator.free(out);
        }

        var rest = body[4..];
        for (out) |*message| {
            const lp = try bcp.readLp(rest);
            message.* = try blobcache.Message.unmarshal(self.allocator, lp.payload);
            rest = lp.rest;
        }

        return out;
    }

    pub fn enqueue(self: *const UnixClient, io: std.Io, queue: blobcache.Handle, messages: []const blobcache.Message) !blobcache.InsertResp {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);
        try bcp.appendInt(&req, u32, @intCast(messages.len), .little);

        var encoded = self.requestBuffer();
        defer encoded.deinit();

        for (messages) |msg| {
            encoded.clearRetainingCapacity();
            try msg.marshal(&encoded);
            try bcp.appendLp(&req, encoded.items);
        }

        const body = try self.ask(io, bcp.mt_queue_enqueue, req.items);
        defer self.allocator.free(body);
        if (body.len < 4) return error.InvalidMessage;

        return .{ .success = std.mem.readInt(u32, body[0..4], .little) };
    }

    pub fn subToVolume(self: *const UnixClient, io: std.Io, queue: blobcache.Handle, volume: blobcache.Handle, spec_json: []const u8) !void {
        var req = self.requestBuffer();
        defer req.deinit();
        try queue.marshal(&req);
        try volume.marshal(&req);
        try bcp.appendLp(&req, spec_json);
        try self.askVoid(io, bcp.mt_queue_sub_to_volume, req.items);
    }
};

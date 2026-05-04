const std = @import("std");

pub const MessageCode = u16;

pub const header_len: usize = 8;
const section_size: u16 = 256;

pub const mt_endpoint: MessageCode = 2;
pub const mt_inspect: MessageCode = 3;

pub const mt_handle_inspect: MessageCode = (1 * section_size) + 0;
pub const mt_handle_drop: MessageCode = (1 * section_size) + 1;
pub const mt_handle_keep_alive: MessageCode = (1 * section_size) + 2;
pub const mt_handle_share_out: MessageCode = (1 * section_size) + 3;
pub const mt_handle_share_in: MessageCode = (1 * section_size) + 4;

pub const mt_volume_inspect: MessageCode = (2 * section_size) + 0;
pub const mt_volume_begin_tx: MessageCode = (2 * section_size) + 1;
pub const mt_open_fiat: MessageCode = 4;
pub const mt_open_from: MessageCode = (2 * section_size) + 2;
pub const mt_create_volume: MessageCode = (3 * section_size) - 1;

pub const mt_tx_inspect: MessageCode = (3 * section_size) + 0;
pub const mt_tx_abort: MessageCode = (3 * section_size) + 1;
pub const mt_tx_commit: MessageCode = (3 * section_size) + 2;
pub const mt_tx_load: MessageCode = (3 * section_size) + 3;
pub const mt_tx_save: MessageCode = (3 * section_size) + 4;
pub const mt_tx_post: MessageCode = (3 * section_size) + 5;
pub const mt_tx_post_salt: MessageCode = (3 * section_size) + 6;
pub const mt_tx_get: MessageCode = (3 * section_size) + 7;
pub const mt_tx_get_salt: MessageCode = (3 * section_size) + 8;
pub const mt_tx_exists: MessageCode = (3 * section_size) + 9;
pub const mt_tx_delete: MessageCode = (3 * section_size) + 10;
pub const mt_tx_copy: MessageCode = (3 * section_size) + 11;
pub const mt_tx_link: MessageCode = (3 * section_size) + 12;
pub const mt_tx_unlink: MessageCode = (3 * section_size) + 13;
pub const mt_tx_visit: MessageCode = (3 * section_size) + 14;
pub const mt_tx_is_visited: MessageCode = (3 * section_size) + 15;
pub const mt_tx_visit_links: MessageCode = (3 * section_size) + 16;

pub const mt_queue_inspect: MessageCode = (4 * section_size) + 0;
pub const mt_queue_enqueue: MessageCode = (4 * section_size) + 1;
pub const mt_queue_dequeue: MessageCode = (4 * section_size) + 2;
pub const mt_queue_sub_to_volume: MessageCode = (4 * section_size) + 3;
pub const mt_queue_create: MessageCode = (5 * section_size) - 1;

pub const mt_ok: MessageCode = (255 * section_size) + 0;
pub const mt_error_timeout: MessageCode = (255 * section_size) + 1;
pub const mt_error_invalid_handle: MessageCode = (255 * section_size) + 2;
pub const mt_error_not_found: MessageCode = (255 * section_size) + 3;
pub const mt_error_no_permission: MessageCode = (255 * section_size) + 4;
pub const mt_error_no_link: MessageCode = (255 * section_size) + 5;
pub const mt_error_too_large: MessageCode = (255 * section_size) + 6;
pub const mt_error_unknown: MessageCode = std.math.maxInt(u16);

pub const Response = struct {
    code: MessageCode,
    body: []u8,
};

pub fn appendInt(out: *std.array_list.Managed(u8), comptime T: type, value: T, endian: std.builtin.Endian) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, endian);
    try out.appendSlice(&buf);
}

pub fn writeMessage(writer: anytype, code: MessageCode, body: []const u8) !void {
    var header = std.mem.zeroes([header_len]u8);
    header[0] = @truncate(code >> 8);
    header[1] = @truncate(code & 0xff);
    std.mem.writeInt(u32, header[4..8], @intCast(body.len), .little);
    try writer.writeAll(&header);
    try writer.writeAll(body);
}

fn readExact(reader: anytype, out: []u8) !void {
    const reader_ty = @TypeOf(reader.*);
    if (@hasDecl(reader_ty, "readSliceAll")) {
        try reader.readSliceAll(out);
        return;
    }
    if (@hasDecl(reader_ty, "readNoEof")) {
        try reader.readNoEof(out);
        return;
    }
    @compileError("reader type must support readSliceAll or readNoEof");
}

pub fn readMessageAlloc(allocator: std.mem.Allocator, reader: anytype) !Response {
    var header: [header_len]u8 = undefined;
    try readExact(reader, &header);
    const code = (@as(u16, header[0]) << 8) | @as(u16, header[1]);
    const body_len = std.mem.readInt(u32, header[4..8], .little);
    const body = try allocator.alloc(u8, body_len);
    errdefer allocator.free(body);
    try readExact(reader, body);
    return .{ .code = code, .body = body };
}

pub fn checkOk(code: MessageCode) !void {
    if (code == mt_ok) return;
    if (code > mt_ok) {
        return switch (code) {
            mt_error_timeout => error.WireTimeout,
            mt_error_invalid_handle => error.WireInvalidHandle,
            mt_error_not_found => error.WireNotFound,
            mt_error_no_permission => error.WireNoPermission,
            mt_error_no_link => error.WireNoLink,
            mt_error_too_large => error.WireTooLarge,
            mt_error_unknown => error.WireUnknown,
            else => error.WireError,
        };
    }
    return error.UnexpectedResponseCode;
}

pub fn writeUvarint(out: *std.array_list.Managed(u8), value: u64) !void {
    var x = value;
    while (x >= 0x80) {
        try out.append(@as(u8, @truncate(x)) | 0x80);
        x >>= 7;
    }
    try out.append(@truncate(x));
}

pub fn readUvarint(data: []const u8) !struct { value: u64, rest: []const u8 } {
    var x: u64 = 0;
    var shift: u6 = 0;
    for (data, 0..) |b, i| {
        if (b < 0x80) {
            if (i > 9 or (i == 9 and b > 1)) return error.InvalidMessage;
            return .{ .value = x | (@as(u64, b) << shift), .rest = data[i + 1 ..] };
        }
        x |= (@as(u64, b & 0x7f) << shift);
        shift += 7;
    }
    return error.InvalidMessage;
}

pub fn appendLp(out: *std.array_list.Managed(u8), payload: []const u8) !void {
    try writeUvarint(out, payload.len);
    try out.appendSlice(payload);
}

pub fn readLp(data: []const u8) !struct { payload: []const u8, rest: []const u8 } {
    const len_and_rest = try readUvarint(data);
    if (len_and_rest.rest.len < len_and_rest.value) return error.InvalidMessage;
    const n: usize = @intCast(len_and_rest.value);
    return .{ .payload = len_and_rest.rest[0..n], .rest = len_and_rest.rest[n..] };
}

pub fn decodeBoolBitset(allocator: std.mem.Allocator, bytes: []const u8, n: usize) ![]bool {
    const out = try allocator.alloc(bool, n);
    var i: usize = 0;
    for (bytes) |byte| {
        var bit: u3 = 0;
        while (bit < 8) : (bit += 1) {
            if (i == n) return out;
            out[i] = (byte & (@as(u8, 1) << bit)) != 0;
            i += 1;
        }
    }
    while (i < n) : (i += 1) out[i] = false;
    return out;
}

test "uvarint roundtrip" {
    var da = std.heap.DebugAllocator(.{}){};
    defer _ = da.deinit();
    const allocator = da.allocator();

    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();

    try writeUvarint(&buf, 0);
    try writeUvarint(&buf, 127);
    try writeUvarint(&buf, 128);
    try writeUvarint(&buf, 65_535);

    var rest: []const u8 = buf.items;
    for ([_]u64{ 0, 127, 128, 65_535 }) |want| {
        const got = try readUvarint(rest);
        try std.testing.expectEqual(want, got.value);
        rest = got.rest;
    }
    try std.testing.expectEqual(@as(usize, 0), rest.len);
}

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.14.0
// source: blobcache.proto

package bcgrpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// BlobcacheClient is the client API for Blobcache service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BlobcacheClient interface {
	// CreateDir creates a directory at name under the parent.
	CreateDir(ctx context.Context, in *CreateDirReq, opts ...grpc.CallOption) (*HandleRes, error)
	// Open resolves a path relative to a handle
	Open(ctx context.Context, in *OpenReq, opts ...grpc.CallOption) (*HandleRes, error)
	// DeleteEntry removes the entry with name from its parent.
	DeleteEntry(ctx context.Context, in *DeleteEntryReq, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// ListEntries lists the entries under a parent.
	ListEntries(ctx context.Context, in *ListEntriesReq, opts ...grpc.CallOption) (Blobcache_ListEntriesClient, error)
	// CreatePinSet creates a PinSet at name under the parent.
	CreatePinSet(ctx context.Context, in *CreatePinSetReq, opts ...grpc.CallOption) (*HandleRes, error)
	// GetPinSet returns information about a PinSet
	GetPinSet(ctx context.Context, in *GetPinSetReq, opts ...grpc.CallOption) (*PinSet, error)
	// Post uploads data and adds it to a PinSet.
	Post(ctx context.Context, in *PostReq, opts ...grpc.CallOption) (*wrapperspb.BytesValue, error)
	// Get retrieves data from a PinSet and returns.
	Get(ctx context.Context, in *GetReq, opts ...grpc.CallOption) (*wrapperspb.BytesValue, error)
	// Add adds data by ID to a PinSet.
	Add(ctx context.Context, in *AddReq, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Delete removes data from a PinSet
	Delete(ctx context.Context, in *DeleteReq, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// List lists blobs by ID in a PinSet
	List(ctx context.Context, in *ListReq, opts ...grpc.CallOption) (*ListRes, error)
	// WaitOK
	WaitOK(ctx context.Context, in *WaitReq, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type blobcacheClient struct {
	cc grpc.ClientConnInterface
}

func NewBlobcacheClient(cc grpc.ClientConnInterface) BlobcacheClient {
	return &blobcacheClient{cc}
}

func (c *blobcacheClient) CreateDir(ctx context.Context, in *CreateDirReq, opts ...grpc.CallOption) (*HandleRes, error) {
	out := new(HandleRes)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/CreateDir", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) Open(ctx context.Context, in *OpenReq, opts ...grpc.CallOption) (*HandleRes, error) {
	out := new(HandleRes)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/Open", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) DeleteEntry(ctx context.Context, in *DeleteEntryReq, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/DeleteEntry", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) ListEntries(ctx context.Context, in *ListEntriesReq, opts ...grpc.CallOption) (Blobcache_ListEntriesClient, error) {
	stream, err := c.cc.NewStream(ctx, &Blobcache_ServiceDesc.Streams[0], "/blobcache.Blobcache/ListEntries", opts...)
	if err != nil {
		return nil, err
	}
	x := &blobcacheListEntriesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Blobcache_ListEntriesClient interface {
	Recv() (*Entry, error)
	grpc.ClientStream
}

type blobcacheListEntriesClient struct {
	grpc.ClientStream
}

func (x *blobcacheListEntriesClient) Recv() (*Entry, error) {
	m := new(Entry)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *blobcacheClient) CreatePinSet(ctx context.Context, in *CreatePinSetReq, opts ...grpc.CallOption) (*HandleRes, error) {
	out := new(HandleRes)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/CreatePinSet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) GetPinSet(ctx context.Context, in *GetPinSetReq, opts ...grpc.CallOption) (*PinSet, error) {
	out := new(PinSet)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/GetPinSet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) Post(ctx context.Context, in *PostReq, opts ...grpc.CallOption) (*wrapperspb.BytesValue, error) {
	out := new(wrapperspb.BytesValue)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/Post", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) Get(ctx context.Context, in *GetReq, opts ...grpc.CallOption) (*wrapperspb.BytesValue, error) {
	out := new(wrapperspb.BytesValue)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) Add(ctx context.Context, in *AddReq, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/Add", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) Delete(ctx context.Context, in *DeleteReq, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) List(ctx context.Context, in *ListReq, opts ...grpc.CallOption) (*ListRes, error) {
	out := new(ListRes)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/List", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blobcacheClient) WaitOK(ctx context.Context, in *WaitReq, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/blobcache.Blobcache/WaitOK", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BlobcacheServer is the server API for Blobcache service.
// All implementations must embed UnimplementedBlobcacheServer
// for forward compatibility
type BlobcacheServer interface {
	// CreateDir creates a directory at name under the parent.
	CreateDir(context.Context, *CreateDirReq) (*HandleRes, error)
	// Open resolves a path relative to a handle
	Open(context.Context, *OpenReq) (*HandleRes, error)
	// DeleteEntry removes the entry with name from its parent.
	DeleteEntry(context.Context, *DeleteEntryReq) (*emptypb.Empty, error)
	// ListEntries lists the entries under a parent.
	ListEntries(*ListEntriesReq, Blobcache_ListEntriesServer) error
	// CreatePinSet creates a PinSet at name under the parent.
	CreatePinSet(context.Context, *CreatePinSetReq) (*HandleRes, error)
	// GetPinSet returns information about a PinSet
	GetPinSet(context.Context, *GetPinSetReq) (*PinSet, error)
	// Post uploads data and adds it to a PinSet.
	Post(context.Context, *PostReq) (*wrapperspb.BytesValue, error)
	// Get retrieves data from a PinSet and returns.
	Get(context.Context, *GetReq) (*wrapperspb.BytesValue, error)
	// Add adds data by ID to a PinSet.
	Add(context.Context, *AddReq) (*emptypb.Empty, error)
	// Delete removes data from a PinSet
	Delete(context.Context, *DeleteReq) (*emptypb.Empty, error)
	// List lists blobs by ID in a PinSet
	List(context.Context, *ListReq) (*ListRes, error)
	// WaitOK
	WaitOK(context.Context, *WaitReq) (*emptypb.Empty, error)
	mustEmbedUnimplementedBlobcacheServer()
}

// UnimplementedBlobcacheServer must be embedded to have forward compatible implementations.
type UnimplementedBlobcacheServer struct {
}

func (UnimplementedBlobcacheServer) CreateDir(context.Context, *CreateDirReq) (*HandleRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateDir not implemented")
}
func (UnimplementedBlobcacheServer) Open(context.Context, *OpenReq) (*HandleRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Open not implemented")
}
func (UnimplementedBlobcacheServer) DeleteEntry(context.Context, *DeleteEntryReq) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteEntry not implemented")
}
func (UnimplementedBlobcacheServer) ListEntries(*ListEntriesReq, Blobcache_ListEntriesServer) error {
	return status.Errorf(codes.Unimplemented, "method ListEntries not implemented")
}
func (UnimplementedBlobcacheServer) CreatePinSet(context.Context, *CreatePinSetReq) (*HandleRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreatePinSet not implemented")
}
func (UnimplementedBlobcacheServer) GetPinSet(context.Context, *GetPinSetReq) (*PinSet, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPinSet not implemented")
}
func (UnimplementedBlobcacheServer) Post(context.Context, *PostReq) (*wrapperspb.BytesValue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Post not implemented")
}
func (UnimplementedBlobcacheServer) Get(context.Context, *GetReq) (*wrapperspb.BytesValue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedBlobcacheServer) Add(context.Context, *AddReq) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Add not implemented")
}
func (UnimplementedBlobcacheServer) Delete(context.Context, *DeleteReq) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedBlobcacheServer) List(context.Context, *ListReq) (*ListRes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method List not implemented")
}
func (UnimplementedBlobcacheServer) WaitOK(context.Context, *WaitReq) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WaitOK not implemented")
}
func (UnimplementedBlobcacheServer) mustEmbedUnimplementedBlobcacheServer() {}

// UnsafeBlobcacheServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BlobcacheServer will
// result in compilation errors.
type UnsafeBlobcacheServer interface {
	mustEmbedUnimplementedBlobcacheServer()
}

func RegisterBlobcacheServer(s grpc.ServiceRegistrar, srv BlobcacheServer) {
	s.RegisterService(&Blobcache_ServiceDesc, srv)
}

func _Blobcache_CreateDir_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateDirReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).CreateDir(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/CreateDir",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).CreateDir(ctx, req.(*CreateDirReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_Open_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OpenReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).Open(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/Open",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).Open(ctx, req.(*OpenReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_DeleteEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteEntryReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).DeleteEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/DeleteEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).DeleteEntry(ctx, req.(*DeleteEntryReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_ListEntries_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListEntriesReq)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(BlobcacheServer).ListEntries(m, &blobcacheListEntriesServer{stream})
}

type Blobcache_ListEntriesServer interface {
	Send(*Entry) error
	grpc.ServerStream
}

type blobcacheListEntriesServer struct {
	grpc.ServerStream
}

func (x *blobcacheListEntriesServer) Send(m *Entry) error {
	return x.ServerStream.SendMsg(m)
}

func _Blobcache_CreatePinSet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreatePinSetReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).CreatePinSet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/CreatePinSet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).CreatePinSet(ctx, req.(*CreatePinSetReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_GetPinSet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetPinSetReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).GetPinSet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/GetPinSet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).GetPinSet(ctx, req.(*GetPinSetReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_Post_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PostReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).Post(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/Post",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).Post(ctx, req.(*PostReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).Get(ctx, req.(*GetReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_Add_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).Add(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/Add",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).Add(ctx, req.(*AddReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).Delete(ctx, req.(*DeleteReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_List_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).List(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/List",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).List(ctx, req.(*ListReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Blobcache_WaitOK_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WaitReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlobcacheServer).WaitOK(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/blobcache.Blobcache/WaitOK",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlobcacheServer).WaitOK(ctx, req.(*WaitReq))
	}
	return interceptor(ctx, in, info, handler)
}

// Blobcache_ServiceDesc is the grpc.ServiceDesc for Blobcache service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Blobcache_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "blobcache.Blobcache",
	HandlerType: (*BlobcacheServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateDir",
			Handler:    _Blobcache_CreateDir_Handler,
		},
		{
			MethodName: "Open",
			Handler:    _Blobcache_Open_Handler,
		},
		{
			MethodName: "DeleteEntry",
			Handler:    _Blobcache_DeleteEntry_Handler,
		},
		{
			MethodName: "CreatePinSet",
			Handler:    _Blobcache_CreatePinSet_Handler,
		},
		{
			MethodName: "GetPinSet",
			Handler:    _Blobcache_GetPinSet_Handler,
		},
		{
			MethodName: "Post",
			Handler:    _Blobcache_Post_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _Blobcache_Get_Handler,
		},
		{
			MethodName: "Add",
			Handler:    _Blobcache_Add_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _Blobcache_Delete_Handler,
		},
		{
			MethodName: "List",
			Handler:    _Blobcache_List_Handler,
		},
		{
			MethodName: "WaitOK",
			Handler:    _Blobcache_WaitOK_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ListEntries",
			Handler:       _Blobcache_ListEntries_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "blobcache.proto",
}

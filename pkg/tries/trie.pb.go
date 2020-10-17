// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.13.0
// source: trie.proto

package tries

import (
	proto "github.com/golang/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_trie_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_trie_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_trie_proto_rawDescGZIP(), []int{0}
}

func (x *Entry) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Entry) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type ChildRef struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id  []byte `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Dek []byte `protobuf:"bytes,2,opt,name=dek,proto3" json:"dek,omitempty"`
}

func (x *ChildRef) Reset() {
	*x = ChildRef{}
	if protoimpl.UnsafeEnabled {
		mi := &file_trie_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChildRef) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChildRef) ProtoMessage() {}

func (x *ChildRef) ProtoReflect() protoreflect.Message {
	mi := &file_trie_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChildRef.ProtoReflect.Descriptor instead.
func (*ChildRef) Descriptor() ([]byte, []int) {
	return file_trie_proto_rawDescGZIP(), []int{1}
}

func (x *ChildRef) GetId() []byte {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *ChildRef) GetDek() []byte {
	if x != nil {
		return x.Dek
	}
	return nil
}

type Node struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Prefix   []byte      `protobuf:"bytes,1,opt,name=prefix,proto3" json:"prefix,omitempty"`
	Entries  []*Entry    `protobuf:"bytes,2,rep,name=entries,proto3" json:"entries,omitempty"`
	Children []*ChildRef `protobuf:"bytes,3,rep,name=children,proto3" json:"children,omitempty"`
}

func (x *Node) Reset() {
	*x = Node{}
	if protoimpl.UnsafeEnabled {
		mi := &file_trie_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Node) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Node) ProtoMessage() {}

func (x *Node) ProtoReflect() protoreflect.Message {
	mi := &file_trie_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Node.ProtoReflect.Descriptor instead.
func (*Node) Descriptor() ([]byte, []int) {
	return file_trie_proto_rawDescGZIP(), []int{2}
}

func (x *Node) GetPrefix() []byte {
	if x != nil {
		return x.Prefix
	}
	return nil
}

func (x *Node) GetEntries() []*Entry {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *Node) GetChildren() []*ChildRef {
	if x != nil {
		return x.Children
	}
	return nil
}

var File_trie_proto protoreflect.FileDescriptor

var file_trie_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x74, 0x72, 0x69, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x2f, 0x0a, 0x05,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x2c, 0x0a,
	0x08, 0x43, 0x68, 0x69, 0x6c, 0x64, 0x52, 0x65, 0x66, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x02, 0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x64, 0x65, 0x6b,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x64, 0x65, 0x6b, 0x22, 0x67, 0x0a, 0x04, 0x4e,
	0x6f, 0x64, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x70, 0x72, 0x65, 0x66, 0x69, 0x78, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x06, 0x70, 0x72, 0x65, 0x66, 0x69, 0x78, 0x12, 0x20, 0x0a, 0x07, 0x65,
	0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x06, 0x2e, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x25, 0x0a,
	0x08, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x09, 0x2e, 0x43, 0x68, 0x69, 0x6c, 0x64, 0x52, 0x65, 0x66, 0x52, 0x08, 0x63, 0x68, 0x69, 0x6c,
	0x64, 0x72, 0x65, 0x6e, 0x42, 0x2a, 0x5a, 0x28, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x62, 0x6c, 0x6f, 0x62, 0x63, 0x61, 0x63, 0x68, 0x65, 0x2f, 0x62, 0x6c, 0x6f,
	0x62, 0x63, 0x61, 0x63, 0x68, 0x65, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x74, 0x72, 0x69, 0x65, 0x73,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_trie_proto_rawDescOnce sync.Once
	file_trie_proto_rawDescData = file_trie_proto_rawDesc
)

func file_trie_proto_rawDescGZIP() []byte {
	file_trie_proto_rawDescOnce.Do(func() {
		file_trie_proto_rawDescData = protoimpl.X.CompressGZIP(file_trie_proto_rawDescData)
	})
	return file_trie_proto_rawDescData
}

var file_trie_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_trie_proto_goTypes = []interface{}{
	(*Entry)(nil),    // 0: Entry
	(*ChildRef)(nil), // 1: ChildRef
	(*Node)(nil),     // 2: Node
}
var file_trie_proto_depIdxs = []int32{
	0, // 0: Node.entries:type_name -> Entry
	1, // 1: Node.children:type_name -> ChildRef
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_trie_proto_init() }
func file_trie_proto_init() {
	if File_trie_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_trie_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_trie_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChildRef); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_trie_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Node); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_trie_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_trie_proto_goTypes,
		DependencyIndexes: file_trie_proto_depIdxs,
		MessageInfos:      file_trie_proto_msgTypes,
	}.Build()
	File_trie_proto = out.File
	file_trie_proto_rawDesc = nil
	file_trie_proto_goTypes = nil
	file_trie_proto_depIdxs = nil
}

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// TopicClient is the client API for Topic service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TopicClient interface {
	TopicEventHandler(ctx context.Context, in *TopicEventRequest, opts ...grpc.CallOption) (*TopicEventResponse, error)
}

type topicClient struct {
	cc grpc.ClientConnInterface
}

func NewTopicClient(cc grpc.ClientConnInterface) TopicClient {
	return &topicClient{cc}
}

func (c *topicClient) TopicEventHandler(ctx context.Context, in *TopicEventRequest, opts ...grpc.CallOption) (*TopicEventResponse, error) {
	out := new(TopicEventResponse)
	err := c.cc.Invoke(ctx, "/api.iothub.v1.Topic/TopicEventHandler", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TopicServer is the server API for Topic service.
// All implementations must embed UnimplementedTopicServer
// for forward compatibility
type TopicServer interface {
	TopicEventHandler(context.Context, *TopicEventRequest) (*TopicEventResponse, error)
	mustEmbedUnimplementedTopicServer()
}

// UnimplementedTopicServer must be embedded to have forward compatible implementations.
type UnimplementedTopicServer struct {
}

func (UnimplementedTopicServer) TopicEventHandler(context.Context, *TopicEventRequest) (*TopicEventResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TopicEventHandler not implemented")
}
func (UnimplementedTopicServer) mustEmbedUnimplementedTopicServer() {}

// UnsafeTopicServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TopicServer will
// result in compilation errors.
type UnsafeTopicServer interface {
	mustEmbedUnimplementedTopicServer()
}

func RegisterTopicServer(s grpc.ServiceRegistrar, srv TopicServer) {
	s.RegisterService(&Topic_ServiceDesc, srv)
}

func _Topic_TopicEventHandler_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TopicEventRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TopicServer).TopicEventHandler(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.iothub.v1.Topic/TopicEventHandler",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TopicServer).TopicEventHandler(ctx, req.(*TopicEventRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Topic_ServiceDesc is the grpc.ServiceDesc for Topic service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Topic_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.iothub.v1.Topic",
	HandlerType: (*TopicServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "TopicEventHandler",
			Handler:    _Topic_TopicEventHandler_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/iothub/v1/topic.proto",
}

package grpc_and_consul

//go:generate protoc -I ./chatpb ./chatpb/chat.proto  --go_out=plugins=grpc:./chatpb
//go:generate protoc -I ./healthpb ./healthpb/health.proto  --go_out=plugins=grpc:./healthpb

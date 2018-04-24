package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	pb "grpc_and_consul/chatpb"
	"grpc_and_consul/core"
	"grpc_and_consul/grpclb"
	healthpb "grpc_and_consul/healthpb"
	"log"
	"net"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var (
	Port        int
	Address     string
	ServiceName string = "chat"
	RedisURL    string
)

type ConnPool struct {
	sync.Map
}

func (p *ConnPool) Del(username string) {
	p.Delete(username)
}

func (p *ConnPool) Add(username string, stream pb.Chater_SurfServer) {
	p.Store(username, stream)
}

func (p *ConnPool) Get(username string) *pb.Chater_SurfServer {
	if stream, ok := p.Load(username); ok {
		return stream.(*pb.Chater_SurfServer)

	} else {
		return nil
	}
}

func (p *ConnPool) BroadCast(from, message string) {
	log.Printf("broadcast from: %s, message: %s\n", from, message)
	p.Range(func(key, value interface{}) bool {
		username := key.(string)
		// 不发送给自己
		if username == from {
			return true
		}
		log.Printf("send message from %s to %s\n", from, username)
		stream := value.(pb.Chater_SurfServer)
		if err := stream.Send(&pb.SurfReply{
			Message:     message,
			MessageType: pb.SurfReply_CHAT_CONTENT,
		}); err != nil {
			log.Printf("Send message error. %v\n", err)
		}
		return true
	})
}

type HealthServer struct{}

func (s *HealthServer) Check(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}, nil
}

type ChatServer struct {
	sessionManager *core.SessionManager
	remoteChannel  *core.RemoteChannel
	connPool       *ConnPool
}

// Loign 登录用户.
func (s *ChatServer) Login(ctx context.Context, in *pb.LoginRequest) (*pb.LoginReply, error) {
	if in.Username == "" {
		return nil, errors.New("username cannot empty")
	}
	if s.sessionManager.Exists(in.Username) {
		return nil, fmt.Errorf("username: %q already exists", in.Username)
	}

	session := s.sessionManager.Create(in.Username)
	if session == nil {
		return &pb.LoginReply{
			Message: "login failed",
			Success: false,
		}, nil

	} else {
		s.BroadCast(in.Username, fmt.Sprintf("welecome %s join", in.Username))
		return &pb.LoginReply{
			Message: "login success",
			Success: true,
			Token:   session.Token,
		}, nil
	}
}

func (s *ChatServer) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingReply, error) {
	session := s.sessionManager.GetFromContext(ctx)
	if session == nil {
		return nil, errors.New("Please login before")
	}

	s.sessionManager.UpdateTTL(session.Username)
	return &pb.PingReply{
		Message: "ok",
	}, nil

}

func (s *ChatServer) Surf(stream pb.Chater_SurfServer) error {
	p, _ := peer.FromContext(stream.Context())
	log.Printf("New connection from %s\n", p.Addr.String())

	session := s.sessionManager.GetFromContext(stream.Context())
	if session == nil {
		return errors.New("Please login before")
	}
	s.connPool.Add(session.Username, stream)

	go func() {
		<-stream.Context().Done()
		s.connPool.Del(session.Username)
		s.sessionManager.Offline(session.Username)
	}()

	for {
		request, err := stream.Recv()
		if err != nil {
			break
		}
		s.BroadCast(session.Username, request.Message)
	}

	return nil
}

func (s *ChatServer) BroadCast(from, message string) {
	// 自己的节点自己广播
	s.connPool.BroadCast(from, message)

	s.remoteChannel.Out <- core.RemoteCommand{
		Command: "broadcast",
		Kwargs: map[string]interface{}{
			"from":    from,
			"message": message,
		},
	}
}

// Forever 监听其他server的广播
func (s *ChatServer) Forever() {
	for command := range s.remoteChannel.In {
		switch command.Command {
		case "broadcast":
			from := command.Kwargs["from"].(string)
			message := command.Kwargs["message"].(string)
			s.connPool.BroadCast(from, message)
		}
	}
}

func StartHealthCheck(lis net.Listener) {
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, &HealthServer{})

	server.Serve(lis)
}

func main() {
	flag.IntVar(&Port, "port", 6060, "listen port")
	flag.StringVar(&Address, "address", "127.0.0.1", "listen address")
	flag.StringVar(&RedisURL, "redis-url", "redis://127.0.0.1:6379/12", "redis address")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", Address, Port))
	if err != nil {
		log.Fatalln("listen error:", err)
	}

	consulCli, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Fatalln("new consul client", err)
	}

	options := []grpc.ServerOption{}

	server := grpc.NewServer(options...)
	s := &ChatServer{
		sessionManager: core.NewSessionManager(RedisURL),
		connPool:       &ConnPool{},
		remoteChannel:  core.NewRemoteChannel(consulCli, ""),
	}

	go s.Forever()
	pb.RegisterChaterServer(server, s)
	healthpb.RegisterHealthServer(server, &HealthServer{})

	go grpclb.RegisterService(consulCli, Address, Port, ServiceName)
	fmt.Printf("Listen: %s:%d\n", Address, Port)

	if err = server.Serve(lis); err != nil {
		panic(err)
	}
}

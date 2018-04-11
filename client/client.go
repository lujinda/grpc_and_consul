package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"grpc_and_consul/grpclb"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"utils"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "grpc_and_consul/chatpb"
)

var (
	Address     string
	ServiceName string = "chat"
	Username    string
	mutex       sync.Mutex
)

func ConsoleLog(message string) {
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Printf("\n----------- %s ---------\n%s\n> ", time.Now(), message)
}

func Input(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	line, _, err := reader.ReadLine()
	if err != nil {
		if err == io.EOF {
			return ""
		} else {
			log.Fatalf("%+v", errors.Wrap(err, "Input"))
		}
	}
	return string(line)
}

type Robot struct {
	sync.Mutex
	cli        *consulapi.Client
	conn       *grpc.ClientConn
	client     pb.ChaterClient
	chatStream pb.Chater_SurfClient
	ctx        context.Context
	cancel     context.CancelFunc
	closed     int32
	token      string
}

func (robot *Robot) Cancel() {
	robot.cancel()
}

func (robot *Robot) Close() {
	if atomic.CompareAndSwapInt32(&robot.closed, 0, 1) {
		robot.conn.Close()
		robot.conn = nil
	}
}

func (robot *Robot) Closed() bool {
	return atomic.LoadInt32(&robot.closed) == int32(0)
}

func (robot *Robot) GetChatStream() pb.Chater_SurfClient {
	robot.Lock()
	defer robot.Unlock()

	if robot.chatStream != nil {
		return robot.chatStream
	}
	for {
		stream, err := robot.client.Surf(robot.ctx)
		if err != nil {
			log.Println("get chat stream error.", err)
			time.Sleep(1 * time.Second)

		} else {
			robot.chatStream = stream
			break
		}
	}
	return robot.chatStream
}

func (robot *Robot) Ping() {
	var err error
	for range time.Tick(2 * time.Second) {
		_, err = robot.client.Ping(robot.ctx, &pb.PingRequest{
			TS: time.Now().UnixNano(),
		})
		if err != nil {
			log.Println("ping error.", err)

		}
	}
}

func (robot *Robot) Login(username string) error {
	reply, err := robot.client.Login(context.Background(), &pb.LoginRequest{
		Username: username,
	})
	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New("login failed")
	}
	robot.token = reply.Token
	robot.ctx = metadata.NewOutgoingContext(robot.ctx,
		metadata.Pairs("token", robot.token, "username", Username))
	return nil
}

func (robot *Robot) Connect() error {
	robot.Lock()
	defer robot.Unlock()

	if !robot.Closed() {
		robot.Close()
	}
	r := grpclb.NewConsulResolver(robot.cli)
	lb := grpc.RoundRobin(r)

	ctx, cancel := context.WithCancel(context.Background())
	robot.ctx = ctx
	robot.cancel = cancel

	options := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithBalancer(lb),
		grpc.WithBlock(),
		grpc.WithTimeout(10 * time.Second),
	}

	conn, err := grpc.DialContext(ctx, ServiceName, options...)
	if err != nil {
		return errors.Wrap(err, "grpc client connect")
	}

	client := pb.NewChaterClient(conn)
	robot.conn = conn
	robot.client = client
	robot.chatStream = nil

	return nil
}

func NewRobot() *Robot {
	cli, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Fatalln("create consul client error.", err)
	}
	robot := &Robot{
		cli: cli,
	}

	return robot
}

func main() {
	flag.StringVar(&Username, "name", "guest", "username")
	flag.Parse()

	robot := NewRobot()

	err := robot.Connect()
	if err != nil {
		log.Fatalln("connect service error.", err)
	}

	err = robot.Login(Username)
	if err != nil {
		log.Fatalln("login failed.", err)
	}
	go robot.Ping()
	ConsoleLog("login success")

	go func() {
		var (
			reply *pb.SurfReply
			err   error
		)
		for {
			reply, err = robot.GetChatStream().Recv()
			replyStatus, _ := status.FromError(err)
			if err != nil && replyStatus.Code() == codes.Unavailable {
				ConsoleLog("与服务器的连接被断开, 进行重试")
				robot.chatStream = nil
				time.Sleep(time.Second)
				continue
			}
			utils.CheckErrorPanic(err)
			ConsoleLog(reply.Message)
		}
	}()

	var line string
	for {
		line = Input("")
		if line == "exit" {
			robot.Cancel()
			break
		}
		err = robot.GetChatStream().Send(&pb.SurfRequest{
			Message: line,
		})
		fmt.Print("> ")
		if err != nil {
			ConsoleLog(fmt.Sprintf("there was error sending data. %s", err.Error()))
			continue
		}
	}

}

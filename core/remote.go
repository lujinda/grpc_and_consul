package core

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// RemoteCommand 远程channel类型
type RemoteCommand struct {
	Command string
	Kwargs  map[string]interface{}
}

// RemoteChannel 远程命令通道.
type RemoteChannel struct {
	// 发布命令
	In chan RemoteCommand
	// 接收命令
	Out    chan RemoteCommand
	closed int32
}

// Close 关闭一个RemoteChannel
func (rc *RemoteChannel) Close() {
	if !atomic.CompareAndSwapInt32(&rc.closed, 0, 1) {
		panic(errors.New("RemoteChannel already closed"))
	}
	close(rc.Out)
}

// generateUUID 根据主机名, timestamp, 随机数, pid生成一个uuid
func generateUUID() uint64 {
	rand.Seed(time.Now().UnixNano())
	h := fnv.New64a()
	hostname, _ := os.Hostname()
	h.Write([]byte(fmt.Sprintf("%s%d%d%d%d",
		hostname, time.Now().UnixNano(),
		rand.Uint64(), os.Getpid(), rand.Uint64())))

	return h.Sum64()
}

// NewRemoteChannel 使用consul创建一个远程channel, 利用kv的WaitIndex和CAS来有次序发布RemoteCommand
func NewRemoteChannel(cli *consulapi.Client, name string) *RemoteChannel {
	if name == "" {
		name = "_default"
	}
	in := make(chan RemoteCommand, 1)
	out := make(chan RemoteCommand, 1)
	remoteChannel := &RemoteChannel{
		In:  in,
		Out: out,
	}
	key := fmt.Sprintf("_remote_channel/%s/command", name)
	kv, meta, _ := cli.KV().Get(key, nil)
	if kv == nil {
		cli.KV().Put(&consulapi.KVPair{
			Key:   key,
			Value: []byte{},
		}, nil)
		kv, meta, _ = cli.KV().Get(key, nil)
	}

	var lastIndex = meta.LastIndex
	var uuid = generateUUID()

	// watcher
	go func() {
		var (
			meta    *consulapi.QueryMeta
			err     error
			kv      *consulapi.KVPair
			buf     bytes.Buffer
			decoder *gob.Decoder
			command RemoteCommand
		)
		for atomic.LoadInt32(&remoteChannel.closed) == 0 {
			kv, meta, err = cli.KV().Get(key, &consulapi.QueryOptions{
				WaitIndex: lastIndex,
			})
			if err != nil {
				log.Println("watch event has error:", err)
				continue
			}

			lastIndex = meta.LastIndex
			// 自己的请求不处理
			if kv.Flags == uuid || kv.Flags == 0 {
				continue
			}
			buf.Reset()
			buf.Write(kv.Value)
			decoder = gob.NewDecoder(&buf)
			if err = decoder.Decode(&command); err != nil {
				log.Fatalln("go decode has error.", err)
			}
			in <- command
		}
	}()

	// notifier
	go func() {
		var (
			buf     bytes.Buffer
			encoder *gob.Encoder
			err     error
			success bool
		)
		for command := range out {
			buf.Reset()
			encoder = gob.NewEncoder(&buf)
			encoder.Encode(command)

			for {
				success, _, err = cli.KV().CAS(&consulapi.KVPair{
					Key:         key,
					Value:       buf.Bytes(),
					Flags:       uuid,
					ModifyIndex: lastIndex,
				}, nil)
				if err != nil {
					log.Println("push remote command error.", err)
					continue
				}
				if success {
					break
				}
			}
		}
	}()

	return remoteChannel
}

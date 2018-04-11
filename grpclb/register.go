package grpclb

import (
	"fmt"
	"log"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

// GetAppropriateID 获取最合适的id
// 1. 获取所有有效的Service
// 2. 排出最合适的id
// 3. 以address:port为value, CAS
// 4. 如果更新成功, 则该id被当前server占用. 如果中途有其他机器抢行了, 则CAS 失败, 循环上述操作
func GetAppropriateID(cli *consulapi.Client, address string, port int, serviceName string) string {
	my_value := fmt.Sprintf("%s:%d", address, port)

	for {
		exists_ids := make(map[string]bool)
		entries, _, err := cli.Health().Service(serviceName, "", true, nil)
		if err != nil {
			log.Fatalf("%+v\n", errors.Wrap(err, "Get health services"))
		}
		for _, entry := range entries {
			exists_ids[entry.Service.ID] = true
		}

		// 以顺序选出最合适的id
		number := 1
		for {
			if exists := exists_ids[fmt.Sprintf("%s_%d", serviceName, number)]; !exists {
				break

			} else {
				number++
			}
		}
		id := fmt.Sprintf("%s_%d", serviceName, number)

		// 获取该id下对应的address:port
		kv_key := fmt.Sprintf("id_generator:%s", id)
		kv, meta, err := cli.KV().Get(kv_key, nil)
		if err != nil {
			log.Fatalf("%+v\n", errors.Wrap(err, "Get address for id."))
		}
		// 如果该key在五秒前才创建的, 则可能服务注册还能响应到它. 等下它
		if kv != nil && (time.Now().UnixNano()-int64(kv.Flags) < int64(5*time.Second)) {
			time.Sleep(1 * time.Second)
			continue
		}

		// 如果该key没有创建, 则需要先创建才可使用 CAS
		if kv == nil {
			cli.KV().Put(&consulapi.KVPair{
				Key:   kv_key,
				Value: []byte{},
			}, nil)
			kv, meta, err = cli.KV().Get(kv_key, nil)
		}

		success, _, err := cli.KV().CAS(&consulapi.KVPair{
			Key:         kv_key,
			Value:       []byte(my_value),
			Flags:       uint64(time.Now().UnixNano()),
			ModifyIndex: meta.LastIndex,
		}, nil)
		if err != nil {
			log.Printf("kv cas '%s' error: %v\n", id, err)
		}
		if success {
			return id
		}
	}
}

func RegisterService(cli *consulapi.Client, address string, port int, serviceName string) {
	var err error
	id := GetAppropriateID(cli, address, port, serviceName)
	log.Printf("My ServiceID: %s\n", id)

	registration := &consulapi.AgentServiceRegistration{
		Address: address,
		Port:    port,
		ID:      id,
		Name:    serviceName,
		Check: &consulapi.AgentServiceCheck{
			TTL:     "3s",
			Timeout: "8s",
			CheckID: id,
		},
	}
	err = cli.Agent().ServiceRegister(registration)
	if err != nil {
		log.Fatalln("consul register service", err)
	}
	log.Printf("%s register success\n", registration.ID)

	for range time.Tick(2 * time.Second) {
		err = cli.Agent().UpdateTTL(id, "", "pass")
		if err != nil {
			log.Println("agent upate ttl error:", err)
		}
	}
}

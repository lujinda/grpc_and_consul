package grpclb

import (
	"net"
	"strconv"
	"sync/atomic"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"google.golang.org/grpc/naming"
)

type watchEntry struct {
	addr string
	modi uint64 // modify index
	last uint64 // last index
}

type consulWatcher struct {
	closed    int32
	cli       *consulapi.Client
	service   string
	watched   map[string]*watchEntry
	lastIndex uint64
}

func (w *consulWatcher) Close() {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		panic(errors.New("watcher already closed"))
	}
}

func (w *consulWatcher) Next() ([]*naming.Update, error) {
	var (
		serviceEntries []*consulapi.ServiceEntry
		services       []*consulapi.AgentService
		meta           *consulapi.QueryMeta
		err            error
	)
	updating := make([]*naming.Update, 0)
	lastIndex := w.lastIndex

	for len(updating) == 0 {
		services = make([]*consulapi.AgentService, 0)

		serviceEntries, meta, err = w.cli.Health().Service(w.service, "", true,
			&consulapi.QueryOptions{
				WaitIndex: lastIndex,
			})
		lastIndex = meta.LastIndex

		if err != nil {
			return nil, errors.Wrap(err, "Get health service")
		}

		for _, serviceEntry := range serviceEntries {
			services = append(services, serviceEntry.Service)
		}
		for _, service := range services {
			entry := w.watched[service.ID]
			if entry == nil {
				entry = &watchEntry{
					addr: net.JoinHostPort(service.Address, strconv.Itoa(service.Port)),
					modi: service.ModifyIndex,
				}
				w.watched[service.ID] = entry
				updating = append(updating, &naming.Update{
					Addr: entry.addr,
					Op:   naming.Add,
				})
			} else if entry.modi != service.ModifyIndex { // 表示这个service已经被修改过了, 我们需要更新过来
				updating = append(updating, &naming.Update{
					Op:   naming.Delete,
					Addr: entry.addr,
				})
				entry.modi, entry.addr = service.ModifyIndex, net.JoinHostPort(service.Address, strconv.Itoa(service.Port))
				updating = append(updating, &naming.Update{
					Op:   naming.Delete,
					Addr: entry.addr,
				})
			}
			entry.last = lastIndex
		}

		for id, entry := range w.watched {
			if entry.last != lastIndex {
				delete(w.watched, id)
				updating = append(updating, &naming.Update{
					Op:   naming.Delete,
					Addr: entry.addr,
				})
			}
		}

	}
	return updating, nil
}

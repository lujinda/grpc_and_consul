package grpclb

import (
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/naming"
)

type consulResolver struct {
	cli *consulapi.Client
}

func (r *consulResolver) Resolve(target string) (naming.Watcher, error) {
	return &consulWatcher{
		cli:     r.cli,
		service: target,
		watched: make(map[string]*watchEntry),
	}, nil
}

func NewConsulResolver(cli *consulapi.Client) naming.Resolver {
	return &consulResolver{
		cli: cli,
	}
}

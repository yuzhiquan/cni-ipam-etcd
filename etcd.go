package etcd

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/facebookgo/errgroup"
	log "github.com/sirupsen/logrus"
)

const timeOut = 60
const lastIPEtcdPrefix = "last_reserved_ip."
const etcdPrefix = "/conf/odin-cni/ipam/"

type EtcdStore struct {
	client  *clientv3.Client
	Session *concurrency.Session
	TimeOut time.Duration
	Prefix  string
	mutex   *concurrency.Mutex
}

/*
	https://github.com/containernetworking/plugins/blob/master/plugins/ipam/host-local/backend/store.go
	Lock() error
	Unlock() error
	Close() error
	Reserve(id string, ip net.IP, rangeID string) (bool, error)
	LastReservedIP(rangeID string) (net.IP, error)
	Release(ip net.IP) error
	ReleaseByID(id string) error

*/

func NewEtcdStore(endpoints []string, key string) (*EtcdStore, error) {
	if endpoints == nil {
		return nil, fmt.Errorf("etcd endpoints cannot be nil")
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		return nil, err
	}

	if key == "" {
		key = etcdPrefix
	}

	return &EtcdStore{
		client:  cli,
		TimeOut: timeOut * time.Second,
		Prefix:  key,
	}, nil
}

func (e *EtcdStore) DoSession() {
	session, err := concurrency.NewSession(e.client)
	if err != nil {
		log.Fatal(err)
	}
	e.Session = session
	e.mutex = concurrency.NewMutex(e.Session, e.Prefix)
}

func (e *EtcdStore) Lock() error {
	if e.Session == nil || e.mutex == nil {
		return fmt.Errorf("session or mutex is nil")
	}
	return e.mutex.Lock(context.TODO())
}

func (e *EtcdStore) Unlock() error {
	if e.Session == nil || e.mutex == nil {
		return fmt.Errorf("session or mutex is nil")
	}
	return e.mutex.Unlock(context.TODO())
}

func (e *EtcdStore) Close() error {
	return errgroup.NewMultiError(
		e.Session.Close(),
		e.client.Close(),
	)
}

func (e *EtcdStore) Release(ip net.IP) error {
	return e.delKV(ip)
}

func (e *EtcdStore) ReleaseByID(id string) error {
	resp, err := e.getKV(e.Prefix)
	if err != nil {
		return fmt.Errorf("GET etcd key err:%s", err)
	}
	for _, value := range resp.Kvs {
		if string(value.Value) == id {
			return e.delKey(string(value.Key))
		}
	}
	return nil
}

func (e *EtcdStore) Reserve(id string, ip net.IP, rangeID string) (bool, error) {

	resp, err := e.getKV(e.Prefix + ip.String())
	log.Printf("[odin-ipam] want reserve ip:%s", ip.String())
	if err != nil {
		return false, err
	} else if len(resp.Kvs) > 0 {
		return false, fmt.Errorf("%s already exist,err:%+v", ip.String(), resp)
	}

	if err := e.putKV(e.Prefix+ip.String(), id); err != nil {
		return false, err
	}

	lastIPKey := e.Prefix + lastIPEtcdPrefix + rangeID

	if err := e.putKV(lastIPKey, ip.String()); err != nil {
		return false, err
	}
	return true, nil
}

func (e *EtcdStore) LastReservedIP(rangeID string) (net.IP, error) {
	lastIPKey := e.Prefix + lastIPEtcdPrefix + rangeID
	resp, err := e.getKV(lastIPKey)

	if err != nil || len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("cannot find last ip in %s", rangeID)
	}

	return net.ParseIP(string(resp.Kvs[0].Value)), nil
}

func (e *EtcdStore) getKV(key string) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.TimeOut)
	defer cancel()
	return e.client.Get(ctx, key, clientv3.WithPrefix())
}

func (e *EtcdStore) putKV(key, val string) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.TimeOut)

	_, err := e.client.Put(ctx, key, val)

	if err != nil {
		return err
	}
	cancel()
	return nil
}

func (e *EtcdStore) delKV(ip net.IP) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.TimeOut)
	_, err := e.client.Delete(ctx, e.Prefix+ip.String())

	defer cancel()
	if err != nil {
		return err
	}

	return nil
}

func (e *EtcdStore) delKey(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.TimeOut)
	_, err := e.client.Delete(ctx, key)

	defer cancel()
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdStore) empltyPrefix() error {
	resp, err := e.getKV(e.Prefix)
	if err != nil {
		return fmt.Errorf("GET etcd key err:%s", err)
	}

	for _, value := range resp.Kvs {
		e.delKey(string(value.Key))
	}

	return nil
}

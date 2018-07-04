package etcd

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestEtcdStore_Reserve(t *testing.T) {
	endpoint := []string{
		"http://conf01.paas.tc.ted:2379",
	}
	s, err := NewEtcdStore(endpoint, "/venus/network/cni/ipam/")
	if err != nil {
		t.Error(err)
	}

	s.DoSession()
	s.TimeOut = 60 * time.Second
	ip := net.IP{}
	ip = []byte("10.33.12.34")
	b, err := s.Reserve("xxxxxx22xxx", ip, "0")
	fmt.Println("123", b)
	if err != nil {
		t.Error(err)
	}

	err = s.Release(ip)
	if err != nil {
		t.Error(err)
	}

	_, err = s.getKV("/venus/network/cni/ipam/")
	if err != nil {
		t.Error(err)
	}

	_, err = s.getKV(etcdPrefix + "last_reserved_ip.0")
	if err != nil {
		t.Error(err)
	}

	_, err = s.LastReservedIP("0")
	if err != nil {
		t.Error(err)
	}

	err = s.ReleaseByID("example66")
	if err != nil {
		t.Error(err)
	}

	err = s.empltyPrefix()
	if err != nil {
		t.Error(err)
	}
}

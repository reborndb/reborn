package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test starting/stopping a server
func TestServer(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	_, err = c.Do("PING")
	ok(t, err)

	// A single client
	equals(t, 1, s.CurrentConnectionCount())
	equals(t, 1, s.TotalConnectionCount())
	equals(t, 1, s.CommandCount())
	_, err = c.Do("PING")
	ok(t, err)
	equals(t, 2, s.CommandCount())
}

func TestMultipleServers(t *testing.T) {
	s1, err := Run()
	ok(t, err)
	s2, err := Run()
	ok(t, err)
	if s1.Addr() == s2.Addr() {
		t.Fatal("Non-unique addresses", s1.Addr(), s2.Addr())
	}

	s2.Close()
	s1.Close()
	// Closing multiple times is fine
	go s1.Close()
	go s1.Close()
	s1.Close()
}

func TestRestart(t *testing.T) {
	s, err := Run()
	ok(t, err)
	addr := s.Addr()

	s.Set("color", "red")

	s.Close()
	err = s.Restart()
	ok(t, err)
	if s.Addr() != addr {
		t.Fatal("should be the same address")
	}

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	_, err = c.Do("PING")
	ok(t, err)

	red, err := redis.String(c.Do("GET", "color"))
	ok(t, err)
	if have, want := red, "red"; have != want {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

package netchan

import (
	"testing"
)

func TestNetchan(t *testing.T) {
	l, err := Listen("tcp", "localhost:8080")
	if err != nil {
		t.Fatalf("no listener: %v", err)
	}
	ch := make(chan string)
	addr, err := l.Publish("foo", ch)
	if err != nil {
		t.Errorf("error publishing: %v", err)
	}

	endpt := make(chan string)
	err = Dial(addr, endpt)
	if err != nil {
		t.Errorf("error dialing: %v", err)
	}

	data := []string{
		"Foo!",
		"Bar!",
		"Baz!",
	}

	go func() {
		for _, s := range data {
			endpt <- s
		}
	}()

	for _, s := range data {
		recv := <-ch
		if recv != s {
			t.Errorf("Got %s, expecting %s", recv, s)
		}
	}
	l.Unpublish(addr)
	// Make sure the channel is closed
	_, ok := <-ch
	if ok {
		t.Errorf("Channel not properly closed")
	}
}

func TestNewName(t *testing.T) {
	N := 100
	m := make(map[string]bool)
	for i := 0; i<N; i++ {
		s := NewName()
		if b, ok := m[s]; b || ok {
			t.Errorf("NewName returned %s twice", s)
		}
	}
}

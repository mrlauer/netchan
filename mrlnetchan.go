// Package netchan makes Go channels available for communication over
// a network.
//
// This is a crappy implementation of the interface proposed by Rob Pike.
// It is not to be taken seriously.
package mrlnetchan

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	"sync"
)

// Addr represents a network address for a single networked channel.
type Addr struct {
	// Net is the network protocol: "tcp"
	Net string
	// Addr is the network address: "machine:1234"
	Addr string
	// Name identifies the individual channel at this network address: "name"
	Name string // "theChannelName"
}

// Listener provides the functionality to publish and manage networked
// channels.
type Listener struct {
	listener net.Listener
	channels map[string]*channelData
	mutex    sync.RWMutex
}

// A listener may have many named channels, each of which
// may have many connections
type channelData struct {
	name        string
	channel     interface{}
	dataType    reflect.Type
	mutex       sync.RWMutex
	connections []net.Conn // maybe should be a map, with remote addr as key
}

// send sends data to the channel. It is its own function to capture
// any panics.
func (cd *channelData) send(val reflect.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch rt := r.(type) {
			case error:
				err = rt
			default:
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	reflect.ValueOf(cd.channel).Send(val)
	return nil
}

// Message types
type connectMsg struct {
	Addr Addr
}

type disconnectMsg struct {
	Addr Addr
}

type dataMsg struct {
	Addr Addr
}

// Listen establishes a Listener on this machine with the specified
// network and address.
func Listen(tp, addr string) (*Listener, error) {
	ln, err := net.Listen(tp, addr)
	if err != nil {
		return nil, err
	}
	l := &Listener{listener: ln}
	// Start a goroutine listening for connections
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// We're done! Should maybe log the error if it wasn't caused on purpose.
				return
			}
			// set up and run the connection.
			go l.run(conn)
		}
	}()
	return l, nil
}

func (l *Listener) log(err error) {
	log.Printf("%v\n", err)
}

func (l *Listener) addConn(name string, conn net.Conn) (*channelData, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	chData := l.channels[name]
	if chData == nil {
		return nil, fmt.Errorf("No published channel named %s", name)
	}
	chData.mutex.Lock()
	defer chData.mutex.Unlock()
	chData.connections = append(chData.connections, conn)
	return chData, nil
}

func (l *Listener) removeConn(name string, conn net.Conn) error {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	chData := l.channels[name]
	if chData == nil {
		return fmt.Errorf("No published channel named %s", name)
	}
	chData.mutex.Lock()
	defer chData.mutex.Unlock()
	for i, c := range chData.connections {
		if c == conn {
			chData.connections = append(chData.connections[:i], chData.connections[i+1:]...)
			return nil
		}
	}
	return errors.New("Could not find connection to remove")
}

func (l *Listener) run(conn net.Conn) {
	// A gob decoder.
	decoder := gob.NewDecoder(conn)
	// We expect a connect message.
	var connMsg connectMsg
	err := decoder.Decode(&connMsg)
	if err != nil {
		// log the error
		conn.Close()
		return
	}
	name := connMsg.Addr.Name
	chData, err := l.addConn(name, conn)
	if err != nil {
		// log the error
		return
	}
	// A place to put data
	data := reflect.New(chData.dataType)
	// Now sit in a for loop awaiting messages
	for {
		var msg interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			// Should discriminate between gob and io messages. Eh.
			// Barf.
			l.removeConn(name, conn)
			return
		}
		switch msg.(type) {
		case dataMsg:
			// We read the value.
			err := decoder.Decode(data)
			if err != nil {
				// log the error. Proceed.
			} else {
				// Send it along to the channel.
				chData.send(data.Elem())
			}
		case disconnectMsg:
			l.removeConn(name, conn)
		}
	}
}

// Publish makes the channel available for communication to other
// machines. Although the type of the argument is interface{}, the
// channel must be a bidirectional channel such as chan string. The
// return value is the Addr that other machines can use to connect
// to the channel and send data to it. After Publish, other machines
// on the network can send to the channel and this machine can receive
// those messages. Also, after Publish only the receive end of the
// channel should be used by the caller.
func (l *Listener) Publish(name string, channel interface{}) (Addr, error) {
	return Addr{}, errors.New("Not implemented")
}

// Unpublish removes network address for the channel with the specified
// address. After Unpublish returns, no further messages can be
// received on the channel (until it is published again).
func (l *Listener) Unpublish(addr Addr) error {
	return errors.New("Not implemented")
}

// Dial connects the channel to the networked channel with the specified
// address. Although the type of the argument is interface{}, the
// channel must be a bidirectional channel, such as chan string, and
// the type of the element must be compatible with the type of the
// channel being connected to. (Details of compatibility TBD.) After
// Dial, this machine can send data on the channel to be received by
// the published network channel on the remote machine. Also, after
// Dial only the send end of the channel should be used by the caller.
func Dial(addr Addr, channel interface{}) error {
	return errors.New("Not implemented")
}

// NewName returns a name that is guaranteed with extremely high
// probability to be globally unique.
func NewName() string {
	return ""
}

/*

// Simple example; for brevity errors are listed but not examined.

type msg1 struct {
        Addr Addr
        Who string
}
type msg2 struct {
        Greeting string
        Who string
}

func server() {
        // Announce a greeting service.
        l, err := netchan.Listen(":12345")
        c1 := make(chan msg1)
        addr, err := l.Publish(c1, "GreetingService")
        for {
                // Receive a message from someone who wants to be greeted.
                m1 := <-c1
                // Create a new channel and connect it to the client.
                c2 := make(chan msg2)
                err = netchan.Dial(m1.Addr, c2)
                // Send the greeting.
                c2 <- msg2{"Hello", m1.Who}
                // Close the channel.
                close(c2)
        }
}

func client() {
        // Announce a service so we can be contacted by the greeting service.
        l, err := netchan.Listen(":23456")
        // The name of the greeting service must be known separately.
        greet := netchan.Addr{"tcp", "server.com:12345", "GreetingService"}
        c1 := make(chan msg1)
        c2 := make(chan msg2)
        // Connect to the greeting service and ask for a message.
        err = netchan.Dial(greet, c1)
        // Publish a place to receive the greeting before we ask for it.
        addr, err := netchan.Publish(c2, netchan.NewName())
        c1 <- msg1{addr, "Eduardo"}
        // Receive the message and tear down.
        reply := <-c2
        fmt.Println(reply.Greeting, ", ", reply.Who) // "Hello, Eduardo"
        // Tell the library we're done with this channel.
        netchan.Unpublish(c2)
        // No more data will be sent on c2.
        close(c2)
}

*/

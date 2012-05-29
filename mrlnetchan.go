// Package netchan makes Go channels available for communication over
// a network.
//
// This is a crappy implementation of the interface proposed by Rob Pike.
// It is not to be taken seriously.
package netchan

import (
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"runtime"
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
	net      string // protocol
	addr     string // address
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

type statusMsg struct {
	Addr  Addr
	Error error
}

func (msg statusMsg) Ok() bool {
	return msg.Error == nil
}

func init() {
	gob.Register(dataMsg{})
	gob.Register(disconnectMsg{})
}

// Listen establishes a Listener on this machine with the specified
// network and address.
func Listen(tp, addr string) (*Listener, error) {
	ln, err := net.Listen(tp, addr)
	if err != nil {
		return nil, err
	}
	l := &Listener{
		net:      tp,
		addr:     addr,
		listener: ln,
		channels: make(map[string]*channelData),
	}
	// Start a goroutine listening for connections
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if err != io.EOF {
					l.log(err)
				}
				return
			}
			// set up and run the connection.
			go l.run(conn)
		}
	}()
	return l, nil
}

func (l *Listener) log(err error) {
	_, file, line, _ := runtime.Caller(1)
	log.Printf("At %s, %d: (%T) %v\n", file, line, err, err)
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
	encoder := gob.NewEncoder(conn)
	chData, err := l.addConn(name, conn)
	if err != nil {
		l.log(err)
		// Send a failure message, then close the connection
		msg := statusMsg{
			Addr:  connMsg.Addr,
			Error: err,
		}
		encoder.Encode(msg)
		conn.Close()
		return
	}
	// Send a simple acknowledgement
	msg := statusMsg{Addr: connMsg.Addr}
	encoder.Encode(msg)
	// A place to put data
	data := reflect.New(chData.dataType)
	// Now sit in a for loop awaiting messages
	defer l.removeConn(name, conn)
	for {
		var msg interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			// Should discriminate between gob and io messages. Eh.
			// Ugliness: if the connection closes because of unpublish, then
			// we'll get an error here. It would be nicer if we had a more elegant way
			// to break out of the loop.
			l.log(err)
			return
		}
		switch msg.(type) {
		case dataMsg:
			// We read the value.
			err := decoder.DecodeValue(data)
			if err != nil {
				l.log(err)
			} else {
				// Send it along to the channel.
				chData.send(data.Elem())
			}
		case disconnectMsg:
			return
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
	l.mutex.Lock()
	defer l.mutex.Unlock()
	// see if it's already there
	if _, ok := l.channels[name]; ok {
		return Addr{}, fmt.Errorf("%s already published", name)
	}
	// Make sure channel really is a channel, and get its type
	chval := reflect.ValueOf(channel)
	if err := isUsableChannel(channel); err != nil {
		return Addr{}, err
	}
	tp := chval.Type().Elem()
	chData := channelData{
		name:     name,
		channel:  channel,
		dataType: tp,
	}
	l.channels[name] = &chData
	return Addr{Net: l.net, Addr: l.addr, Name: name}, nil
}

func isUsableChannel(ch interface{}) error {
	chval := reflect.ValueOf(ch)
	if chval.Kind() != reflect.Chan || chval.Type().ChanDir() != reflect.BothDir {
		return fmt.Errorf("Publish needs a bidirectional channel")
	}
	return nil
}

// Unpublish removes network address for the channel with the specified
// address. After Unpublish returns, no further messages can be
// received on the channel (until it is published again).
func (l *Listener) Unpublish(addr Addr) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	chData := l.channels[addr.Name]
	if chData != nil {
		// close all the open connections
		chData.mutex.Lock()
		for _, conn := range chData.connections {
			conn.Close()
		}
		chData.mutex.Unlock()

		// Close the channel
		reflect.ValueOf(chData.channel).Close()
		// Remove the data
		delete(l.channels, addr.Name)
		return nil
	}
	return fmt.Errorf("%s is not published", addr.Name)
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
	if err := isUsableChannel(channel); err != nil {
		return err
	}
	conn, err := net.Dial(addr.Net, addr.Addr)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	// Send a start message
	msg := connectMsg{Addr: addr}
	err = encoder.Encode(msg)
	if err != nil {
		return err
	}
	// Wait for the acknowledgement
	var ack statusMsg
	err = decoder.Decode(&ack)
	if err != nil {
		return err
	}
	if !ack.Ok() {
		return ack.Error
	}
	errs := getErrorChannel(addr)
	// Start a goroutine listening to the channel
	go func() {
		defer releaseErrorChannel(addr)
		chval := reflect.ValueOf(channel)

		// Start a goroutine listening for stuff.
		// Right now just shut things down at eof
		// by closing the error channel. That is probably not right.
		go func() {
			buffer := make([]byte, 128)
			_, err := conn.Read(buffer)
			errs <- err
			close(errs)
		}()

		for {
			val, ok := chval.Recv()
			if !ok {
				// Channel was closed. We're done.
				var msg interface{} = disconnectMsg{addr}
				encoder.Encode(&msg)
				return
			}
			// Send the value
			var msg interface{} = dataMsg{addr}
			err := encoder.Encode(&msg)
			if err != nil {
				errs <- err
				return
			}
			err = encoder.EncodeValue(val)
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	return nil
}

// Errors channels. They are buffered
type errChanData struct {
	Chan  chan error
	Count int
}

var errChans map[Addr]*errChanData
var errChanMutex sync.RWMutex

func init() {
	errChans = make(map[Addr]*errChanData)
}

func getErrorChannel(addr Addr) chan error {
	errChanMutex.Lock()
	defer errChanMutex.Unlock()
	chdat := errChans[addr]
	if chdat == nil {
		ch := make(chan error, 10)
		chdat = &errChanData{Chan: ch}
		errChans[addr] = chdat
	}
	chdat.Count++
	return chdat.Chan
}

func releaseErrorChannel(addr Addr) {
	errChanMutex.Lock()
	defer errChanMutex.Unlock()
	chdat := errChans[addr]
	if chdat != nil {
		chdat.Count--
		if chdat.Count <= 0 {
			close(chdat.Chan)
			delete(errChans, addr)
		}
	}
}

// Errors returns a channel for errors in the connection.
// If the connection is dropped because of anything other than closing
// the channel on the client side, then some appropriate error will be
// returned here. The channel will always be closed when a connection ends.
func Errors(addr Addr) chan error {
	errChanMutex.RLock()
	defer errChanMutex.RUnlock()
	chdata := errChans[addr]
	if chdata != nil {
		return chdata.Chan
	}
	return nil
}

// NewName returns a name that is guaranteed with extremely high
// probability to be globally unique.
func NewName() string {
	const sz = 16
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
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

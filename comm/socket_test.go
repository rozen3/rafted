package comm

import (
    "fmt"
    "github.com/hhkbp2/rafted/str"
    "github.com/hhkbp2/testify/assert"
    "github.com/hhkbp2/testify/require"
    "io"
    "net"
    "testing"
)

type TestSocketServer struct {
    host     string
    port     int
    listener net.Listener

    connHandler func(net.Conn)
}

func NewTestSocketServer(
    host string, port int, connHandler func(net.Conn)) (*TestSocketServer, error) {

    listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
    if err != nil {
        return nil, err
    }

    object := &TestSocketServer{
        host:        host,
        port:        port,
        listener:    listener,
        connHandler: connHandler,
    }
    return object, nil
}

func (self *TestSocketServer) Serve() {
    routine := func() {
        for {
            conn, err := self.listener.Accept()
            if err != nil {
                return
            }
            go self.connHandler(conn)
        }
    }
    go routine()
}

func (self *TestSocketServer) Close() error {
    return self.listener.Close()
}

const (
    TestSocketHost = "localhost"
    TestSocketPort = 33333
)

func TestSocketTransport(t *testing.T) {
    size := 100
    message := str.RandomString(uint32(size))
    handler := func(conn net.Conn) {
        defer conn.Close()
        buf := make([]byte, 1024)
        n, err := ReadN(conn, buf[:size])
        assert.True(t, (err == io.EOF) || (err == nil))
        assert.Equal(t, size, n)
        assert.Equal(t, message, string(buf[:n]))
        n, err = WriteN(conn, buf[:n])
        assert.Nil(t, err)
        assert.Equal(t, size, n)
    }
    server, err := NewTestSocketServer(TestSocketHost, TestSocketPort, handler)
    require.Nil(t, err)
    server.Serve()
    defer server.Close()

    addr, err := net.ResolveTCPAddr(
        "tcp", fmt.Sprintf("%s:%d", TestSocketHost, TestSocketPort))
    transport := NewSocketTransport(addr)
    err = transport.Open()
    require.Nil(t, err)
    n, err := WriteN(transport, []byte(message))
    require.Nil(t, err)
    buf := make([]byte, size)
    n, err = ReadN(transport, buf)
    require.Nil(t, err)
    require.Equal(t, size, n)
    require.Equal(t, message, string(buf))
    err = transport.Close()
    require.Nil(t, err)
}

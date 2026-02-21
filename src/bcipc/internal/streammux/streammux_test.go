package streammux

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"blobcache.io/blobcache/src/internal/bcp"
	"github.com/stretchr/testify/require"
)

func TestSingleResponse(t *testing.T) {
	c1, c2 := net.Pipe()
	client := New(c1)
	server := New(c2)
	defer client.Close()
	defer server.Close()

	var req bcp.Message
	req.SetCode(bcp.MT_PING)
	req.SetBody([]byte("hello"))

	stream, err := client.Open(req)
	require.NoError(t, err)

	msg, rw, err := server.Accept()
	require.NoError(t, err)
	require.Equal(t, bcp.MessageType(bcp.MT_PING), msg.Header().Code())
	require.Equal(t, "hello", string(msg.Body()))

	var resp bcp.Message
	resp.SetCode(bcp.MT_OK)
	resp.SetBody([]byte("world"))
	require.NoError(t, rw.Send(resp, true))

	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, bcp.MessageType(bcp.MT_OK), got.Header().Code())
	require.Equal(t, "world", string(got.Body()))

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestMultiResponse(t *testing.T) {
	c1, c2 := net.Pipe()
	client := New(c1)
	server := New(c2)
	defer client.Close()
	defer server.Close()

	var req bcp.Message
	req.SetCode(bcp.MT_PING)

	stream, err := client.Open(req)
	require.NoError(t, err)

	_, rw, err := server.Accept()
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		var resp bcp.Message
		resp.SetCode(bcp.MT_OK)
		resp.SetBody([]byte{byte(i)})
		fin := i == 2
		require.NoError(t, rw.Send(resp, fin))
	}

	for i := 0; i < 3; i++ {
		got, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, bcp.MessageType(bcp.MT_OK), got.Header().Code())
		require.Equal(t, []byte{byte(i)}, got.Body())
	}

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestConcurrentStreams(t *testing.T) {
	c1, c2 := net.Pipe()
	client := New(c1)
	server := New(c2)
	defer client.Close()
	defer server.Close()

	const n = 10
	errs := make(chan error, n*2)

	// Server goroutine: accept and echo back
	go func() {
		for i := 0; i < n; i++ {
			msg, rw, err := server.Accept()
			if err != nil {
				errs <- fmt.Errorf("accept: %w", err)
				return
			}
			var resp bcp.Message
			resp.SetCode(bcp.MT_OK)
			resp.SetBody(msg.Body())
			if err := rw.Send(resp, true); err != nil {
				errs <- fmt.Errorf("send: %w", err)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var req bcp.Message
			req.SetCode(bcp.MT_PING)
			req.SetBody([]byte{byte(i)})

			stream, err := client.Open(req)
			if err != nil {
				errs <- fmt.Errorf("open %d: %w", i, err)
				return
			}
			got, err := stream.Recv()
			if err != nil {
				errs <- fmt.Errorf("recv %d: %w", i, err)
				return
			}
			if got.Header().Code() != bcp.MT_OK {
				errs <- fmt.Errorf("stream %d: expected MT_OK, got %d", i, got.Header().Code())
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

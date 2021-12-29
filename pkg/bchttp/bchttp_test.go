package bchttp

func TestClienServer(t *testing.T) {
	blobcachetest.TestService(t, func(t testing.TB) blobcache.Service {
		n := blobcache.NewNode(NewMemParams())
		l, err := net.Listen("tcp", "127.0.0.1:")
		require.NoError(t, err)
		hs := NewServer(n)
		go http.Serve(l, hs)
		t.Cleanup(func() { hs.Stop() })
		return NewClient(n, "http://"+l.Addr())
	})
}

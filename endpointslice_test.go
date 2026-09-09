package kuberesolver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
)

type recordingConn struct {
	resolver.ClientConn
	states []resolver.State
}

func (c *recordingConn) UpdateState(state resolver.State) error {
	c.states = append(c.states, state)
	return nil
}

func testSlice(name string, addresses ...string) EndpointSlice {
	ready := true
	slice := EndpointSlice{Metadata: Metadata{Name: name}}
	for _, address := range addresses {
		slice.Endpoints = append(slice.Endpoints, Endpoint{
			Addresses: []string{address}, Conditions: EndpointConditions{Ready: &ready},
		})
	}
	return slice
}

func newRecordingResolver(t *testing.T) (*kResolver, *recordingConn) {
	t.Helper()
	c := &recordingConn{}
	timer := time.NewTimer(time.Hour)
	t.Cleanup(func() { timer.Stop() })
	return &kResolver{
		ctx: context.Background(), cc: c,
		target: targetInfo{serviceName: "app", serviceNamespace: "default", port: "8443"},
		t:      timer, freq: time.Hour,
		endpoints:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_endpoints"}),
		addresses:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_addresses"}),
		lastUpdateUnix: prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_last_update"}),
	}, c
}

func requireAddresses(t *testing.T, state resolver.State, want ...string) {
	t.Helper()
	got := make([]string, 0, len(state.Addresses))
	for _, address := range state.Addresses {
		got = append(got, address.Addr)
		require.Equal(t, "app.default", address.ServerName)
	}
	require.Equal(t, want, got)
}

func TestResolverCombinesSlicesAndReplacesUpdates(t *testing.T) {
	r, c := newRecordingResolver(t)
	r.handle(testSlice("one", "192.0.2.2", "192.0.2.1"))
	r.handle(testSlice("two", "192.0.2.3", "192.0.2.1"))
	requireAddresses(t, c.states[len(c.states)-1], "192.0.2.1:8443", "192.0.2.2:8443", "192.0.2.3:8443")
	require.Equal(t, float64(4), testutil.ToFloat64(r.endpoints))
	require.Equal(t, float64(3), testutil.ToFloat64(r.addresses))

	r.handle(testSlice("one", "192.0.2.4"))
	requireAddresses(t, c.states[len(c.states)-1], "192.0.2.1:8443", "192.0.2.3:8443", "192.0.2.4:8443")
	r.handle(testSlice("two"))
	requireAddresses(t, c.states[len(c.states)-1], "192.0.2.4:8443")
}

func TestResolverPublishesEmptyWhenAllEndpointsBecomeUnready(t *testing.T) {
	r, c := newRecordingResolver(t)
	slice := testSlice("one", "192.0.2.1")
	r.handle(slice)
	notReady := false
	slice.Endpoints[0].Conditions.Ready = &notReady
	r.handle(slice)
	require.Len(t, c.states, 2)
	require.Empty(t, c.states[1].Addresses)
	require.Zero(t, testutil.ToFloat64(r.addresses))
	require.Positive(t, testutil.ToFloat64(r.lastUpdateUnix))
}

func TestResolverPublishesEmptySliceWithoutPorts(t *testing.T) {
	r, c := newRecordingResolver(t)
	r.target.port = ""
	r.target.useFirstPort = true
	slice := testSlice("one", "192.0.2.1")
	slice.Ports = []EndpointPort{{Port: 8443}}
	r.handle(slice)
	r.handle(testSlice("one"))
	require.Len(t, c.states, 2)
	require.Empty(t, c.states[1].Addresses)
}

func TestResolverListReplacesSnapshotAndPublishesEmpty(t *testing.T) {
	for _, test := range []struct {
		name  string
		items []EndpointSlice
		want  []string
	}{
		{"multiple slices", []EndpointSlice{testSlice("one", "192.0.2.1"), testSlice("two", "192.0.2.2")}, []string{"192.0.2.1:8443", "192.0.2.2:8443"}},
		{"empty list", nil, []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				_ = json.NewEncoder(w).Encode(EndpointSliceList{Metadata: Metadata{ResourceVersion: "12"}, Items: test.items})
			}))
			defer api.Close()
			r, c := newRecordingResolver(t)
			r.k8sClient = NewInsecureK8sClient(api.URL)
			r.handle(testSlice("old", "192.0.2.99"))
			c.states = nil
			r.resolve()
			require.Len(t, c.states, 1)
			requireAddresses(t, c.states[0], test.want...)
		})
	}
}

func TestResolverKeepsSnapshotWhenListFails(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer api.Close()
	r, c := newRecordingResolver(t)
	r.k8sClient = NewInsecureK8sClient(api.URL)
	r.handle(testSlice("one", "192.0.2.1"))
	r.resolve()
	require.Len(t, c.states, 1)
	r.handle(testSlice("two", "192.0.2.2"))
	requireAddresses(t, c.states[len(c.states)-1], "192.0.2.1:8443", "192.0.2.2:8443")
}

type watchingConn struct {
	resolver.ClientConn
	states chan resolver.State
}

func (c *watchingConn) UpdateState(state resolver.State) error {
	c.states <- state
	return nil
}

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for resolver activity")
		var zero T
		return zero
	}
}

func TestResolverWatchUsesSnapshotVersionAndRefreshesCache(t *testing.T) {
	lists := make(chan EndpointSliceList, 3)
	lists <- EndpointSliceList{Metadata: Metadata{ResourceVersion: "10"}, Items: []EndpointSlice{testSlice("one", "192.0.2.1"), testSlice("two", "192.0.2.2")}}
	lists <- EndpointSliceList{Metadata: Metadata{ResourceVersion: "20"}, Items: []EndpointSlice{testSlice("three", "192.0.2.3")}}
	lists <- EndpointSliceList{Metadata: Metadata{ResourceVersion: "30"}}
	versions := make(chan string, 3)
	events := make(chan *Event)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("labelSelector") != "kubernetes.io/service-name=app" {
			http.Error(w, "missing service selector", http.StatusBadRequest)
			return
		}
		switch req.URL.Path {
		case "/apis/discovery.k8s.io/v1/namespaces/default/endpointslices":
			select {
			case list := <-lists:
				_ = json.NewEncoder(w).Encode(list)
			case <-req.Context().Done():
			}
		case "/apis/discovery.k8s.io/v1/watch/namespaces/default/endpointslices":
			versions <- req.URL.Query().Get("resourceVersion")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			for {
				select {
				case event := <-events:
					if event == nil {
						return
					}
					_ = json.NewEncoder(w).Encode(event)
					w.(http.Flusher).Flush()
				case <-req.Context().Done():
					return
				}
			}
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(api.Close)
	c := &watchingConn{states: make(chan resolver.State, 16)}
	b := NewBuilder(NewInsecureK8sClient(api.URL), kubernetesSchema)
	r, err := b.Build(parseTarget("kubernetes:///app.default:8443"), c, resolver.BuildOptions{})
	require.NoError(t, err)
	t.Cleanup(r.Close)
	requireAddresses(t, receive(t, c.states), "192.0.2.1:8443", "192.0.2.2:8443")
	require.Equal(t, "10", receive(t, versions))
	events <- &Event{Type: Modified, Object: testSlice("one")}
	requireAddresses(t, receive(t, c.states), "192.0.2.2:8443")
	events <- nil
	requireAddresses(t, receive(t, c.states), "192.0.2.3:8443")
	require.Equal(t, "20", receive(t, versions))

	r.(*kResolver).t.Reset(0)
	require.Empty(t, receive(t, c.states).Addresses)
	require.Equal(t, "30", receive(t, versions))
}

func TestResolverCloseCancelsInitialList(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(started)
		<-req.Context().Done()
		close(canceled)
	}))
	t.Cleanup(api.Close)
	c := &watchingConn{states: make(chan resolver.State, 1)}
	b := NewBuilder(NewInsecureK8sClient(api.URL), kubernetesSchema)
	r, err := b.Build(parseTarget("kubernetes:///app.default:8443"), c, resolver.BuildOptions{})
	require.NoError(t, err)
	t.Cleanup(r.Close)
	receive(t, started)
	closed := make(chan struct{})
	go func() {
		r.Close()
		close(closed)
	}()
	receive(t, closed)
	receive(t, canceled)
}

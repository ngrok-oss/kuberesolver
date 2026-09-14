package kuberesolver

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

const (
	kubernetesSchema = "kubernetes"
	defaultFreq      = time.Minute * 30
)

var (
	endpointsForTarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_endpoints_total",
			Help: "The number of endpoints for a given target",
		},
		[]string{"target"},
	)
	addressesForTarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_addresses_total",
			Help: "The number of addresses for a given target",
		},
		[]string{"target"},
	)
	clientLastUpdate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_client_last_update",
			Help: "The last time the resolver client was updated",
		},
		[]string{"target"},
	)
)

type targetInfo struct {
	scheme            string
	serviceName       string
	serviceNamespace  string
	port              string
	resolveByPortName bool
	useFirstPort      bool
}

func (ti targetInfo) String() string {
	if ti.scheme != "" {
		return fmt.Sprintf("%s://%s/%s:%s", ti.scheme, ti.serviceNamespace, ti.serviceName, ti.port)
	} else {
		return fmt.Sprintf("kubernetes://%s/%s:%s", ti.serviceNamespace, ti.serviceName, ti.port)
	}
}

// RegisterInCluster registers the kuberesolver builder to grpc with kubernetes schema
func RegisterInCluster() {
	RegisterInClusterWithSchema(kubernetesSchema)
}

// RegisterInClusterWithSchema registers the kuberesolver builder to the grpc with custom schema
func RegisterInClusterWithSchema(schema string) {
	resolver.Register(NewBuilder(nil, schema))
}

// NewBuilder creates a kubeBuilder which is used by grpc resolver.
func NewBuilder(client K8sClient, schema string) resolver.Builder {
	return &kubeBuilder{
		k8sClient: client,
		schema:    schema,
	}
}

type kubeBuilder struct {
	k8sClient K8sClient
	schema    string
}

func splitServicePortNamespace(hpn string) (service, port, namespace string) {
	service = hpn

	colon := strings.LastIndexByte(service, ':')
	if colon != -1 {
		service, port = service[:colon], service[colon+1:]
	}

	// we want to split into the service name, namespace, and whatever else is left
	// this will support fully qualified service names, e.g. {service-name}.<namespace>.svc.<cluster-domain-name>.
	// Note that since we lookup the endpoints by service name and namespace, we don't care about the
	// cluster-domain-name, only that we can parse out the service name and namespace properly.
	parts := strings.SplitN(service, ".", 3)
	if len(parts) >= 2 {
		service, namespace = parts[0], parts[1]
	}

	return
}

func parseResolverTarget(target resolver.Target) (targetInfo, error) {
	var service, port, namespace string
	if target.URL.Host == "" {
		// kubernetes:///service.namespace:port
		service, port, namespace = splitServicePortNamespace(target.Endpoint())
	} else if target.URL.Port() == "" && target.Endpoint() != "" {
		// kubernetes://namespace/service:port
		service, port, _ = splitServicePortNamespace(target.Endpoint())
		namespace = target.URL.Hostname()
	} else {
		// kubernetes://service.namespace:port
		service, port, namespace = splitServicePortNamespace(target.URL.Host)
	}

	if service == "" {
		return targetInfo{}, fmt.Errorf("target %s must specify a service", &target.URL)
	}

	resolveByPortName := false
	useFirstPort := false
	if port == "" {
		useFirstPort = true
	} else if _, err := strconv.Atoi(port); err != nil {
		resolveByPortName = true
	}

	return targetInfo{
		scheme:            target.URL.Scheme,
		serviceName:       service,
		serviceNamespace:  namespace,
		port:              port,
		resolveByPortName: resolveByPortName,
		useFirstPort:      useFirstPort,
	}, nil
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (b *kubeBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if b.k8sClient == nil {
		if cl, err := NewInClusterK8sClient(); err == nil {
			b.k8sClient = cl
		} else {
			return nil, err
		}
	}
	ti, err := parseResolverTarget(target)
	if err != nil {
		return nil, err
	}
	if ti.serviceNamespace == "" {
		ti.serviceNamespace = getCurrentNamespaceOrDefault()
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &kResolver{
		target:    ti,
		ctx:       ctx,
		cancel:    cancel,
		cc:        cc,
		k8sClient: b.k8sClient,
		t:         time.NewTimer(defaultFreq),
		freq:      defaultFreq,

		endpoints:      endpointsForTarget.WithLabelValues(ti.String()),
		addresses:      addressesForTarget.WithLabelValues(ti.String()),
		lastUpdateUnix: clientLastUpdate.WithLabelValues(ti.String()),
	}
	go until(func() {
		r.wg.Add(1)
		err := r.watch()
		if err != nil && err != io.EOF {
			grpclog.Errorf("kuberesolver: watching ended with error='%v', will reconnect again", err)
		}
	}, time.Second, time.Second*30, ctx.Done())
	return r, nil
}

// Scheme returns the scheme supported by this resolver.
// Scheme is defined at https://github.com/grpc/grpc/blob/master/doc/naming.md.
func (b *kubeBuilder) Scheme() string {
	return b.schema
}

type kResolver struct {
	target    targetInfo
	ctx       context.Context
	cancel    context.CancelFunc
	cc        resolver.ClientConn
	k8sClient K8sClient
	// wg is used to enforce Close() to return after the watcher() goroutine has finished.
	wg   sync.WaitGroup
	t    *time.Timer
	freq time.Duration

	endpoints prometheus.Gauge
	addresses prometheus.Gauge
	// lastUpdateUnix is the timestamp of the last successful update to the resolver client
	lastUpdateUnix prometheus.Gauge

	endpointSlices endpointSliceCache
}

// ResolveNow will be called by gRPC to try to resolve the target name again.
// It's just a hint, resolver can ignore this if it's not necessary.
func (k *kResolver) ResolveNow(resolver.ResolveNowOptions) {
	// ignore
}

// Close closes the resolver.
func (k *kResolver) Close() {
	k.cancel()
	k.wg.Wait()
}

func (k *kResolver) handle(e EndpointSlice, remove bool) {
	k.endpointSlices.update(e, remove)
	k.publish()
}

func (k *kResolver) publish() {
	addrs, endpointCount := k.endpointSlices.getAddresses(k.target)
	// An empty snapshot must remove the previous destinations from gRPC.
	k.cc.UpdateState(resolver.State{Addresses: addrs})
	k.lastUpdateUnix.Set(float64(time.Now().Unix()))
	k.endpoints.Set(float64(endpointCount))
	k.addresses.Set(float64(len(addrs)))
}

func (k *kResolver) resolve() (string, error) {
	list, err := getEndpointSliceList(k.ctx, k.k8sClient, k.target.serviceNamespace, k.target.serviceName)
	if err != nil {
		return "", err
	}
	// A complete list also removes slices that disappeared during a watch outage.
	k.endpointSlices.replace(list.Items)
	k.publish()
	k.t.Reset(k.freq)
	return list.Metadata.ResourceVersion, nil
}

func (k *kResolver) watch() error {
	defer k.wg.Done()
	version, err := k.resolve()
	if err != nil {
		return err
	}
	sw, err := watchEndpointSlice(k.ctx, k.k8sClient, k.target.serviceNamespace, k.target.serviceName, version)
	if err != nil {
		return err
	}
	defer sw.Stop()
	for {
		select {
		case <-k.ctx.Done():
			return nil
		case <-k.t.C:
			// Restart from a new snapshot so queued events cannot undo a newer list.
			return nil
		case up, hasMore := <-sw.ResultChan():
			if !hasMore {
				return nil
			}
			switch up.Type {
			case Added, Modified:
				k.handle(up.Object, false)
			case Deleted:
				k.handle(up.Object, true)
			case Error:
				// Watch errors require a new list before we can trust further updates.
				return fmt.Errorf("kuberesolver: received a watch error")
			}
		}
	}
}

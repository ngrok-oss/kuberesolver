/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kuberesolver

import (
	"net"
	"sort"
	"strconv"

	"google.golang.org/grpc/resolver"
)

// Adapted from Kubernetes' EndpointSliceCache:
// https://github.com/kubernetes/kubernetes/blob/fc112d4cef32938d9ee6070ce9c954856999e8f5/pkg/proxy/endpointslicecache.go
// Each resolver watches one service and applies updates immediately in one goroutine.
// It needs no service index, pending updates, or lock.
type endpointSliceCache struct {
	// Keep slices separate because an endpoint can occur in multiple slices during a move.
	slices map[string]EndpointSlice
}

func (cache *endpointSliceCache) update(slice EndpointSlice, remove bool) {
	if remove {
		delete(cache.slices, slice.Metadata.Name)
		return
	}
	if cache.slices == nil {
		cache.slices = make(map[string]EndpointSlice)
	}
	cache.slices[slice.Metadata.Name] = slice
}

func (cache *endpointSliceCache) replace(slices []EndpointSlice) {
	cache.slices = make(map[string]EndpointSlice, len(slices))
	for _, slice := range slices {
		cache.update(slice, false)
	}
}

type endpointInfo struct {
	ready       bool
	terminating bool
}

func (cache *endpointSliceCache) getAddresses(target targetInfo) ([]resolver.Address, int) {
	endpointSet := make(map[string]endpointInfo)
	endpointCount := 0
	for _, slice := range cache.slices {
		endpointCount += len(slice.Endpoints)
		port, ok := endpointSlicePort(slice, target)
		if ok {
			addEndpoints(endpointSet, slice.Endpoints, port)
		}
	}

	addresses := make([]resolver.Address, 0, len(endpointSet))
	for address, endpoint := range endpointSet {
		// Select the duplicate's conditions before filtering for readiness.
		if endpoint.ready {
			addresses = append(addresses, resolver.Address{
				Addr:       address,
				ServerName: target.serviceName + "." + target.serviceNamespace,
			})
		}
	}
	// Stable order prevents changes caused only by map iteration.
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].Addr < addresses[j].Addr })
	return addresses, endpointCount
}

func addEndpoints(endpointSet map[string]endpointInfo, endpoints []Endpoint, port string) {
	for _, endpoint := range endpoints {
		info := endpointInfo{
			ready:       endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready,
			terminating: endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating,
		}
		for _, address := range endpoint.Addresses {
			ip := net.ParseIP(address)
			if ip == nil {
				continue
			}
			key := net.JoinHostPort(ip.String(), port)
			// A new pod can reuse an IP before the old pod leaves the API.
			// Match kube-proxy: prefer the non-terminating endpoint when duplicates disagree.
			if _, exists := endpointSet[key]; !exists || !info.terminating {
				endpointSet[key] = info
			}
		}
	}
}

func endpointSlicePort(slice EndpointSlice, target targetInfo) (string, bool) {
	if !target.useFirstPort && !target.resolveByPortName {
		return target.port, target.port != ""
	}
	for _, port := range slice.Ports {
		if target.useFirstPort || port.Name == target.port {
			if port.Port <= 0 || port.Port > 65535 {
				return "", false
			}
			return strconv.Itoa(port.Port), true
		}
	}
	return "", false
}

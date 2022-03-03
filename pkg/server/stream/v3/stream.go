package stream

import (
	"google.golang.org/grpc"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Generic RPC stream.
type Stream interface {
	grpc.ServerStream

	Send(*discovery.DiscoveryResponse) error
	Recv() (*discovery.DiscoveryRequest, error)
}

type DeltaStream interface {
	grpc.ServerStream

	Send(*discovery.DeltaDiscoveryResponse) error
	Recv() (*discovery.DeltaDiscoveryRequest, error)
}

// StreamState will keep track of resource state per type on a stream.
type StreamState struct { // nolint:golint,revive
	// Indicates whether the original DeltaRequest was a wildcard LDS/RDS request.
	wildcard bool

	// ResourceVersions contains a hash of the resource as the value and the resource name as the key.
	// This field stores the last state sent to the client.
	resourceVersions map[string]string

	// knownResourceNames contains resource names that a client has received previously (SOTW)
	knownResourceNames map[string]map[string]struct{}

	// indicates whether the object has beed modified since its creation
	first bool

	// indicates whether we want an ordered ADS stream or not
	ordered bool
}

// NewStreamState initializes a stream state with empty defaults.
func NewStreamState(wildcard bool, initialResourceVersions map[string]string) StreamState {
	state := StreamState{
		wildcard:           wildcard,
		resourceVersions:   initialResourceVersions,
		first:              true,
		knownResourceNames: map[string]map[string]struct{}{},
		ordered:            false, // Ordered comes from the first request since that's when we discover if they want ADS
	}

	if initialResourceVersions == nil {
		state.resourceVersions = make(map[string]string)
	}

	return state
}

// GetResourceVersions returns a map of current resources grouped by type URL.
func (s *StreamState) GetResourceVersions() map[string]string {
	return s.resourceVersions
}

// SetResourceVersions sets a list of resource versions by type URL and removes the flag
// of "first" since we can safely assume another request has come through the stream.
func (s *StreamState) SetResourceVersions(resourceVersions map[string]string) {
	s.first = false
	s.resourceVersions = resourceVersions
}

// IsFirst returns whether or not the state of the stream is based upon the initial request.
func (s *StreamState) IsFirst() bool {
	return s.first
}

// IsWildcard returns whether or not an xDS client requested in wildcard mode on the initial request.
func (s *StreamState) IsWildcard() bool {
	return s.wildcard
}

// IsOrdered returns wherther or not the current stream should run as an ordered ADS stream.
// This means less backpressure relief but a guarantee of correct discovery response order.
func (s *StreamState) IsOrdered(ordered bool) bool {
	s.ordered = ordered
	return ordered
}

// SetKnownResourceNames sets a list of resource names in a stream utilizing the SOTW protocol.
func (s *StreamState) SetKnownResourceNames(url string, names map[string]struct{}) {
	s.knownResourceNames[url] = names
}

// SetKnownResourceNamesAsList is a helper function to set resource names as a slice input.
func (s *StreamState) SetKnownResourceNamesAsList(url string, names []string) {
	m := map[string]struct{}{}
	for _, name := range names {
		m[name] = struct{}{}
	}
	s.knownResourceNames[url] = m
}

// GetKnownResourceNames returns the current known list of resources on a SOTW stream.
func (s *StreamState) GetKnownResourceNames(url string) map[string]struct{} {
	return s.knownResourceNames[url]
}

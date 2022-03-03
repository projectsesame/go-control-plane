package sotw

import (
	"reflect"
	"sync/atomic"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// process handles a bi-di stream request
func (s *server) process(str stream.Stream, reqCh <-chan *discovery.DiscoveryRequest, defaultTypeURL string) error {
	sw := streamWrapper{
		stream:      str,
		ID:          atomic.AddInt64(&s.streamCount, 1), // increment stream count
		callbacks:   s.callbacks,
		streamState: stream.NewStreamState(false, map[string]string{}),
		watches:     newWatches(),
	}

	lastDiscoveryResponses := map[string]lastDiscoveryResponse{}
	// a collection of stack allocated watches per request type

	defer func() {
		sw.watches.close()
		if s.callbacks != nil {
			s.callbacks.OnStreamClosed(sw.ID)
		}
	}()

	if s.callbacks != nil {
		if err := s.callbacks.OnStreamOpen(str.Context(), sw.ID, defaultTypeURL); err != nil {
			return err
		}
	}

	// node may only be set on the first discovery request
	var node = &core.Node{}

	// recompute dynamic channels for this stream
	sw.watches.recompute(s.ctx, reqCh)

	for {
		// The list of select cases looks like this:
		// 0: <- ctx.Done
		// 1: <- reqCh
		// 2...: per type watches
		index, value, ok := reflect.Select(sw.watches.cases)
		switch index {
		// ctx.Done() -> if we receive a value here we return as no further computation is needed
		case 0:
			return nil
		// Case 1 handles any request inbound on the stream and handles all initialization as needed
		case 1:
			// input stream ended or errored out
			if !ok {
				return nil
			}

			req := value.Interface().(*discovery.DiscoveryRequest)
			if req == nil {
				return status.Errorf(codes.Unavailable, "empty request")
			}

			// Only first request is guaranteed to hold node info so if it's missing, reassign.
			if req.Node != nil {
				node = req.Node
			} else {
				req.Node = node
			}

			// nonces can be reused across streams; we verify nonce only if nonce is not initialized
			nonce := req.GetResponseNonce()

			// type URL is required for ADS but is implicit for xDS
			if defaultTypeURL == resource.AnyType {
				if req.TypeUrl == "" {
					return status.Errorf(codes.InvalidArgument, "type URL is required for ADS")
				}

				// When using ADS we need to order responses. This is guaranteed in the xDS protocol specification
				// as ADS is required to be eventually consistent. More details can be found here if interested:
				// https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#eventual-consistency-considerations
				sw.streamState.IsOrdered(true)

				// Trigger a different code path specifically for ADS. We want resource ordering
				// so things don't get sent before they should. This is a blocking call and
				// will exit the process function on successful completion.
				return s.processADS(&sw, reqCh, defaultTypeURL)
			} else if req.TypeUrl == "" {
				req.TypeUrl = defaultTypeURL
			}

			if s.callbacks != nil {
				if err := s.callbacks.OnStreamRequest(sw.ID, req); err != nil {
					return err
				}
			}

			if lastResponse, ok := lastDiscoveryResponses[req.TypeUrl]; ok {
				if lastResponse.nonce == "" || lastResponse.nonce == nonce {
					// Let's record Resource names that a client has received.
					sw.streamState.SetKnownResourceNames(req.TypeUrl, lastResponse.resources)
				}
			}

			typeURL := req.GetTypeUrl()
			responder := make(chan cache.Response, 1)
			if w, ok := sw.watches.responders[typeURL]; ok {
				// We've found a pre-existing watch, lets check and update if needed.
				// If these requirements aren't satisfied, leave an open watch.
				if w.nonce == "" || w.nonce == nonce {
					w.close()

					sw.watches.addWatch(typeURL, &watch{
						cancel:   s.cache.CreateWatch(req, sw.streamState, responder),
						response: responder,
					})
				}
			} else {
				// No pre-existing watch exists, let's create one.
				// We need to precompute the watches first then open a watch in the cache.
				sw.watches.addWatch(typeURL, &watch{
					cancel:   s.cache.CreateWatch(req, sw.streamState, responder),
					response: responder,
				})
			}

			// Recompute the dynamic select cases for this stream.
			sw.watches.recompute(s.ctx, reqCh)
		default:
			// Channel n -> these are the dynamic list of responders that correspond to the stream request typeURL
			if !ok {
				// Receiver channel was closed. TODO(jpeach): probably cancel the watch or something?
				return status.Errorf(codes.Unavailable, "resource watch %d -> failed", index)
			}

			res := value.Interface().(cache.Response)
			nonce, err := sw.send(res)
			if err != nil {
				return err
			}

			sw.watches.responders[res.GetRequest().TypeUrl].nonce = nonce
		}
	}
}

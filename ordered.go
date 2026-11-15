// Copyright 2018 Adam Tauber
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colly

import (
	"io"
	"net/http"
	"net/url"
	"sync"
)

// defaultOrderedLookahead is the number of requests which are allowed to be
// started but not yet published when Collector.Lookahead is not positive.
const defaultOrderedLookahead = 8

// orderedTask is a single request of an ordered crawl round.
type orderedTask struct {
	// seq is the admission sequence number of the request.
	seq uint64
	// start performs the download phase and returns its outcome.
	start func() *fetchOutcome
	// done is true once outcome has been written. It is guarded by orderState.mu.
	done bool
	// outcome holds the result of the download phase. It is written under
	// orderState.mu so that the publication sees a consistent Request.
	outcome *fetchOutcome
	// next, when not nil, admits further requests right after the publication
	// of this one, on the coordinator goroutine.
	next func(err error)
}

// orderState serializes the admission and the publication of ordered requests.
//
// Sequence numbers are handed out in admission order, and the requests are
// published strictly in sequence order. Because the links discovered by a
// response callback are admitted while its publication is running, the
// admission order is exactly the order of a serial breadth-first traversal.
type orderState struct {
	mu   sync.Mutex
	cond *sync.Cond

	// pending contains the admitted but not yet published requests. The keys
	// are a contiguous sequence range: [headSeq, nextSeq).
	pending map[uint64]*orderedTask
	// nextSeq is the sequence number of the next admitted request.
	nextSeq uint64
	// headSeq is the sequence number of the next request to publish.
	headSeq uint64
	// nextStart is the sequence number of the next request whose download may
	// be started. [headSeq, nextStart) is the set of started but unpublished
	// requests, which is bounded by the lookahead.
	nextStart uint64
	// claims holds the URLs which were already admitted in the current round.
	claims map[uint64]bool
	// done is non nil while a coordinator goroutine is alive. It is closed
	// when that goroutine exits, which marks the end of a crawl round.
	done chan struct{}
}

func newOrderState() *orderState {
	o := &orderState{
		pending: make(map[uint64]*orderedTask),
		claims:  make(map[uint64]bool),
	}
	o.cond = sync.NewCond(&o.mu)
	return o
}

// orderedActive reports whether the ordered scheduler has to be used.
func (c *Collector) orderedActive() bool {
	return c.OrderedAsync
}

// effectiveLookahead returns the lookahead limit, falling back to the default
// for non positive values so that a zero value cannot stall the crawl.
func (c *Collector) effectiveLookahead() int {
	if c.Lookahead > 0 {
		return c.Lookahead
	}
	return defaultOrderedLookahead
}

// admitOrdered admits a request to the ordered scheduler and reports the
// result of its publication to next, which runs on the coordinator goroutine
// right after the publication. The returned error only reports a synchronous
// admission failure; the outcome of the request itself is reported through
// the callbacks.
func (c *Collector) admitOrdered(parsedURL *url.URL, method string, depth int, requestData io.Reader, ctx *Context, req *http.Request, checkRevisit bool, next func(error)) error {
	o := c.order
	o.mu.Lock()
	spawn := o.done == nil
	if spawn {
		// No coordinator is running, so the previous round is finished and
		// the sequences and the per round claims start over.
		o.nextSeq, o.headSeq, o.nextStart = 0, 0, 0
		o.pending = make(map[uint64]*orderedTask)
		o.claims = make(map[uint64]bool)
		o.done = make(chan struct{})
	}
	if err := c.claimOrdered(parsedURL, method, req.GetBody, checkRevisit); err != nil {
		if spawn {
			// No request was admitted, so there is nothing for the coordinator
			// to do. Leave the state as an idle round.
			o.done = nil
		}
		o.mu.Unlock()
		return err
	}
	seq := o.nextSeq
	o.nextSeq++
	id := c.requestCount.Add(1)
	task := &orderedTask{
		seq: seq,
		start: func() *fetchOutcome {
			return c.dispatch(method, depth, requestData, ctx, req, id)
		},
		next: next,
	}
	o.pending[seq] = task
	c.wg.Add(1)
	o.cond.Signal()
	done := o.done
	o.mu.Unlock()
	if spawn {
		go c.runOrdered(done)
	}
	return nil
}

// claimOrdered records the URL of a request in the current round. It must be
// called with orderState.mu held.
func (c *Collector) claimOrdered(parsedURL *url.URL, method string, getBody func() (io.ReadCloser, error), checkRevisit bool) error {
	if !checkRevisit {
		return nil
	}
	// Keep the behaviour of requestCheck: a non GET request without a body
	// (for example the HEAD probe of CheckHead) does not consume the URL.
	if method != "GET" && getBody == nil {
		return nil
	}
	var body io.ReadCloser
	if getBody != nil {
		var err error
		body, err = getBody()
		if err != nil {
			return err
		}
		defer body.Close()
	}
	uHash := requestHash(parsedURL.String(), body)
	if c.AllowURLRevisit {
		// The regular revisit check keeps the permanent store untouched when
		// revisits are allowed, so the ordered mode deduplicates per round and
		// the claims are reset when the next round starts.
		if c.order.claims[uHash] {
			return &AlreadyVisitedError{parsedURL}
		}
		c.order.claims[uHash] = true
		return nil
	}
	visited, err := c.store.IsVisited(uHash)
	if err != nil {
		return err
	}
	if visited {
		return &AlreadyVisitedError{parsedURL}
	}
	return c.store.Visited(uHash)
}

// runOrdered is the coordinator of an ordered crawl round. It starts the
// downloads in sequence order, bounded by the lookahead, and publishes the
// finished requests in sequence order. It exits once the round is drained.
func (c *Collector) runOrdered(done chan struct{}) {
	o := c.order
	o.mu.Lock()
	for {
		if task, ok := o.pending[o.headSeq]; ok && task.done {
			seq := o.headSeq
			outcome := task.outcome
			next := task.next
			// The response callbacks may admit new requests, which needs the
			// lock, so publishing has to run without it.
			o.mu.Unlock()
			err := c.publish(outcome)
			if next != nil {
				next(err)
			}
			o.mu.Lock()
			delete(o.pending, seq)
			o.headSeq++
			c.wg.Done()
			continue
		}
		if o.nextStart < o.nextSeq && o.nextStart-o.headSeq < uint64(c.effectiveLookahead()) {
			seq := o.nextStart
			o.nextStart++
			o.mu.Unlock()
			go c.downloadOrdered(seq)
			o.mu.Lock()
			continue
		}
		if o.nextSeq == o.headSeq {
			o.done = nil
			o.mu.Unlock()
			close(done)
			return
		}
		o.cond.Wait()
	}
}

// downloadOrdered performs the download phase of an admitted request and
// hands the outcome over to the coordinator.
func (c *Collector) downloadOrdered(seq uint64) {
	o := c.order
	o.mu.Lock()
	task := o.pending[seq]
	o.mu.Unlock()
	outcome := task.start()
	o.mu.Lock()
	task.outcome = outcome
	task.done = true
	o.cond.Signal()
	o.mu.Unlock()
}

// waitOrdered waits until the running coordinator, if any, has exited. It is
// used by Collector.Wait to make sure the ordered state is quiescent before
// it returns.
func (c *Collector) waitOrdered() {
	o := c.order
	o.mu.Lock()
	done := o.done
	o.mu.Unlock()
	if done != nil {
		<-done
	}
}

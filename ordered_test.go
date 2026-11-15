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
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// orderedTestServer serves a small link graph. /a is slow and links to /a1 and
// /shared, /b is fast and links to /b1 and /shared, and the rest are leaves.
type orderedTestServer struct {
	*httptest.Server
	hits       sync.Map // path -> *int32
	inFlight   int32
	maxInFligh int32
}

func newOrderedTestServer(t *testing.T) *orderedTestServer {
	t.Helper()
	s := &orderedTestServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&s.inFlight, 1)
		for {
			max := atomic.LoadInt32(&s.maxInFligh)
			if cur <= max || atomic.CompareAndSwapInt32(&s.maxInFligh, max, cur) {
				break
			}
		}
		defer atomic.AddInt32(&s.inFlight, -1)

		v, _ := s.hits.LoadOrStore(r.URL.Path, new(int32))
		atomic.AddInt32(v.(*int32), 1)

		w.Header().Set("Content-Type", "text/html")
		switch r.URL.Path {
		case "/a":
			time.Sleep(300 * time.Millisecond)
			w.Write([]byte(`<a href="/a1">a1</a><a href="/shared">shared</a>`))
		case "/b":
			w.Write([]byte(`<a href="/b1">b1</a><a href="/shared">shared</a>`))
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`error`))
		case "/slowlink":
			time.Sleep(50 * time.Millisecond)
			w.Write([]byte(`<a href="/leaf">leaf</a>`))
		case "/d1", "/d2":
			time.Sleep(200 * time.Millisecond)
			w.Write([]byte(`ok`))
		default:
			w.Write([]byte(`ok`))
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *orderedTestServer) hitCount(path string) int32 {
	v, ok := s.hits.Load(path)
	if !ok {
		return 0
	}
	return atomic.LoadInt32(v.(*int32))
}

// publishRecorder records the published request paths in order.
type publishRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *publishRecorder) record(path string) {
	r.mu.Lock()
	r.order = append(r.order, path)
	r.mu.Unlock()
}

func (r *publishRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

func equalPaths(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestOrderedAsyncMatchesSerialBFS(t *testing.T) {
	for _, lookahead := range []int{1, 2, 8} {
		ts := newOrderedTestServer(t)
		rec := &publishRecorder{}
		c := NewCollector(OrderedAsync(lookahead))
		c.OnResponse(func(r *Response) {
			rec.record(r.Request.URL.Path)
		})
		c.OnHTML("a[href]", func(e *HTMLElement) {
			e.Request.Visit(e.Attr("href"))
		})

		if err := c.Visit(ts.URL + "/a"); err != nil {
			t.Fatalf("lookahead %d: Visit(/a): %v", lookahead, err)
		}
		if err := c.Visit(ts.URL + "/b"); err != nil {
			t.Fatalf("lookahead %d: Visit(/b): %v", lookahead, err)
		}
		c.Wait()

		want := []string{"/a", "/b", "/a1", "/shared", "/b1"}
		if got := rec.snapshot(); !equalPaths(got, want) {
			t.Errorf("lookahead %d: published %v, want %v", lookahead, got, want)
		}
		if n := ts.hitCount("/shared"); n != 1 {
			t.Errorf("lookahead %d: /shared fetched %d times, want 1", lookahead, n)
		}
	}
}

func TestOrderedAsyncSlowPageIsNotOvertaken(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(8))
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.URL.Path)
	})

	start := time.Now()
	if err := c.Visit(ts.URL + "/a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Visit(ts.URL + "/b"); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	elapsed := time.Since(start)

	got := rec.snapshot()
	if len(got) < 2 || got[0] != "/a" || got[1] != "/b" {
		t.Fatalf("published %v, want /a before /b", got)
	}
	if elapsed < 300*time.Millisecond {
		t.Errorf("elapsed %v, want at least the slow page's delay", elapsed)
	}
}

func TestOrderedAsyncDownloadsStayConcurrent(t *testing.T) {
	ts := newOrderedTestServer(t)
	c := NewCollector(OrderedAsync(8))
	var responses int32
	c.OnResponse(func(r *Response) {
		atomic.AddInt32(&responses, 1)
	})

	start := time.Now()
	for _, path := range []string{"/d1", "/d2", "/d1?x", "/d2?x"} {
		if err := c.Visit(ts.URL + path); err != nil {
			t.Fatal(err)
		}
	}
	c.Wait()
	elapsed := time.Since(start)

	if got := atomic.LoadInt32(&responses); got != 4 {
		t.Fatalf("%d responses, want 4", got)
	}
	// Four 200ms downloads take 800ms serially; concurrent downloads take
	// about 200-400ms.
	if elapsed > 700*time.Millisecond {
		t.Errorf("elapsed %v for four 200ms pages, downloads are not concurrent", elapsed)
	}
	if max := atomic.LoadInt32(&ts.maxInFligh); max < 2 {
		t.Errorf("max concurrent requests %d, want concurrent downloads", max)
	}
}

func TestOrderedAsyncLookaheadBound(t *testing.T) {
	ts := newOrderedTestServer(t)
	const lookahead = 4
	var maxStarted int
	c := NewCollector(OrderedAsync(lookahead))
	c.OnResponse(func(r *Response) {
		c.order.mu.Lock()
		started := int(c.order.nextStart - c.order.headSeq)
		c.order.mu.Unlock()
		if started > maxStarted {
			maxStarted = started
		}
	})

	for _, path := range []string{"/a1", "/b1", "/leaf", "/slowlink", "/a"} {
		if err := c.Visit(ts.URL + path); err != nil {
			t.Fatal(err)
		}
	}
	c.Wait()

	if maxStarted > lookahead {
		t.Errorf("started-but-unpublished = %d, want <= %d", maxStarted, lookahead)
	}
	if max := atomic.LoadInt32(&ts.maxInFligh); max > lookahead {
		t.Errorf("max concurrent requests %d, want <= %d", max, lookahead)
	}
}

func TestOrderedAsyncFailureAdvancesOrder(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(2))
	c.OnResponse(func(r *Response) {
		rec.record("response:" + r.Request.URL.Path)
	})
	c.OnError(func(r *Response, err error) {
		rec.record("error:" + r.Request.URL.Path)
	})

	if err := c.Visit(ts.URL + "/500"); err != nil {
		t.Fatal(err)
	}
	if err := c.Visit(ts.URL + "/leaf"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	want := []string{"error:/500", "response:/leaf"}
	if got := rec.snapshot(); !equalPaths(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
}

func TestOrderedAsyncHeaderAbortAdvancesOrder(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(2))
	c.OnResponseHeaders(func(r *Response) {
		if r.Request.URL.Path == "/a" {
			r.Request.Abort()
		}
	})
	c.OnResponse(func(r *Response) {
		rec.record("response:" + r.Request.URL.Path)
	})
	c.OnError(func(r *Response, err error) {
		if err == ErrAbortedAfterHeaders {
			rec.record("aborted:" + r.Request.URL.Path)
			return
		}
		rec.record("error:" + r.Request.URL.Path)
	})

	if err := c.Visit(ts.URL + "/a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Visit(ts.URL + "/leaf"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	want := []string{"aborted:/a", "response:/leaf"}
	if got := rec.snapshot(); !equalPaths(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
}

func TestOrderedAsyncWaitCoversDerivedRequests(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(2))
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.URL.Path)
	})
	c.OnHTML("a[href]", func(e *HTMLElement) {
		e.Request.Visit(e.Attr("href"))
	})

	if err := c.Visit(ts.URL + "/slowlink"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	want := []string{"/slowlink", "/leaf"}
	if got := rec.snapshot(); !equalPaths(got, want) {
		t.Errorf("published %v after Wait, want %v", got, want)
	}
}

func TestOrderedAsyncRoundReset(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}

	c := NewCollector(OrderedAsync(2), AllowURLRevisit())
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.URL.Path)
	})

	// /a takes long enough that the round is still active when the duplicate
	// admission is attempted.
	if err := c.Visit(ts.URL + "/a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Visit(ts.URL + "/a"); err == nil {
		t.Error("second Visit in the same round should be rejected as already visited")
	}
	c.Wait()
	if got := rec.snapshot(); !equalPaths(got, []string{"/a"}) {
		t.Errorf("first round published %v, want [/a]", got)
	}

	// A new round starts after Wait, so the same URL is collected again.
	if err := c.Visit(ts.URL + "/a"); err != nil {
		t.Fatalf("Visit after Wait: %v", err)
	}
	c.Wait()
	if got := rec.snapshot(); !equalPaths(got, []string{"/a", "/a"}) {
		t.Errorf("second round published %v, want [/a /a]", got)
	}
}

func TestOrderedAsyncRetry(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(2))
	retried := false
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.URL.Path)
		if !retried {
			retried = true
			if err := r.Request.Retry(); err != nil {
				t.Errorf("Retry: %v", err)
			}
		}
	})

	if err := c.Visit(ts.URL + "/leaf"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	if got := rec.snapshot(); !equalPaths(got, []string{"/leaf", "/leaf"}) {
		t.Errorf("published %v, want two entries for the retried request", got)
	}
}

func TestOrderedAsyncClone(t *testing.T) {
	c := NewCollector(OrderedAsync(3))
	clone := c.Clone()
	if !clone.OrderedAsync || !clone.Async {
		t.Fatalf("clone async=%v ordered=%v, want both true", clone.Async, clone.OrderedAsync)
	}
	if clone.Lookahead != 3 {
		t.Errorf("clone lookahead = %d, want 3", clone.Lookahead)
	}
	if clone.order == c.order {
		t.Error("clone shares the ordered state with the original")
	}
}

func TestOrderedAsyncOption(t *testing.T) {
	for _, tc := range []struct {
		opts      []CollectorOption
		lookahead int
	}{
		{[]CollectorOption{OrderedAsync()}, defaultOrderedLookahead},
		{[]CollectorOption{OrderedAsync(2)}, 2},
		{[]CollectorOption{OrderedAsync(0)}, defaultOrderedLookahead},
	} {
		c := NewCollector(tc.opts...)
		if !c.OrderedAsync || !c.Async {
			t.Errorf("async=%v ordered=%v, want both true", c.Async, c.OrderedAsync)
		}
		if c.Lookahead != tc.lookahead {
			t.Errorf("lookahead = %d, want %d", c.Lookahead, tc.lookahead)
		}
	}
}

func TestOrderedAsyncZeroLookaheadDoesNotStall(t *testing.T) {
	ts := newOrderedTestServer(t)
	c := NewCollector(OrderedAsync())
	c.Lookahead = 0
	var responses int32
	c.OnResponse(func(r *Response) {
		atomic.AddInt32(&responses, 1)
	})
	if err := c.Visit(ts.URL + "/leaf"); err != nil {
		t.Fatal(err)
	}
	if err := c.Visit(ts.URL + "/b1"); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	if got := atomic.LoadInt32(&responses); got != 2 {
		t.Errorf("%d responses, want 2", got)
	}
}

func TestOrderedAsyncCheckHead(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(2), CheckHead())
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.Method + ":" + r.Request.URL.Path)
	})

	if err := c.Visit(ts.URL + "/leaf"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	want := []string{"HEAD:/leaf", "GET:/leaf"}
	if got := rec.snapshot(); !equalPaths(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
}

func TestOrderedAsyncMaxDepth(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(3), MaxDepth(1))
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.URL.Path)
	})
	c.OnHTML("a[href]", func(e *HTMLElement) {
		e.Request.Visit(e.Attr("href"))
	})

	if err := c.Visit(ts.URL + "/a"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	want := []string{"/a"}
	if got := rec.snapshot(); !equalPaths(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
}

func TestOrderedAsyncOnRequestAbortLetLaterPublish(t *testing.T) {
	ts := newOrderedTestServer(t)
	rec := &publishRecorder{}
	c := NewCollector(OrderedAsync(2))
	c.OnRequest(func(r *Request) {
		if r.URL.Path == "/d1" {
			r.Abort()
		}
	})
	c.OnResponse(func(r *Response) {
		rec.record(r.Request.URL.Path)
	})
	c.OnError(func(r *Response, err error) {
		rec.record("error:" + r.Request.URL.Path)
	})

	if err := c.Visit(ts.URL + "/d1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Visit(ts.URL + "/d2"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	want := []string{"/d2"}
	if got := rec.snapshot(); !equalPaths(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
}

func TestOrderedAsyncLookaheadOneIsSerialOracle(t *testing.T) {
	ts := newOrderedTestServer(t)

	run := func(lookahead int) []string {
		rec := &publishRecorder{}
		c := NewCollector(OrderedAsync(lookahead), AllowURLRevisit())
		c.OnResponse(func(r *Response) {
			rec.record(r.Request.URL.Path)
		})
		c.OnHTML("a[href]", func(e *HTMLElement) {
			e.Request.Visit(e.Attr("href"))
		})
		for _, path := range []string{"/a", "/b", "/slowlink"} {
			if err := c.Visit(ts.URL + path); err != nil {
				t.Fatal(err)
			}
		}
		c.Wait()
		return rec.snapshot()
	}

	serial := run(1)
	for _, lookahead := range []int{2, 4, 8, 16} {
		if got := run(lookahead); !equalPaths(got, serial) {
			t.Errorf("lookahead %d published %v, serial order is %v", lookahead, got, serial)
		}
	}
	if len(serial) == 0 {
		t.Fatal("no pages published")
	}
}

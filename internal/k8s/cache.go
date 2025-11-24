package k8s

import (
	"sort"
	"sync"
)

type genericResourceCache struct {
	sync.Mutex
	values []*GenericResource
}

// GenericResourceCache - storage for generic resources with a rendezvous point for goroutines
// waiting for or announcing the occurence of a cache events.
type GenericResourceCache struct {
	genericResourceCache
	Cond
}

// Values returns a copy of the contents of the cache.
func (cc *genericResourceCache) Values() []*GenericResource {
	cc.Lock()
	r := []*GenericResource{}
	for _, v := range cc.values {
		r = append(r, v.DeepCopy())
	}
	cc.Unlock()
	return r
}

// Add adds an entry to the cache. If a GenericResource with the same
// name exists, it is replaced.
func (cc *genericResourceCache) Add(grs ...*GenericResource) {
	if len(grs) == 0 {
		return
	}
	cc.Lock()
	defer cc.Unlock()
	
	// Only sort if cache is not empty and we're adding multiple items
	needsSort := len(cc.values) > 0 && len(grs) > 1
	if needsSort {
		sort.Sort(genericResource(cc.values))
	}
	
	for _, gr := range grs {
		cc.addUnsafe(gr)
	}
	
	// Sort once at the end if we added multiple items
	if len(grs) > 1 {
		sort.Sort(genericResource(cc.values))
	}
}

// addUnsafe adds c to the cache without locking. Caller must hold the lock.
// If c is already present, the cached value of c is overwritten.
func (cc *genericResourceCache) addUnsafe(c *GenericResource) {
	if len(cc.values) == 0 {
		// Empty cache, just append
		cc.values = append(cc.values, c)
		return
	}
	
	// Use binary search if cache is sorted, otherwise linear search
	i := sort.Search(len(cc.values), func(i int) bool { return cc.values[i].Identifier >= c.Identifier })
	if i < len(cc.values) && cc.values[i].Identifier == c.Identifier {
		// c is already present, replace
		cc.values[i] = c
	} else {
		// c is not present, append (will be sorted later if needed)
		cc.values = append(cc.values, c)
	}
}

// Remove removes the named entry from the cache. If the entry
// is not present in the cache, the operation is a no-op.
func (cc *genericResourceCache) Remove(identifiers ...string) {
	if len(identifiers) == 0 {
		return
	}
	cc.Lock()
	defer cc.Unlock()
	
	// Only sort if cache is not empty and we're removing multiple items
	if len(cc.values) > 0 && len(identifiers) > 1 {
		sort.Sort(genericResource(cc.values))
	}
	
	for _, n := range identifiers {
		cc.removeUnsafe(n)
	}
}

// removeUnsafe removes the named entry from the cache without locking. Caller must hold the lock.
func (cc *genericResourceCache) removeUnsafe(identifier string) {
	if len(cc.values) == 0 {
		return
	}
	
	// Use binary search if cache might be sorted, otherwise linear search
	i := sort.Search(len(cc.values), func(i int) bool { return cc.values[i].Identifier >= identifier })
	if i < len(cc.values) && cc.values[i].Identifier == identifier {
		// Found, remove
		cc.values = append(cc.values[:i], cc.values[i+1:]...)
	} else {
		// Not found with binary search, try linear search (cache might not be sorted)
		for j, v := range cc.values {
			if v.Identifier == identifier {
				cc.values = append(cc.values[:j], cc.values[j+1:]...)
				return
			}
		}
	}
}

// Cond implements a condition variable, a rendezvous point for goroutines
// waiting for or announcing the occurence of an event.
type Cond struct {
	mu      sync.Mutex
	waiters []chan int
	last    int
}

// Register registers ch to receive a value when Notify is called.
func (c *Cond) Register(ch chan int, last int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if last < c.last {
		// notify this channel immediately
		ch <- c.last
		return
	}
	c.waiters = append(c.waiters, ch)
}

// Notify notifies all registered waiters that an event has occured.
func (c *Cond) Notify() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last++

	for _, ch := range c.waiters {
		ch <- c.last
	}
	c.waiters = c.waiters[:0]
}

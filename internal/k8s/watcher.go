package k8s

import (
	"os"
	"sync"
	"time"

	"github.com/keel-hq/keel/constants"
	"github.com/keel-hq/keel/internal/workgroup"
	"github.com/sirupsen/logrus"

	apps_v1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// WatchDeployments creates a SharedInformer for apps/v1.Deployments and registers it with g.
func WatchDeployments(g *workgroup.Group, client *kubernetes.Clientset, log logrus.FieldLogger, rs ...cache.ResourceEventHandler) {
	watch(g, client.AppsV1().RESTClient(), log, "deployments", new(apps_v1.Deployment), rs...)
}

// WatchStatefulSets creates a SharedInformer for apps/v1.StatefulSet and registers it with g.
func WatchStatefulSets(g *workgroup.Group, client *kubernetes.Clientset, log logrus.FieldLogger, rs ...cache.ResourceEventHandler) {
	watch(g, client.AppsV1().RESTClient(), log, "statefulsets", new(apps_v1.StatefulSet), rs...)
}

// WatchDaemonSets creates a SharedInformer for apps/v1.DaemonSet and registers it with g.
func WatchDaemonSets(g *workgroup.Group, client *kubernetes.Clientset, log logrus.FieldLogger, rs ...cache.ResourceEventHandler) {
	watch(g, client.AppsV1().RESTClient(), log, "daemonsets", new(apps_v1.DaemonSet), rs...)
}

// WatchCronJobs creates a SharedInformer for batch_v1.CronJob and registers it with g.
func WatchCronJobs(g *workgroup.Group, client *kubernetes.Clientset, log logrus.FieldLogger, rs ...cache.ResourceEventHandler) {
	watch(g, client.BatchV1().RESTClient(), log, "cronjobs", new(batch_v1.CronJob), rs...)
}

func watch(g *workgroup.Group, c cache.Getter, log logrus.FieldLogger, resource string, objType runtime.Object, rs ...cache.ResourceEventHandler) {
	//Check if the env var RESTRICTED_NAMESPACE is empty or equal to keel
	// If equal to keel or empty, the scan will be over all the cluster
	// If RESTRICTED_NAMESPACE is different than keel or empty, keel will scan in the defined namespace
	namespaceScan := "keel"
	if os.Getenv(constants.EnvRestrictedNamespace) == "keel" {
		namespaceScan = v1.NamespaceAll
	} else if os.Getenv(constants.EnvRestrictedNamespace) == "" {
		namespaceScan = v1.NamespaceAll
	} else {
		namespaceScan = os.Getenv(constants.EnvRestrictedNamespace)
	}

	lw := cache.NewListWatchFromClient(c, resource, namespaceScan, fields.Everything())
	sw := cache.NewSharedInformer(lw, objType, 30*time.Minute)
	for _, r := range rs {
		sw.AddEventHandler(r)
	}
	g.Add(func(stop <-chan struct{}) {
		log := log.WithField("resource", resource)
		log.Println("started")
		defer log.Println("stopped")
		sw.Run(stop)
	})
}

type buffer struct {
	ev chan interface{}
	logrus.StdLogger
	rh cache.ResourceEventHandler

	// Backpressure tracking
	lastLogTime   time.Time
	blockedCount  int64
	blockedMutex  sync.Mutex
	logInterval   time.Duration
	
	// Worker pool
	workerCount int
}

type addEvent struct {
	obj             interface{}
	isInInitialList bool
}

type updateEvent struct {
	oldObj, newObj interface{}
}

type deleteEvent struct {
	obj interface{}
}

// NewBuffer returns a ResourceEventHandler which buffers and serialises ResourceEventHandler events.
func NewBuffer(g *workgroup.Group, rh cache.ResourceEventHandler, log logrus.FieldLogger, size int) cache.ResourceEventHandler {
	return NewBufferWithWorkers(g, rh, log, size, 0)
}

// NewBufferWithWorkers returns a ResourceEventHandler with configurable worker count.
// If workers is 0 or negative, it defaults to runtime.NumCPU().
func NewBufferWithWorkers(g *workgroup.Group, rh cache.ResourceEventHandler, log logrus.FieldLogger, size int, workers int) cache.ResourceEventHandler {
	if workers <= 0 {
		workers = 4 // Default to 4 workers for better throughput
	}
	buf := &buffer{
		ev:          make(chan interface{}, size),
		StdLogger:   log.WithField("context", "buffer"),
		rh:          rh,
		lastLogTime: time.Now(),
		logInterval: 5 * time.Second, // Log at most once every 5 seconds
		workerCount: workers,
	}
	// Start worker pool
	for i := 0; i < workers; i++ {
		g.Add(buf.loop)
	}
	return buf
}

func (b *buffer) loop(stop <-chan struct{}) {
	// Use a ticker for batching events
	ticker := time.NewTicker(10 * time.Millisecond) // Batch events every 10ms
	defer ticker.Stop()
	
	var batch []interface{}
	
	for {
		select {
		case ev := <-b.ev:
			batch = append(batch, ev)
			// Process batch if it gets too large (100 events)
			if len(batch) >= 100 {
				b.processBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			// Process accumulated batch
			if len(batch) > 0 {
				b.processBatch(batch)
				batch = batch[:0]
			}
		case <-stop:
			// Process remaining events before stopping
			if len(batch) > 0 {
				b.processBatch(batch)
			}
			return
		}
	}
}

func (b *buffer) processBatch(batch []interface{}) {
	for _, ev := range batch {
		switch ev := ev.(type) {
		case *addEvent:
			b.rh.OnAdd(ev.obj, ev.isInInitialList)
		case *updateEvent:
			b.rh.OnUpdate(ev.oldObj, ev.newObj)
		case *deleteEvent:
			b.rh.OnDelete(ev.obj)
		default:
			b.Printf("unhandled event type: %T: %v", ev, ev)
		}
	}
}

func (b *buffer) OnAdd(obj interface{}, isInInitialList bool) {
	b.send(&addEvent{obj, isInInitialList})
}

func (b *buffer) OnUpdate(oldObj, newObj interface{}) {
	b.send(&updateEvent{oldObj, newObj})
}

func (b *buffer) OnDelete(obj interface{}) {
	b.send(&deleteEvent{obj})
}

func (b *buffer) send(ev interface{}) {
	select {
	case b.ev <- ev:
		// all good
	default:
		// Channel is full, track blocked events and log periodically
		b.blockedMutex.Lock()
		b.blockedCount++
		shouldLog := time.Since(b.lastLogTime) >= b.logInterval
		if shouldLog {
			b.Printf("event channel is full, len: %v, cap: %v, blocked events since last log: %d", len(b.ev), cap(b.ev), b.blockedCount)
			b.lastLogTime = time.Now()
			b.blockedCount = 0
		}
		b.blockedMutex.Unlock()
		
		// Block until we can send (we don't want to lose events)
		b.ev <- ev
	}
}

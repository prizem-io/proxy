package director

import (
	"container/heap"
	"sync"
	"time"

	"github.com/prizem-io/proxy/pkg/log"
	"github.com/satori/go.uuid"
)

type OutlierMonitor struct {
	logger               log.Logger
	maxConsecutiveErrors int
	returnInterval       time.Duration

	instances   map[uuid.UUID]instance
	ejectedMu   sync.RWMutex
	ejected     map[uuid.UUID]struct{}
	returnQueue PriorityQueue

	Success chan *ProxyInfo
	Failure chan *ProxyInfo
	done    chan struct{}
}

type instance struct {
	consecutiveErrors int
}

func NewOutlierMonitor(logger log.Logger, size, maxConsecutiveErrors int, returnInterval time.Duration) *OutlierMonitor {
	return &OutlierMonitor{
		logger:               logger,
		maxConsecutiveErrors: maxConsecutiveErrors,
		returnInterval:       returnInterval,
		returnQueue:          make(PriorityQueue, 0, 100),
		instances:            make(map[uuid.UUID]instance, size),
		ejected:              make(map[uuid.UUID]struct{}, size/3),
		Success:              make(chan *ProxyInfo, 1000),
		Failure:              make(chan *ProxyInfo, 100),
		done:                 make(chan struct{}),
	}
}

func (m *OutlierMonitor) Process() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case proxyInfo := <-m.Success:
			delete(m.instances, proxyInfo.Destination.ID)
		case proxyInfo := <-m.Failure:
			m.logger.Infof("Service instance %q failed", proxyInfo.Destination.ID)
			o, _ := m.instances[proxyInfo.Destination.ID]
			o.consecutiveErrors++

			// Eject the service instance
			if o.consecutiveErrors == m.maxConsecutiveErrors {
				m.ejectedMu.Lock()
				m.ejected[proxyInfo.Destination.ID] = struct{}{}
				m.ejectedMu.Unlock()
				heap.Push(&m.returnQueue, &EjectedInstance{
					InstanceID: proxyInfo.Destination.ID,
					Expiry:     time.Now().Add(m.returnInterval),
				})
				delete(m.instances, proxyInfo.Destination.ID)
				m.logger.Infof("Service instance %q ejected", proxyInfo.Destination.ID)
			} else {
				m.instances[proxyInfo.Destination.ID] = o
			}
		case now := <-ticker.C:
			for len(m.returnQueue) > 0 {
				next := m.returnQueue.Peek().(*EjectedInstance)
				if next.Expiry.After(now) {
					break
				}

				// Return the service instance
				heap.Pop(&m.returnQueue)
				m.ejectedMu.Lock()
				delete(m.ejected, next.InstanceID)
				delete(m.instances, next.InstanceID)
				m.ejectedMu.Unlock()
				m.logger.Infof("Service instance %q returned", next.InstanceID)
			}
		case <-m.done:
			ticker.Stop()
			return
		}
	}
}

func (m *OutlierMonitor) IsServiceable(proxyInfo *ProxyInfo) bool {
	m.ejectedMu.RLock()
	defer m.ejectedMu.RUnlock()
	_, exists := m.ejected[proxyInfo.Destination.ID]
	return !exists
}

func (m *OutlierMonitor) ReportSuccess(proxyInfo *ProxyInfo) {
	m.Success <- proxyInfo
}

func (m *OutlierMonitor) ReportFailure(proxyInfo *ProxyInfo) {
	m.Failure <- proxyInfo
}

func (m *OutlierMonitor) Close() {
	close(m.done)
}

type EjectedInstance struct {
	InstanceID uuid.UUID
	Expiry     time.Time
}

type PriorityQueue []*EjectedInstance

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest based on expiration number as the priority
	// The lower the expiry, the higher the priority
	return pq[i].Expiry.Before(pq[j].Expiry)
}

// Implementation of heap interface.

func (pq *PriorityQueue) Peek() interface{} {
	n := len(*pq)
	item := (*pq)[n-1]
	return item
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*EjectedInstance)
	*pq = append(*pq, item)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

package enhanced

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/duyhai-bic/rds_exporter/sessions"
)

// Collector collects enhanced RDS metrics by utilizing several scrapers.
type Collector struct {
	sessions *sessions.Sessions
	logger   log.Logger

	rw         sync.RWMutex
	metrics    map[string][]prometheus.Metric
	cancelFunc context.CancelFunc
}

// Maximal and minimal metrics update interval.
const (
	maxInterval = 60 * time.Second
	minInterval = 2 * time.Second
)

// NewCollector creates new collector and starts scrapers.
func NewCollector(sessions *sessions.Sessions, logger log.Logger) *Collector {
	c := &Collector{
		sessions: sessions,
		logger:   log.With(logger, "component", "enhanced"),
		metrics:  make(map[string][]prometheus.Metric),
	}
	// Create a cancellable context for all goroutines in this collector.
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelFunc = cancel

	for session, instances := range sessions.AllSessions() {
		enabledInstances := getEnabledInstances(instances)
		s := newScraper(session, enabledInstances, logger)

		interval := maxInterval
		for _, instance := range enabledInstances {
			if instance.EnhancedMonitoringInterval > 0 && instance.EnhancedMonitoringInterval < interval {
				interval = instance.EnhancedMonitoringInterval
			}
		}
		if interval < minInterval {
			interval = minInterval
		}
		level.Info(s.logger).Log("msg", fmt.Sprintf("Updating enhanced metrics every %s.", interval))

		// perform first scrapes synchronously so returned collector has all metric descriptions
		level.Info(s.logger).Log("msg", "perform first scrapes synchronously so returned collector has all metric descriptions")
		m, _ := s.scrape(context.TODO())
		c.setMetrics(m)

		level.Info(s.logger).Log("msg", "Start performing scrapes periodically")
		ch := make(chan map[string][]prometheus.Metric)
		go func() {
			for {
				select {
				case m, ok := <-ch:
					if !ok {
						// Channel closed: exit the goroutine.
						level.Info(s.logger).Log("msg", "metrics channel closed, exiting goroutine")
						return
					}
					c.setMetrics(m)
				case <-ctx.Done():
					// Context cancelled: exit the goroutine.
					level.Info(s.logger).Log("msg", "context cancelled, exiting metrics updater")
					return
				}
			}
		}()
		go s.start(ctx, interval, ch)
	}

	return c
}

func getEnabledInstances(instances []sessions.Instance) []sessions.Instance {
	enabledInstances := make([]sessions.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.DisableEnhancedMetrics {
			continue
		}
		enabledInstances = append(enabledInstances, instance)
	}

	return enabledInstances
}

// setMetrics saves latest scraped metrics.
func (c *Collector) setMetrics(m map[string][]prometheus.Metric) {
	c.rw.Lock()
	for id, metrics := range m {
		c.metrics[id] = metrics
	}
	c.rw.Unlock()
}

func (c *Collector) Update(sessions *sessions.Sessions, logger log.Logger) {
	level.Info(c.logger).Log("msg", "Canceling the old enhanced monitoring scraper")
	c.cancelFunc()
	level.Info(c.logger).Log("msg", "The old enhanced monitoring scraper has been canceled")
	newCollector := NewCollector(sessions, logger)
	c.cancelFunc = newCollector.cancelFunc
	c.logger = newCollector.logger
	c.sessions = newCollector.sessions
	c.metrics = newCollector.metrics
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	// unchecked collector
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.rw.RLock()
	defer c.rw.RUnlock()

	for _, metrics := range c.metrics {
		for _, m := range metrics {
			ch <- m
		}
	}
}

// check interfaces
var (
	_ prometheus.Collector = (*Collector)(nil)
)

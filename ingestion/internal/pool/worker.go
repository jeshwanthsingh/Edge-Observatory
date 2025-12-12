package pool

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrPoolFull = errors.New("worker pool is full")

	poolJobsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pool_jobs_active",
		Help: "Active jobs currently running in the worker pool",
	})
	poolJobsQueued = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pool_jobs_queued",
		Help: "Jobs currently buffered/queued in the worker pool",
	})
	poolJobsRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pool_jobs_rejected_total",
		Help: "Total number of jobs rejected because the worker pool buffer was full",
	})
)

type Job func()

type WorkerPool struct {
	jobs chan Job
	wg   sync.WaitGroup
	size int
}

func New(size int) *WorkerPool {
	bufferSize := size * 10
	p := &WorkerPool{
		jobs: make(chan Job, bufferSize),
		size: size,
	}

	p.wg.Add(size)
	for i := 0; i < size; i++ {
		go p.worker()
	}

	return p
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for job := range p.jobs {
		poolJobsQueued.Dec()
		poolJobsActive.Inc()
		job()
		poolJobsActive.Dec()
	}
}

func (p *WorkerPool) Submit(job Job) error {
	select {
	case p.jobs <- job:
		poolJobsQueued.Inc()
		return nil
	default:
		poolJobsRejectedTotal.Inc()
		return ErrPoolFull
	}
}

func (p *WorkerPool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
}

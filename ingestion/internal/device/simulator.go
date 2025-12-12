package device

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/jeshwanthsingh/edge-observatory/ingestion/internal/pool"
	"github.com/jeshwanthsingh/edge-observatory/ingestion/proto"
)

type DeviceType int

const (
	MacEndpoint DeviceType = iota
	EdgeNode
	IoTSensor
)

type Device struct {
	ID        string
	Type      DeviceType
	Region    string
	battery   float64 // state for IoTSensor
}

type Simulator struct {
	devices    []*Device
	macDevs    []*Device
	edgeDevs   []*Device
	iotDevs    []*Device
	pool       *pool.WorkerPool
	ratePerSec int
}

func New(count int, ratePerSec int, p *pool.WorkerPool) *Simulator {
	regions := []string{"us-west", "us-east", "eu-west", "ap-southeast"}
	s := &Simulator{
		devices:    make([]*Device, 0, count),
		macDevs:    make([]*Device, 0),
		edgeDevs:   make([]*Device, 0),
		iotDevs:    make([]*Device, 0),
		pool:       p,
		ratePerSec: ratePerSec,
	}

	for i := 0; i < count; i++ {
		r := rand.Intn(100)
		dt := MacEndpoint
		if r >= 70 {
			dt = IoTSensor
		} else if r >= 40 {
			dt = EdgeNode
		}

		dev := &Device{
			ID:      fmt.Sprintf("dev-%08x", rand.Uint32()),
			Type:    dt,
			Region:  regions[rand.Intn(len(regions))],
			battery: 100.0,
		}

		s.devices = append(s.devices, dev)
		switch dt {
		case MacEndpoint:
			s.macDevs = append(s.macDevs, dev)
		case EdgeNode:
			s.edgeDevs = append(s.edgeDevs, dev)
		case IoTSensor:
			s.iotDevs = append(s.iotDevs, dev)
		}
	}

	return s
}

func (s *Simulator) Start(ctx context.Context, publish func(metrics *proto.DeviceMetrics) error) {
	if len(s.macDevs) > 0 {
		go s.runGroup(ctx, s.macDevs, float64(s.ratePerSec)*float64(len(s.macDevs))/float64(len(s.devices)), publish)
	}
	if len(s.edgeDevs) > 0 {
		go s.runGroup(ctx, s.edgeDevs, float64(s.ratePerSec)*float64(len(s.edgeDevs))/float64(len(s.devices)), publish)
	}
	if len(s.iotDevs) > 0 {
		go s.runGroup(ctx, s.iotDevs, float64(s.ratePerSec)*float64(len(s.iotDevs))/float64(len(s.devices)), publish)
	}
}

func (s *Simulator) runGroup(ctx context.Context, devs []*Device, rate float64, publish func(*proto.DeviceMetrics) error) {
	if rate <= 0 {
		return
	}
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dev := devs[rand.Intn(len(devs))]
			metrics := dev.generateMetrics()
			s.pool.Submit(func() {
				_ = publish(metrics)
			})
		}
	}
}

func (d *Device) generateMetrics() *proto.DeviceMetrics {
	m := &proto.DeviceMetrics{
		DeviceId:     d.ID,
		TimestampMs:  time.Now().UnixMilli(),
		Region:       d.Region,
		OsCompliant:  true,
	}

	switch d.Type {
	case MacEndpoint:
		m.Type = proto.DeviceType_MAC_ENDPOINT
		if rand.Float64() < 0.05 {
			m.CpuPercent = 99.0
		} else {
			m.CpuPercent = 20.0 + rand.Float64()*75.0 // 20-95
		}
		m.MemoryPercent = 40.0 + rand.Float64()*50.0 // 40-90
		m.BatteryHealth = 80.0 + rand.Float64()*20.0 // 80-100

	case EdgeNode:
		m.Type = proto.DeviceType_EDGE_NODE
		m.CpuPercent = 10.0 + rand.Float64()*60.0 // 10-70
		if rand.Float64() < 0.03 {
			m.NetworkLatencyMs = 1000.0 + rand.Float64()*4000.0 // 1000-5000
		} else {
			m.NetworkLatencyMs = 1.0 + rand.Float64()*499.0 // 1-500
		}

	case IoTSensor:
		m.Type = proto.DeviceType_IOT_SENSOR
		d.battery -= 0.1
		if d.battery < 0 {
			d.battery = 0
		}
		m.BatteryHealth = d.battery
		if d.battery < 10.0 {
			m.OsCompliant = false // critical anomaly
		}
	}

	return m
}

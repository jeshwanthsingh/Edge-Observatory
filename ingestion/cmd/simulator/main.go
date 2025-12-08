package main

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	pb "github.com/jeshwanthsingh/edge-observatory/ingestion/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RegionDistribution struct {
	Region string
	Weight float64
}

var regions = []RegionDistribution{
	{"us-east", 0.30},
	{"us-west", 0.20},
	{"eu-west", 0.25},
	{"ap-southeast", 0.15},
	{"ap-northeast", 0.10},
}

func getRandRegion() string {
	r := rand.Float64()
	sum := 0.0
	for _, dist := range regions {
		sum += dist.Weight
		if r <= sum {
			return dist.Region
		}
	}
	return "us-east"
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	valStr := getEnv(key, "")
	if valStr == "" {
		return fallback
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return fallback
	}
	return val
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	gatewayAddr := getEnv("GATEWAY_ADDR", "gateway:9090")
	baseRPS := getEnvFloat("BASE_RPS", 50.0)
	burstRPS := getEnvFloat("BURST_RPS", 500.0)
	periodSeconds := getEnvFloat("PERIOD_SECONDS", 600.0)

	amplitude := 30.0

	logger.Info("Starting Sinusoidal Simulator",
		zap.String("gateway", gatewayAddr),
		zap.Float64("base_rps", baseRPS),
		zap.Float64("burst_rps", burstRPS),
		zap.Float64("period_seconds", periodSeconds),
	)

	conn, err := grpc.Dial(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to gateway", zap.Error(err))
	}
	defer conn.Close()

	client := pb.NewTelemetryGatewayClient(conn)

	startTime := time.Now()
	ticker := time.NewTicker(10 * time.Millisecond) // tick frequently to evaluate traffic
	defer ticker.Stop()

	lastLog := time.Now()
	devices := 100

	rand.Seed(time.Now().UnixNano())

	callsInLastSecond := 0
	lastSecondTick := time.Now()

	for {
		now := time.Now()
		elapsed := now.Sub(startTime).Seconds()
		elapsedMinutes := now.Sub(startTime).Minutes()

		// Determine mode and RPS target
		mode := "sinusoidal"
		targetRPS := baseRPS + amplitude*math.Sin(2*math.Pi*elapsed/periodSeconds)

		// Check for burst: every 15 minutes, for 30 seconds
		// 15 minutes = 900 seconds
		if int(elapsed)%900 < 30 && elapsed > 30 {
			mode = "burst"
			targetRPS = burstRPS
		}

		// Logging every 10 seconds
		if now.Sub(lastLog) >= 10*time.Second {
			if mode == "burst" {
				logger.Info("Burst mode",
					zap.Float64("rps", targetRPS),
					zap.String("duration", "30s"),
				)
			} else {
				logger.Info("Traffic stats",
					zap.Float64("current_rps", targetRPS),
					zap.String("mode", mode),
					zap.Float64("elapsed_minutes", elapsedMinutes),
				)
			}
			lastLog = now
		}

		// Calculate sleep interval correctly if we want precise RPS,
		// but since we are in a tight loop checking ticker, we can emit requests.
		// A better approach: calculate if it's time to send the next message based on targetRPS.

		// Let's ensure we send targetRPS per second on average.
		if now.Sub(lastSecondTick) >= 1*time.Second {
			callsInLastSecond = 0
			lastSecondTick = now
		}

		// Probabilistic approach for smooth distribution:
		// targetRPS is expected events per second.
		// Ticker is 10ms (100 ticks per second).
		// Probability of sending an event on this tick = targetRPS / 100.
		// E.g., at 50 RPS, P = 0.5. At 500 RPS, P = 5.0 (need to send 5 instantly).

		expectedPerTick := targetRPS * 0.01

		toSend := int(expectedPerTick)
		remainder := expectedPerTick - float64(toSend)
		if rand.Float64() < remainder {
			toSend++
		}

		for i := 0; i < toSend; i++ {
			deviceID := "sim-device-" + strconv.Itoa(rand.Intn(devices))
			region := getRandRegion()

			req := &pb.DeviceMetrics{
				DeviceId:         deviceID,
				Type:             pb.DeviceType_MAC_ENDPOINT,
				TimestampMs:      time.Now().UnixMilli(),
				CpuPercent:       rand.Float64(),
				MemoryPercent:    rand.Float64(),
				BatteryHealth:    rand.Float64(),
				NetworkLatencyMs: rand.Float64() * 100,
				OsCompliant:      true,
				Region:           region,
			}

			go func(metrics *pb.DeviceMetrics, reg string) {
				// Artificial jitter simulation
				if rand.Float64() < 0.05 {
					time.Sleep(500 * time.Millisecond)
				}
				if rand.Float64() < 0.02 {
					time.Sleep(2 * time.Second)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Increased timeout for jitter
				defer cancel()
				_, err := client.IngestMetrics(ctx, metrics)
				if err != nil {
					logger.Warn("Failed to ingest metrics (jittered)",
						zap.Error(err),
						zap.String("region", reg),
						zap.String("device", metrics.DeviceId))
				}
			}(req, region)

			callsInLastSecond++
		}

		<-ticker.C
	}
}

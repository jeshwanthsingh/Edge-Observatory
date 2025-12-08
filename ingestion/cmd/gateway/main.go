package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jeshwanthsingh/edge-observatory/ingestion/internal/pool"
	"github.com/jeshwanthsingh/edge-observatory/ingestion/internal/producer"
	pb "github.com/jeshwanthsingh/edge-observatory/ingestion/proto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

var (
	grpcRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_requests_total",
		Help: "Total number of gRPC requests",
	}, []string{"method", "status"})

	grpcRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_request_duration_seconds",
		Help:    "Duration of gRPC requests",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	}, []string{"method"})

	chaosEnabled atomic.Bool
)

func init() {
	chaosEnabled.Store(true)
}

func unaryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		start := time.Now()

		defer func() {
			if r := recover(); r != nil {
				logger.Error("Panic recovered", zap.Any("recover", r))
				err = status.Errorf(codes.Internal, "panic recovered: %v", r)
			}
		}()

		resp, err = handler(ctx, req)

		duration := time.Since(start).Seconds()
		st := "success"
		if err != nil {
			st = "error"
		}
		grpcRequestsTotal.WithLabelValues(info.FullMethod, st).Inc()
		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration)

		return resp, err
	}
}

func streamInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		start := time.Now()

		defer func() {
			if r := recover(); r != nil {
				logger.Error("Panic recovered in stream", zap.Any("recover", r))
				err = status.Errorf(codes.Internal, "panic recovered in stream: %v", r)
			}
		}()

		err = handler(srv, ss)

		duration := time.Since(start).Seconds()
		st := "success"
		if err != nil {
			st = "error"
		}
		grpcRequestsTotal.WithLabelValues(info.FullMethod, st).Inc()
		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration)

		return err
	}
}

type server struct {
	pb.UnimplementedTelemetryGatewayServer
	pool     *pool.WorkerPool
	producer *producer.Producer
	logger   *zap.Logger
}

func (s *server) IngestMetrics(ctx context.Context, req *pb.DeviceMetrics) (*pb.IngestResponse, error) {
	err := s.pool.Submit(func() {
		data, err := gproto.Marshal(req)
		if err != nil {
			s.logger.Error("Failed to marshal metrics", zap.Error(err))
			return
		}
		if err := s.producer.Publish("metrics-raw", req.DeviceId, data); err != nil {
			s.logger.Error("Failed to publish to kafka", zap.Error(err))
		}
	})

	if err != nil {
		if err == pool.ErrPoolFull {
			return nil, status.Error(codes.ResourceExhausted, "worker pool is full")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.IngestResponse{Accepted: true, Message: "queued"}, nil
}

func (s *server) IngestStream(stream pb.TelemetryGateway_IngestStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		err = s.pool.Submit(func() {
			data, marErr := gproto.Marshal(req)
			if marErr != nil {
				s.logger.Error("Marshal error", zap.Error(marErr))
				return
			}
			s.producer.Publish("metrics-raw", req.DeviceId, data)
		})

		resp := &pb.IngestResponse{Accepted: err == nil}
		if err != nil {
			resp.Message = err.Error()
		}
		if sendErr := stream.Send(resp); sendErr != nil {
			return sendErr
		}
	}
}

func (s *server) chaosHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Failed to read chaos body", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := s.producer.Publish("chaos-events", "chaos", body); err != nil {
		s.logger.Error("Failed to publish chaos event", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("chaos logged"))
}

func (s *server) chaosStopHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	chaosEnabled.Store(false)
	s.logger.Warn("Chaos EMERGENCY STOP triggered")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "chaos_stopped",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (s *server) chaosStartHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	chaosEnabled.Store(true)
	s.logger.Info("Chaos emergency stop CLEARED")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "chaos_started",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (s *server) chaosStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"chaos_enabled": chaosEnabled.Load(),
	})
}

func (s *server) telemetryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Failed to read telemetry body", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse custom JSON payload from CF Worker
	var cfPayload struct {
		Region    string  `json:"region"`
		Latency   float64 `json:"latency"`
		Timestamp string  `json:"timestamp"`
	}

	if err := json.Unmarshal(body, &cfPayload); err != nil {
		s.logger.Error("Invalid telemetry json", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Marshal CF payload into the Protobuf struct expected by the consumers
	req := &pb.DeviceMetrics{
		DeviceId:         cfPayload.Region,
		Type:             pb.DeviceType_EDGE_NODE,
		CpuPercent:       0.5, // Mock data for now since CF only reports latency
		MemoryPercent:    0.5,
		NetworkLatencyMs: cfPayload.Latency,
		TimestampMs:      time.Now().UnixMilli(),
		Region:           cfPayload.Region,
	}

	// Queue it up in the worker pool
	err = s.pool.Submit(func() {
		data, err := gproto.Marshal(req)
		if err != nil {
			s.logger.Error("Failed to marshal edge metrics", zap.Error(err))
			return
		}
		if err := s.producer.Publish("metrics-raw", req.DeviceId, data); err != nil {
			s.logger.Error("Failed to publish edge metrics to kafka", zap.Error(err))
		}
	})

	if err != nil {
		s.logger.Error("Telemetry worker pool full", zap.Error(err))
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("edge telemetry logged"))
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	workerPoolSize, _ := strconv.Atoi(getEnv("WORKER_POOL_SIZE", "100"))
	grpcPort := getEnv("GRPC_PORT", "9090")
	metricsPort := getEnv("METRICS_PORT", "8080")

	// Init Producer
	prod, err := producer.New(strings.Split(kafkaBrokers, ","))
	if err != nil {
		logger.Fatal("Failed to create kafka producer", zap.Error(err))
	}

	// Init Pool
	workerPool := pool.New(workerPoolSize)

	// Setup gRPC Server
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(unaryInterceptor(logger)),
		grpc.StreamInterceptor(streamInterceptor(logger)),
	)

	pb.RegisterTelemetryGatewayServer(grpcServer, &server{
		pool:     workerPool,
		producer: prod,
		logger:   logger,
	})

	// Setup Metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("Starting metrics server", zap.String("port", metricsPort))
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			logger.Error("Metrics server failed", zap.Error(err))
		}
	}()

	// Setup Health
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		})
		logger.Info("Starting health server", zap.String("port", "8081"))
		if err := http.ListenAndServe(":8081", mux); err != nil {
			logger.Error("Health server failed", zap.Error(err))
		}
	}()

	// Setup Standard HTTP endpoints on 8080 (Chaos & Edge Telemetry)
	go func() {
		gw := &server{pool: workerPool, producer: prod, logger: logger}
		http.HandleFunc("/chaos", gw.chaosHandler)
		http.HandleFunc("/chaos/stop", gw.chaosStopHandler)
		http.HandleFunc("/chaos/start", gw.chaosStartHandler)
		http.HandleFunc("/chaos/status", gw.chaosStatusHandler)
		http.HandleFunc("/telemetry", gw.telemetryHandler)
		logger.Info("Starting HTTP ingestion routes on :8080/chaos (/stop, /start, /status) and :8080/telemetry")
	}()

	// Start gRPC
	go func() {
		logger.Info("Starting gRPC server", zap.String("port", grpcPort))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	// Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("Shutting down gracefully...")
	grpcServer.GracefulStop()
	workerPool.Shutdown()
	prod.Close()
	logger.Info("Shutdown complete")
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

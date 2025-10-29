package dev.jeshwanth.observatory.anomaly;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@SpringBootApplication
public class AnomalyDetectorApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnomalyDetectorApplication.class, args);
    }
}

record DeviceMetrics(
        String deviceId,
        String type, // MAC_ENDPOINT, EDGE_NODE, IOT_SENSOR
        Long timestampMs,
        Double cpuPercent,
        Double memoryPercent,
        Double batteryHealth,
        Double networkLatencyMs,
        Boolean osCompliant,
        String region) {
}

record AnomalyEvent(
        String deviceId,
        String deviceType,
        String anomalyType,
        String reason,
        Long timestampMs) {
}

@Service
class AnomalyDetectionService {
    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionService.class);
    private final KafkaTemplate<String, AnomalyEvent> kafkaTemplate;

    private final Counter messagesConsumed;
    private final MeterRegistry registry;
    private final Timer detectionDuration;

    public AnomalyDetectionService(KafkaTemplate<String, AnomalyEvent> kafkaTemplate, MeterRegistry registry) {
        this.kafkaTemplate = kafkaTemplate;
        this.registry = registry;
        this.messagesConsumed = Counter.builder("messages_consumed_total")
                .description("Total messages consumed")
                .register(registry);
        this.detectionDuration = Timer.builder("detection_duration_ms")
                .description("Time taken to detect anomalies")
                .register(registry);
    }

    @KafkaListener(topics = "metrics-raw", groupId = "anomaly-detector")
    public void processMetrics(byte[] data) {
        try {
            dev.jeshwanth.observatory.telemetry.DeviceMetrics proto = dev.jeshwanth.observatory.telemetry.DeviceMetrics
                    .parseFrom(data);
            DeviceMetrics metrics = new DeviceMetrics(
                    proto.getDeviceId(),
                    proto.getType().name(),
                    proto.getTimestampMs(),
                    proto.getCpuPercent(),
                    proto.getMemoryPercent(),
                    proto.getBatteryHealth(),
                    proto.getNetworkLatencyMs(),
                    proto.getOsCompliant(),
                    proto.getRegion());
            handleMetrics(metrics);
        } catch (Exception e) {
            log.error("Failed to parse metrics: {}", e.getMessage());
        }
    }

    private void handleMetrics(DeviceMetrics metrics) {
        messagesConsumed.increment();
        detectionDuration.record(() -> {
            if (metrics == null || metrics.type() == null)
                return;

            switch (metrics.type()) {
                case "MAC_ENDPOINT":
                    if (metrics.cpuPercent() != null && metrics.cpuPercent() > 95) {
                        emitAnomaly(metrics, "HIGH_CPU", "CPU > 95%");
                    }
                    break;
                case "EDGE_NODE":
                    if (metrics.networkLatencyMs() != null && metrics.networkLatencyMs() > 1000) {
                        emitAnomaly(metrics, "HIGH_LATENCY", "Latency > 1000ms");
                    }
                    break;
                case "IOT_SENSOR":
                    if (metrics.batteryHealth() != null && metrics.batteryHealth() < 10) {
                        emitAnomaly(metrics, "LOW_BATTERY", "Battery < 10%");
                    }
                    break;
            }
        });
    }

    private void emitAnomaly(DeviceMetrics metrics, String anomalyType, String reason) {
        log.warn("Anomaly detected for device {}: {}", metrics.deviceId(), reason);
        Counter.builder("anomalies_detected_total")
                .description("Total anomalies detected")
                .tag("device_type", metrics.type())
                .tag("anomaly_type", anomalyType)
                .register(registry)
                .increment();

        AnomalyEvent event = new AnomalyEvent(
                metrics.deviceId(),
                metrics.type(),
                anomalyType,
                reason,
                System.currentTimeMillis());
        kafkaTemplate.send("metrics-anomaly", metrics.deviceId(), event);
    }
}

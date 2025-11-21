package dev.jeshwanth.observatory.state;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

@SpringBootApplication
public class StateAggregatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(StateAggregatorApplication.class, args);
    }
}

record DeviceMetrics(
        String deviceId,
        String type,
        Long timestampMs,
        Double cpuPercent,
        Double memoryPercent,
        Double batteryHealth,
        Double networkLatencyMs,
        Boolean osCompliant,
        String region) {
}

@Table("device_state")
class DeviceState {
    @Id
    private String deviceId;
    private String deviceType;
    private String region;
    private Double cpuPercent;
    private Double memoryPercent;
    private Double batteryHealth;
    private Double networkLatencyMs;
    private Boolean osCompliant;
    private Instant lastSeen;
    private Instant updatedAt;

    public DeviceState() {
    }

    public DeviceState(String deviceId, String deviceType, String region, Double cpuPercent, Double memoryPercent,
            Double batteryHealth, Double networkLatencyMs, Boolean osCompliant, Instant lastSeen, Instant updatedAt) {
        this.deviceId = deviceId;
        this.deviceType = deviceType;
        this.region = region;
        this.cpuPercent = cpuPercent;
        this.memoryPercent = memoryPercent;
        this.batteryHealth = batteryHealth;
        this.networkLatencyMs = networkLatencyMs;
        this.osCompliant = osCompliant;
        this.lastSeen = lastSeen;
        this.updatedAt = updatedAt;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }
}

interface DeviceStateRepository extends CrudRepository<DeviceState, String> {
    @org.springframework.data.jdbc.repository.query.Modifying
    @org.springframework.data.jdbc.repository.query.Query("INSERT INTO device_state (device_id, device_type, region, cpu_percent, memory_percent, battery_health, network_latency_ms, os_compliant, last_seen, updated_at) VALUES (:deviceId, :deviceType, :region, :cpuPercent, :memoryPercent, :batteryHealth, :networkLatencyMs, :osCompliant, :lastSeen, NOW()) ON CONFLICT (device_id) DO UPDATE SET device_type=EXCLUDED.device_type, region=EXCLUDED.region, cpu_percent=EXCLUDED.cpu_percent, memory_percent=EXCLUDED.memory_percent, battery_health=EXCLUDED.battery_health, network_latency_ms=EXCLUDED.network_latency_ms, os_compliant=EXCLUDED.os_compliant, last_seen=EXCLUDED.last_seen, updated_at=NOW()")
    void upsert(@org.springframework.data.repository.query.Param("deviceId") String deviceId,
            @org.springframework.data.repository.query.Param("deviceType") String deviceType,
            @org.springframework.data.repository.query.Param("region") String region,
            @org.springframework.data.repository.query.Param("cpuPercent") Double cpuPercent,
            @org.springframework.data.repository.query.Param("memoryPercent") Double memoryPercent,
            @org.springframework.data.repository.query.Param("batteryHealth") Double batteryHealth,
            @org.springframework.data.repository.query.Param("networkLatencyMs") Double networkLatencyMs,
            @org.springframework.data.repository.query.Param("osCompliant") Boolean osCompliant,
            @org.springframework.data.repository.query.Param("lastSeen") Instant lastSeen);
}

@Service
class StateAggregationService {
    private static final Logger log = LoggerFactory.getLogger(StateAggregationService.class);
    private final DeviceStateRepository repository;
    private final StringRedisTemplate redisTemplate;

    private final Counter dbWrites;
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final Counter redisWriteErrors;

    public StateAggregationService(DeviceStateRepository repository, StringRedisTemplate redisTemplate,
            MeterRegistry registry) {
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.dbWrites = Counter.builder("db_writes_total").description("Total database writes").register(registry);
        this.cacheHits = Counter.builder("cache_hits_total").description("Total cache hits").register(registry);
        this.cacheMisses = Counter.builder("cache_misses_total").description("Total cache misses").register(registry);
        this.redisWriteErrors = Counter.builder("redis_write_errors_total").description("Total redis write errors")
                .register(registry);
    }

    @KafkaListener(topics = "metrics-raw", groupId = "state-aggregator")
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
        if (metrics == null || metrics.deviceId() == null)
            return;

        String cacheKey = "device:state:" + metrics.deviceId();
        boolean inCache = false;

        try {
            inCache = Boolean.TRUE.equals(redisTemplate.hasKey(cacheKey));
            if (inCache) {
                cacheHits.increment();
            } else {
                cacheMisses.increment();
            }
        } catch (Exception e) {
            log.warn("Redis read failed: {}", e.getMessage());
        }

        Instant lastSeen = Instant
                .ofEpochMilli(metrics.timestampMs() != null ? metrics.timestampMs() : System.currentTimeMillis());
        DeviceState state = new DeviceState(
                metrics.deviceId(),
                metrics.type() != null ? metrics.type() : "UNKNOWN",
                metrics.region(),
                metrics.cpuPercent(),
                metrics.memoryPercent(),
                metrics.batteryHealth(),
                metrics.networkLatencyMs(),
                metrics.osCompliant(),
                lastSeen,
                Instant.now());

        if (!inCache) {
            try {
                repository.upsert(
                        metrics.deviceId(),
                        metrics.type() != null ? metrics.type() : "UNKNOWN",
                        metrics.region(),
                        metrics.cpuPercent(),
                        metrics.memoryPercent(),
                        metrics.batteryHealth(),
                        metrics.networkLatencyMs(),
                        metrics.osCompliant(),
                        lastSeen);
                dbWrites.increment();
            } catch (Exception e) {
                log.error("DB write failed: {}", e.getMessage());
            }

            try {
                redisTemplate.opsForValue().set(cacheKey, "1", Duration.ofSeconds(30));
            } catch (Exception e) {
                redisWriteErrors.increment();
                log.warn("Redis write failed: {}", e.getMessage());
            }
        } else {
            // In cache hot path: skip PostgreSQL save and extend Redis TTL
            try {
                redisTemplate.expire(cacheKey, Duration.ofSeconds(30));
            } catch (Exception e) {
                redisWriteErrors.increment();
            }
        }
    }
}

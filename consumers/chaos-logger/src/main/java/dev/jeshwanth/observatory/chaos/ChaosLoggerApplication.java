package dev.jeshwanth.observatory.chaos;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@SpringBootApplication
public class ChaosLoggerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ChaosLoggerApplication.class, args);
    }
}

record ChaosEvent(
        @JsonProperty("event_type") String eventType,
        String target,
        @JsonProperty("started_at") java.time.Instant startedAt,
        @JsonProperty("resolved_at") java.time.Instant resolvedAt,
        @JsonProperty("duration_ms") Long durationMs,
        Boolean dryRun) {
}

@Table("chaos_incidents")
class ChaosIncident {
    @Id
    private Long id;
    private String scenario;
    private Instant startedAt;
    private Instant recoveredAt;
    private Long recoveryDurationMs;
    private String affectedComponent;
    private String details;
    private Instant createdAt;

    public ChaosIncident() {
    }

    public ChaosIncident(String scenario, Instant startedAt, Instant recoveredAt, Long recoveryDurationMs,
            String affectedComponent, String details, Instant createdAt) {
        this.scenario = scenario;
        this.startedAt = startedAt;
        this.recoveredAt = recoveredAt;
        this.recoveryDurationMs = recoveryDurationMs;
        this.affectedComponent = affectedComponent;
        this.details = details;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getScenario() {
        return scenario;
    }
}

interface ChaosIncidentRepository extends CrudRepository<ChaosIncident, Long> {
}

@Service
class ChaosLoggingService {
    private static final Logger log = LoggerFactory.getLogger(ChaosLoggingService.class);
    private final ChaosIncidentRepository repository;
    private final MeterRegistry registry;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public ChaosLoggingService(ChaosIncidentRepository repository, MeterRegistry registry) {
        this.repository = repository;
        this.registry = registry;
    }

    @KafkaListener(topics = "chaos-events", groupId = "chaos-logger")
    public void processEvent(byte[] data) {
        try {
            ChaosEvent event = objectMapper.readValue(data, ChaosEvent.class);
            handleEvent(event);
        } catch (Exception e) {
            log.error("Failed to parse chaos event: {}", e.getMessage());
        }
    }

    private void handleEvent(ChaosEvent event) {
        if (event == null || event.eventType() == null)
            return;

        Counter.builder("chaos_events_received_total")
                .description("Total chaos events received")
                .tag("scenario", event.eventType())
                .register(registry)
                .increment();

        Instant start = event.startedAt() != null ? event.startedAt() : Instant.now();
        Instant end = event.resolvedAt();

        if (event.durationMs() != null) {
            DistributionSummary.builder("chaos_recovery_duration_ms")
                    .description("MTTR metric for chaos events")
                    .serviceLevelObjectives(1000, 5000, 10000, 30000, 60000)
                    .register(registry)
                    .record(event.durationMs());
        }

        ChaosIncident incident = new ChaosIncident(
                event.eventType(),
                start,
                end,
                event.durationMs(),
                event.target(),
                "DryRun: " + event.dryRun(),
                Instant.now());

        try {
            repository.save(incident);
        } catch (Exception e) {
            log.error("Failed to log chaos incident to PostgreSQL: {}", e.getMessage());
        }
    }
}

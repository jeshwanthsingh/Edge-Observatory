package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// AlertmanagerWebhookPayload represents the JSON sent by Alertmanager
type AlertmanagerWebhookPayload struct {
	Receiver string  `json:"receiver"`
	Status   string  `json:"status"`
	Alerts   []Alert `json:"alerts"`
}

type Alert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    string            `json:"startsAt"`
}

type Janitor struct {
	logger       *zap.Logger
	db           *sql.DB
	env          string
	grafanaToken string
}

func (j *Janitor) webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		j.logger.Error("Failed to read webhook body", zap.Error(err))
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload AlertmanagerWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		j.logger.Error("Failed to parse webhook JSON", zap.Error(err))
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	j.logger.Info("Received webhook", zap.String("status", payload.Status), zap.Int("alerts", len(payload.Alerts)))

	for _, alert := range payload.Alerts {
		if alert.Status != "firing" {
			continue // Only act on firing alerts
		}

		alertName := alert.Labels["alertname"]

		switch alertName {
		case "KafkaLatencyHigh", "ConsumerGroupLag", "ServiceDown":
			j.logger.Warn("Critical alert detected, initiating remediation", zap.String("alert", alertName))

			targetName := "kafka"
			if alertName == "ConsumerGroupLag" {
				targetName = "consumer-metrics"
			}

			// If it's ServiceDown, check labels to see what service is down
			if alertName == "ServiceDown" {
				jobToDeployment := map[string]string{
					"kafka":            "kafka",
					"anomaly-detector": "consumer-anomaly",
					"state-aggregator": "consumer-metrics",
					"chaos-logger":     "consumer-chaos",
					"gateway":          "gateway",
				}
				job := alert.Labels["job"]
				deployment, ok := jobToDeployment[job]
				if !ok {
					j.logger.Info("ServiceDown alert received for non-remediable target, skipping", zap.String("job", job))
					continue
				}
				targetName = deployment
			}

			err = j.remediateTask(r.Context(), targetName, alertName)
			if err != nil {
				j.logger.Error("Remediation failed", zap.Error(err))
			}
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (j *Janitor) remediateTask(ctx context.Context, targetName, trigger string) error {
	j.logger.Info("Attempting remediation", zap.String("target", targetName), zap.String("env", j.env))

	j.postAnnotation(targetName, "START_HEAL")

	start := time.Now()
	var err error

	if j.env == "prod" {
		err = j.restartKubernetesDeployment(ctx, targetName)
	} else {
		err = j.restartDockerContainer(ctx, targetName)
	}

	duration := time.Since(start)

	// Log the action to the DB regardless of success/fail, so it appears in Grafana annotations
	status := "success"
	var errMsg string
	if err != nil {
		status = "failed"
		errMsg = err.Error()
	} else {
		j.postAnnotation(targetName, "COMPLETE_HEAL")
	}

	j.logIncident(targetName, trigger, status, duration.Milliseconds(), errMsg)

	return err
}

func (j *Janitor) postAnnotation(target, action string) {
	grafanaBase := getEnv("GRAFANA_URL", "http://grafana:3000")
	primaryURL := grafanaBase + "/api/annotations"
	fallbackURL := primaryURL

	color := "#FF0000"
	if action == "COMPLETE_HEAL" {
		color = "#00FF00"
	}

	payload := map[string]interface{}{
		"text":  "Janitor: " + action + " on " + target,
		"tags":  []string{"janitor", "remediation", target},
		"color": color,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		j.logger.Error("Failed to marshal annotation payload", zap.Error(err))
		return
	}

	sendReq := func(reqURL string) bool {
		req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewBuffer(body))
		if err != nil {
			j.logger.Error("Failed to create annotation request", zap.Error(err), zap.String("url", reqURL))
			return false
		}

		// Use a Grafana API token/Bearer token instead of basic auth
		token := j.grafanaToken
		if token == "" {
			token = getEnv("GRAFANA_TOKEN", "")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			j.logger.Warn("Failed to post annotation", zap.Error(err), zap.String("url", reqURL))
			return false
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			respBody, _ := io.ReadAll(resp.Body)
			j.logger.Warn("Failed to post annotation, bad status", zap.Int("status", resp.StatusCode), zap.String("response", string(respBody)), zap.String("url", reqURL))
			return false
		}

		j.logger.Info("Posted Grafana annotation successfully", zap.String("action", action), zap.String("target", target), zap.String("url", reqURL))
		return true
	}

	// Try primary
	success := sendReq(primaryURL)
	if !success {
		j.logger.Warn("Primary annotation failed, waiting 2s to retry with fallback")
		time.Sleep(2 * time.Second)
		sendReq(fallbackURL)
	}
}

func (j *Janitor) restartDockerContainer(ctx context.Context, name string) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithVersion("1.44"))
	if err != nil {
		return fmt.Errorf("failed to connect to docker: %w", err)
	}
	defer cli.Close()

	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	var targetID string
	for _, c := range containers {
		for _, n := range c.Names {
			// Docker names start with a slash
			if strings.Contains(n, name) {
				targetID = c.ID
				break
			}
		}
		if targetID != "" {
			break
		}
	}

	if targetID == "" {
		return fmt.Errorf("container matching '%s' not found", name)
	}

	j.logger.Info("Restarting container", zap.String("id", targetID[:12]), zap.String("name", name))

	err = cli.ContainerRestart(ctx, targetID, container.StopOptions{})
	if err != nil {
		return fmt.Errorf("failed to restart container: %w", err)
	}

	return nil
}

func (j *Janitor) restartKubernetesDeployment(ctx context.Context, name string) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	if name == "kafka" {
		return j.restartStrimziKafka(ctx, config)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create k8s client: %w", err)
	}

	ns := "chaos-consumers"
	if name == "gateway" {
		ns = "chaos-ingestion"
	}
	j.logger.Info("Patching deployment to force restart", zap.String("name", name), zap.String("namespace", ns))

	patchData := fmt.Sprintf(`{"spec": {"template": {"metadata": {"annotations": {"chaos-janitor/restartedAt": "%s"}}}}}`, time.Now().Format(time.RFC3339))

	_, err = clientset.AppsV1().Deployments(ns).Patch(ctx, name, k8stypes.StrategicMergePatchType, []byte(patchData), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch deployment %s: %w", name, err)
	}

	return nil
}

func (j *Janitor) restartStrimziKafka(ctx context.Context, config *rest.Config) error {
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	kafkaGVR := schema.GroupVersionResource{
		Group:    "kafka.strimzi.io",
		Version:  "v1beta2",
		Resource: "kafkas",
	}

	kafkaName := getEnv("KAFKA_CR_NAME", "chaos-cluster")
	kafkaNS := getEnv("KAFKA_CR_NAMESPACE", "kafka")

	j.logger.Info("Annotating Strimzi Kafka CR for rolling restart", zap.String("name", kafkaName), zap.String("namespace", kafkaNS))

	patchData := `{"metadata": {"annotations": {"strimzi.io/manual-rolling-update": "true"}}}`
	_, err = dynClient.Resource(kafkaGVR).Namespace(kafkaNS).Patch(ctx, kafkaName, k8stypes.MergePatchType, []byte(patchData), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to annotate Kafka CR: %w", err)
	}

	j.logger.Info("Strimzi Kafka CR annotated for rolling restart", zap.String("name", kafkaName))
	return nil
}

func (j *Janitor) logIncident(target, trigger, status string, durationMs int64, details string) {
	if j.db == nil {
		j.logger.Warn("Database not connected, skipping incident log")
		return
	}

	query := `
		INSERT INTO chaos_incidents (scenario, affected_component, started_at, recovered_at, recovery_duration_ms)
		VALUES ($1, $2, NOW(), NOW() + ($3 * interval '1 millisecond'), $3)
		RETURNING id
	`
	// We're repurposing chaos_incidents slightly to log remedies too.
	// Prefix event_type with "REMEDY_"
	eventType := "REMEDY_" + trigger

	if status == "failed" {
		eventType = "REMEDY_FAILED_" + trigger
		j.logger.Error("Remedy failed", zap.String("details", details))
	}

	var incidentID int
	err := j.db.QueryRow(query, eventType, target, durationMs).Scan(&incidentID)
	if err != nil {
		j.logger.Error("Failed to write incident to database", zap.Error(err))
		return
	}

	j.logger.Info("Logged remedy incident to DB successfully", zap.Int("incident_id", incidentID))

	// Also insert into healing_actions
	actionName := "Restart " + target
	healQuery := `
		INSERT INTO healing_actions (incident_id, action_name, target_component, status, completed_at)
		VALUES ($1, $2, $3, $4, NOW())
	`
	_, err = j.db.Exec(healQuery, incidentID, actionName, target, status)
	if err != nil {
		j.logger.Error("Failed to write healing action to database", zap.Error(err))
	} else {
		j.logger.Info("Logged healing action to DB successfully")
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	env := getEnv("APP_ENV", "local")
	port := getEnv("PORT", "8080")
	dbURL := getEnv("SPRING_DATASOURCE_URL", "")

	j := &Janitor{
		logger: logger,
		env:    env,
	}

	// Fetch the service account token asynchronously after a delay
	go func() {
		token := fetchGrafanaToken(logger)
		if token != "" {
			j.grafanaToken = token
		}
	}()

	if dbURL != "" {
		// Extract JDBC specifics
		dbURL = strings.Replace(dbURL, "jdbc:postgresql://", "postgres://", 1)

		dbUsername := getEnv("SPRING_DATASOURCE_USERNAME", "postgres")
		dbPassword := getEnv("SPRING_DATASOURCE_PASSWORD", "postgres")

		// Very basic connection string assembly
		connStr := strings.Replace(dbURL, "postgres://", fmt.Sprintf("postgres://%s:%s@", dbUsername, dbPassword), 1)
		if !strings.Contains(connStr, "?") {
			connStr += "?sslmode=disable"
		} else if !strings.Contains(connStr, "sslmode=disable") {
			connStr += "&sslmode=disable"
		}

		db, err := sql.Open("postgres", connStr)
		if err != nil {
			logger.Error("Failed to open DB connection", zap.Error(err))
		} else {
			if err = db.Ping(); err != nil {
				logger.Error("Failed to ping DB", zap.Error(err))
			} else {
				logger.Info("Connected to PostgreSQL for logging")
				j.db = db
			}
		}
	}

	http.HandleFunc("/webhook", j.webhookHandler)
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	logger.Info("Starting Janitor service", zap.String("port", port), zap.String("env", env))
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		logger.Fatal("Server failed", zap.Error(err))
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func fetchGrafanaToken(logger *zap.Logger) string {
	grafanaBase := getEnv("GRAFANA_URL", "http://grafana:3000")
	client := &http.Client{Timeout: 5 * time.Second}
	var saID float64

	// wait for grafana to be up and return the service account
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest("GET", grafanaBase+"/api/serviceaccounts/search?query=janitor", nil)
		req.SetBasicAuth("admin", "admin")
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			var result map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&result)
			resp.Body.Close()

			if accounts, ok := result["serviceAccounts"].([]interface{}); ok && len(accounts) > 0 {
				if account, ok := accounts[0].(map[string]interface{}); ok {
					saID = account["id"].(float64)
					break
				}
			}
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}

	if saID == 0 {
		logger.Warn("Could not find janitor service account, attempting to create via API")

		payloadSA := map[string]interface{}{
			"name": "janitor",
			"role": "Editor",
		}
		bodySA, _ := json.Marshal(payloadSA)
		reqSA, _ := http.NewRequest("POST", grafanaBase+"/api/serviceaccounts", bytes.NewBuffer(bodySA))
		reqSA.SetBasicAuth("admin", "admin")
		reqSA.Header.Set("Content-Type", "application/json")
		respSA, errSA := client.Do(reqSA)
		if errSA == nil && (respSA.StatusCode == 201 || respSA.StatusCode == 200) {
			var resultSA map[string]interface{}
			json.NewDecoder(respSA.Body).Decode(&resultSA)
			if id, ok := resultSA["id"].(float64); ok {
				saID = id
				logger.Info("Successfully created service account via API", zap.Float64("saID", saID))
			}
			respSA.Body.Close()
		} else {
			if respSA != nil {
				b, _ := io.ReadAll(respSA.Body)
				logger.Error("Failed to create service account via API", zap.Int("status", respSA.StatusCode), zap.String("response", string(b)))
				respSA.Body.Close()
			} else {
				logger.Error("Failed to create service account via API", zap.Error(errSA))
			}
			return ""
		}
	}

	// Create token
	payload := map[string]interface{}{
		"name": fmt.Sprintf("token-%d", time.Now().Unix()),
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/serviceaccounts/%.0f/tokens", grafanaBase, saID), bytes.NewBuffer(body))
	req.SetBasicAuth("admin", "admin")
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Failed to create service account token", zap.Error(err))
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		if token, ok := result["key"].(string); ok {
			logger.Info("Successfully obtained Grafana SA token")
			return token
		}
	} else {
		b, _ := io.ReadAll(resp.Body)
		logger.Error("Failed to create token, bad status", zap.Int("status", resp.StatusCode), zap.String("body", string(b)))
	}

	return ""
}

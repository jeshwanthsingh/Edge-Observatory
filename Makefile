.PHONY: help build test lint fmt apply-infra destroy-infra deploy

help:
	@echo "Global Edge Observatory — Available targets:"
	@echo "  build          Build all Go services"
	@echo "  test           Run all tests"
	@echo "  lint           Lint all Go services"
	@echo "  fmt            Format all Go services"
	@echo "  apply-infra    terraform apply in infra/terraform"
	@echo "  destroy-infra  terraform destroy in infra/terraform"
	@echo "  deploy         Apply all k8s manifests"

build:
	go build ./ingestion/... ./consumers/... ./chaos-controller/...

test:
	go test ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

apply-infra:
	cd infra/terraform && terraform init && terraform apply

destroy-infra:
	cd infra/terraform && terraform destroy

deploy:
	kubectl apply -f infra/k8s/kafka/
	kubectl apply -f infra/k8s/ingestion/
	kubectl apply -f infra/k8s/consumers/
	kubectl apply -f infra/k8s/chaos-controller/
	kubectl apply -f infra/k8s/observability/

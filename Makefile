.PHONY: build docker-build deploy-nats deploy-marketfeeder deploy-all clean test-nats install-python-deps

# Build the Go binary
build:
	cd services/marketfeeder && go build -o marketfeeder .

# Build the Docker image
docker-build: build
	cd services/marketfeeder && docker build -t marketfeeder:latest .

# Install Python dependencies
install-python-deps:
	cd bots/solobot && pip3 install -r requirements.txt

# Test NATS communication
test-nats: install-python-deps
	cd bots && python -m solobot.test_nats

# Deploy NATS using Helm
deploy-nats:
	helm repo add nats https://nats-io.github.io/k8s/helm/charts/
	helm repo update
	helm install nats nats/nats --namespace default --create-namespace

# Deploy marketfeeder
deploy-marketfeeder: docker-build
	kubectl apply -f k8s/marketfeeder-deployment.yaml

# Deploy all
deploy-all: deploy-nats deploy-marketfeeder

# Clean up
clean:
	kubectl delete deployment marketfeeder --ignore-not-found=true
	helm uninstall nats --namespace default --ignore-not-found=true
	docker rmi marketfeeder:latest --force
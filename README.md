# Collector Service

Email notification worker service that consumes messages from Kafka and sends emails via SMTP.

## Quick Start

1. Clone the repository and navigate to the project directory
2. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
3. Edit `.env` and set your credentials:
   ```bash
   EMAIL_USERNAME=your-email@gmail.com
   EMAIL_PASSWORD=your-app-password
   EMAIL_SECRET_KEY=your-fernet-key
   ```
4. Run tests:
   ```bash
   make test
   ```
5. Build and run:
   ```bash
   make build
   ./bin/collector
   ```

## Configuration

### Environment Variables

The service can be configured using environment variables. Create a `.env` file in the project root (see `.env.example` for reference).

#### Log Configuration
- `LOG_LEVEL` - Logging level (default: `info`, options: `debug`, `info`, `warn`, `warning`, `error`)

#### Broker Configuration
- `BROKER_BROKERS` - Kafka broker addresses (default: `localhost:29092`)
- `BROKER_TOPIC_EMAIL` - Kafka topic for email events (default: `verify_email_events`)
- `BROKER_CONSUMER_GROUP` - Kafka consumer group ID (default: `email.smtp`)
- `BROKER_TIMEOUT` - Kafka operation timeout (default: `5s`)

#### Email Configuration
- `EMAIL_HOST` - SMTP server host (default: `smtp.gmail.com`)
- `EMAIL_PORT` - SMTP server port (default: `465`)
- `EMAIL_USERNAME` - SMTP username (required)
- `EMAIL_PASSWORD` - SMTP password (required)
- `EMAIL_SECRET_KEY` - Fernet encryption key for access codes (required)

### Local Development Setup

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and fill in the required values:
```bash
EMAIL_USERNAME=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
EMAIL_SECRET_KEY=your-fernet-key
```

3. Run tests:
```bash
make test
```

4. Build the service:
```bash
make build
```

5. Run the service:
```bash
make run
```

## Kubernetes Deployment

### Prerequisites
- Kubernetes cluster running
- `kubectl` configured to access your cluster

### Deploy to Kubernetes

1. Create the namespace (if not exists):
```bash
kubectl create namespace backend
```

2. Update the Secret with your credentials:
```bash
kubectl create secret generic collector-secret \
  --from-literal=EMAIL_USERNAME='your-email@gmail.com' \
  --from-literal=EMAIL_PASSWORD='your-app-password' \
  --from-literal=EMAIL_SECRET_KEY='your-fernet-key' \
  --namespace=backend
```

3. Apply the ConfigMap:
```bash
kubectl apply -f k8s/configmap.yaml
```

4. Apply the Deployment:
```bash
kubectl apply -f k8s/deployment.yaml
```

5. Check the deployment status:
```bash
kubectl get pods -n backend
kubectl logs -f deployment/collector -n backend
```

### Minikube Deployment

For local Minikube deployment:

```bash
make deploy
```

This will:
- Set up Minikube Docker environment
- Build the Docker image
- Apply all Kubernetes manifests

## Architecture

The service uses a worker pool pattern:
- One reader goroutine reads messages from Kafka
- N worker goroutines process messages in parallel
- Graceful shutdown on SIGTERM/SIGINT signals
- Health check endpoint on port 8000 (TCP)

## Message Format

Expected Kafka message format:
```json
{
  "name": "John Doe",
  "email": "user@example.com",
  "code": "123456"
}
```

The service will:
1. Parse the JSON message
2. Encrypt the code using Fernet
3. Send an email with the encrypted token

## Health Check

The service exposes a TCP health check on port 8000. Kubernetes uses this for:
- Readiness probe (checks if service can accept traffic)
- Liveness probe (checks if service is running)

## Makefile Commands

- `make test` - Run tests
- `make test-cover` - Run tests with coverage
- `make vet` - Run go vet
- `make lint` - Run linters
- `make build` - Build binary
- `make run` - Run the service
- `make deploy` - Deploy to Minikube

## Security Notes

- Never commit `.env` file with real credentials
- Use Kubernetes Secrets for production deployments
- Rotate `EMAIL_SECRET_KEY` periodically
- Use app-specific passwords for Gmail (not your main password)

## Troubleshooting

### Service won't start
- Check if all required environment variables are set
- Verify Kafka broker is accessible
- Check SMTP credentials are correct

### Messages not being processed
- Check Kafka topic exists and has messages
- Verify consumer group is not stuck
- Check service logs for errors

### Emails not being sent
- Verify SMTP credentials
- Check if Gmail app password is being used
- Ensure less secure app access is enabled (if applicable)
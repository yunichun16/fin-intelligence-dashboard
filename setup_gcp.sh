#!/bin/bash
# =============================================================================
# setup_gcp.sh — Financial Intelligence Platform on GCP
# Group 7 · Columbia University
#
# Usage:
#   chmod +x setup_gcp.sh
#   ./setup_gcp.sh
#
# Prerequisites:
#   1. Google Cloud SDK installed → https://cloud.google.com/sdk/docs/install
#   2. Run: gcloud auth login
#   3. Have your .env file ready with all credentials
# =============================================================================

set -e  # exit on any error

# ── Config — change these if needed ──────────────────────────────────────────
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION="us-central1"
ZONE="us-central1-a"
VM_NAME="finintel-demo"
MACHINE_TYPE="e2-standard-4"   # 4 vCPU, 16 GB RAM — enough for Kafka+Spark+Airflow
DISK_SIZE="50GB"
REPO_URL="https://github.com/yunichun16/fin-intelligence-dashboard"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════╗"
echo "║     Financial Intelligence Platform — GCP Deploy       ║"
echo "║     Group 7 · Columbia University                      ║"
echo "╚════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ── Step 0: Check prerequisites ───────────────────────────────────────────────
echo -e "${YELLOW}[0/6] Checking prerequisites...${NC}"

if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}✗ gcloud not found. Install from: https://cloud.google.com/sdk/docs/install${NC}"
    exit 1
fi

if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}✗ No GCP project set. Run: gcloud config set project YOUR_PROJECT_ID${NC}"
    exit 1
fi

if [ ! -f ".env" ]; then
    echo -e "${RED}✗ .env file not found in current directory${NC}"
    echo "Create it with your credentials (see FULLSTACK_README.md)"
    exit 1
fi

echo -e "${GREEN}✓ gcloud found | Project: $PROJECT_ID${NC}"

# ── Step 1: Enable required APIs ─────────────────────────────────────────────
echo -e "${YELLOW}[1/6] Enabling GCP APIs...${NC}"
gcloud services enable compute.googleapis.com --quiet
echo -e "${GREEN}✓ Compute Engine API enabled${NC}"

# ── Step 2: Create firewall rules ─────────────────────────────────────────────
echo -e "${YELLOW}[2/6] Creating firewall rules...${NC}"

# Airflow UI (8080)
gcloud compute firewall-rules create finintel-airflow \
    --allow=tcp:8080 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=finintel \
    --description="Airflow webserver" \
    --quiet 2>/dev/null || echo "  (firewall rule already exists)"

# Spark UI (4040)
gcloud compute firewall-rules create finintel-spark \
    --allow=tcp:4040 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=finintel \
    --description="Spark UI" \
    --quiet 2>/dev/null || echo "  (firewall rule already exists)"

echo -e "${GREEN}✓ Firewall rules ready${NC}"

# ── Step 3: Upload .env to GCP Secret Manager ─────────────────────────────────
echo -e "${YELLOW}[3/6] Storing credentials in Secret Manager...${NC}"
gcloud services enable secretmanager.googleapis.com --quiet

# Store each credential as a separate secret
while IFS='=' read -r key value; do
    # Skip comments and empty lines
    [[ "$key" =~ ^#.*$ ]] && continue
    [[ -z "$key" ]] && continue
    value="${value%\"}"
    value="${value#\"}"
    SECRET_NAME="finintel-${key,,}"  # lowercase
    echo "  Storing $key..."
    echo -n "$value" | gcloud secrets create "$SECRET_NAME" --data-file=- --quiet 2>/dev/null || \
    echo -n "$value" | gcloud secrets versions add "$SECRET_NAME" --data-file=- --quiet 2>/dev/null
done < .env

echo -e "${GREEN}✓ Credentials stored in Secret Manager${NC}"

# ── Step 4: Create the VM with startup script ─────────────────────────────────
echo -e "${YELLOW}[4/6] Creating VM: $VM_NAME ($MACHINE_TYPE)...${NC}"
echo "  This takes ~2 minutes..."

# Create startup script that runs on VM boot
STARTUP_SCRIPT='#!/bin/bash
set -e

# ── System setup ──────────────────────────────────────────────────────────────
apt-get update -q
apt-get install -y docker.io docker-compose git curl jq python3-pip -q

# Enable docker without sudo
usermod -aG docker $(whoami)
systemctl start docker
systemctl enable docker

# ── Install gcloud (to read secrets) ─────────────────────────────────────────
if ! command -v gcloud &> /dev/null; then
    curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts
    source /root/google-cloud-sdk/path.bash.inc
fi

# ── Clone the repo ─────────────────────────────────────────────────────────────
cd /opt
git clone '"$REPO_URL"' finintel
cd /opt/finintel

# ── Reconstruct .env from Secret Manager ──────────────────────────────────────
PROJECT=$(gcloud config get-value project)
echo "# Auto-generated from Secret Manager" > .env

SECRETS=(
    "NEWS_API_KEY:finintel-news_api_key"
    "FRED_API_KEY:finintel-fred_api_key"
    "SUPABASE_HOST:finintel-supabase_host"
    "SUPABASE_PORT:finintel-supabase_port"
    "SUPABASE_DB:finintel-supabase_db"
    "SUPABASE_USER:finintel-supabase_user"
    "SUPABASE_PASSWORD:finintel-supabase_password"
    "MONGO_URI:finintel-mongo_uri"
    "ALPACA_API_KEY:finintel-alpaca_api_key"
    "ALPACA_SECRET_KEY:finintel-alpaca_secret_key"
)

for pair in "${SECRETS[@]}"; do
    KEY="${pair%%:*}"
    SECRET="${pair##*:}"
    VALUE=$(gcloud secrets versions access latest --secret="$SECRET" 2>/dev/null || echo "")
    if [ -n "$VALUE" ]; then
        echo "$KEY=$VALUE" >> .env
    fi
done

echo "KAFKA_BOOTSTRAP=kafka:29092" >> .env
echo "✓ .env reconstructed from Secret Manager"

# ── Create required directories ───────────────────────────────────────────────
mkdir -p airflow/logs airflow/dags spark producers

# ── Start the full stack ──────────────────────────────────────────────────────
docker-compose up -d

echo "✓ Stack started. Services:"
docker-compose ps

# ── Wait for Airflow to be ready then run the pipeline ───────────────────────
echo "Waiting for Airflow to be ready (~90 seconds)..."
sleep 90

# Trigger the pipeline DAG
docker exec finintel-airflow-webserver airflow dags trigger finintel_pipeline 2>/dev/null || true

echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║  FinIntel stack is running!                        ║"
EXTERNAL_IP=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor: Google")
echo "║  Airflow UI: http://$EXTERNAL_IP:8080       ║"
echo "║  Login: admin / finintel                           ║"
echo "║  Spark UI:   http://$EXTERNAL_IP:4040       ║"
echo "╚════════════════════════════════════════════════════╝"
'

gcloud compute instances create "$VM_NAME" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --boot-disk-size="$DISK_SIZE" \
    --image-family="ubuntu-2204-lts" \
    --image-project="ubuntu-os-cloud" \
    --tags="finintel,http-server" \
    --metadata="startup-script=$STARTUP_SCRIPT" \
    --scopes="cloud-platform" \
    --quiet

echo -e "${GREEN}✓ VM created: $VM_NAME${NC}"

# ── Step 5: Get external IP ────────────────────────────────────────────────────
echo -e "${YELLOW}[5/6] Getting VM IP address...${NC}"
sleep 5
EXTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" \
    --format="get(networkInterfaces[0].accessConfigs[0].natIP)")

echo -e "${GREEN}✓ External IP: $EXTERNAL_IP${NC}"

# ── Step 6: Done ──────────────────────────────────────────────────────────────
echo -e "${YELLOW}[6/6] Waiting for stack to initialize (~3 minutes)...${NC}"
echo "  The startup script is installing Docker and starting services."
echo "  You can watch progress with:"
echo ""
echo -e "  ${BLUE}gcloud compute ssh $VM_NAME --zone=$ZONE -- 'tail -f /var/log/syslog | grep startup'${NC}"
echo ""

echo -e "${GREEN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  GCP Deployment Complete!                                    ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  Airflow UI  →  http://$EXTERNAL_IP:8080                    ║"
echo "║  Login       →  admin / finintel                            ║"
echo "║  Spark UI    →  http://$EXTERNAL_IP:4040                    ║"
echo "║                                                              ║"
echo "║  SSH into VM:                                                ║"
echo "║  gcloud compute ssh $VM_NAME --zone=$ZONE                   ║"
echo "║                                                              ║"
echo "║  To stop VM (saves money):                                   ║"
echo "║  gcloud compute instances stop $VM_NAME --zone=$ZONE        ║"
echo "║                                                              ║"
echo "║  To DELETE VM after presentation:                            ║"
echo "║  gcloud compute instances delete $VM_NAME --zone=$ZONE      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Save connection details to a file
cat > gcp_connection.txt << EOF
VM Name:     $VM_NAME
Zone:        $ZONE
External IP: $EXTERNAL_IP
Airflow UI:  http://$EXTERNAL_IP:8080  (admin / finintel)
Spark UI:    http://$EXTERNAL_IP:4040

SSH command:
  gcloud compute ssh $VM_NAME --zone=$ZONE

Stop VM:
  gcloud compute instances stop $VM_NAME --zone=$ZONE

Delete VM (after presentation):
  gcloud compute instances delete $VM_NAME --zone=$ZONE
EOF

echo -e "${GREEN}✓ Connection details saved to: gcp_connection.txt${NC}"

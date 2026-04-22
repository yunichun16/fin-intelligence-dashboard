#!/bin/bash
# teardown_gcp.sh — Delete GCP resources after presentation

VM_NAME="finintel-demo"
ZONE="us-central1-a"

echo "This will DELETE the VM and all GCP resources."
read -p "Are you sure? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then echo "Cancelled."; exit 0; fi

gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet && echo "✓ VM deleted"
gcloud compute firewall-rules delete finintel-airflow --quiet 2>/dev/null || true
gcloud compute firewall-rules delete finintel-spark   --quiet 2>/dev/null || true

for secret in finintel-news_api_key finintel-fred_api_key finintel-supabase_host \
    finintel-supabase_port finintel-supabase_db finintel-supabase_user \
    finintel-supabase_password finintel-mongo_uri finintel-alpaca_api_key \
    finintel-alpaca_secret_key; do
    gcloud secrets delete "$secret" --quiet 2>/dev/null || true
done

echo "✓ All GCP resources deleted. No further charges."

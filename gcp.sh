#!/bin/bash

# Define file paths
CA_CERT="ca.cert"
SERVER_CERT="server.cert"
SERVER_KEY="server.key"
OUTPUT_JSON="certs_keys.json"

# GCP configurations
PROJECT_ID="project"
SECRET_NAME="my-secret"
SECRET_VERSION="latest" # Use "latest" for always updating the same secret, or automate versioning as needed

# Function to read and escape file contents
read_and_escape_file() {
    local file_path="$1"
    awk '{printf "%s\\n", $0}' "$file_path"
}

# Read files and escape newlines
CA_CERT_CONTENT=$(read_and_escape_file "$CA_CERT")
SERVER_CERT_CONTENT=$(read_and_escape_file "$SERVER_CERT")
SERVER_KEY_CONTENT=$(read_and_escape_file "$SERVER_KEY")

# Create JSON structure
JSON_CONTENT=$(cat <<EOF
{
    "ca_cert": "$CA_CERT_CONTENT",
    "server_cert": "$SERVER_CERT_CONTENT",
    "server_key": "$SERVER_KEY_CONTENT"
}
EOF
)

# Save to JSON file
echo "$JSON_CONTENT" > "$OUTPUT_JSON"

# Use gcloud to create the secret (if it doesn't exist)
create_or_update_secret() {
    local secret_name="$1"
    local json_file="$2"
    local project_id="$3"
    
    gcloud secrets describe "$secret_name" --project="$project_id" >/dev/null 2>&1 || \
        gcloud secrets create "$secret_name" --data-file="$json_file" --replication-policy="automatic" --project="$project_id"
    
    gcloud secrets versions add "$secret_name" --data-file="$json_file" --project="$project_id"
}

# Create or update the secret
create_or_update_secret "$SECRET_NAME" "$OUTPUT_JSON" "$PROJECT_ID"

echo "JSON content uploaded to GCP Secret Manager under the secret name $SECRET_NAME."

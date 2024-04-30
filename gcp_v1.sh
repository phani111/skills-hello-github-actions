#!/bin/bash

# Define file paths
FILES=("$@")
OUTPUT_JSON="certs_keys.json"

# Check if array of files is passed as command-line arguments
if [ ${#FILES[@]} -eq 0 ]; then
    echo "Please provide an array of file paths as command-line arguments."
    exit 1
fi

# GCP configurations
PROJECT_ID="project"
SECRET_NAME="my-secret"
SECRET_VERSION="latest" # Use "latest" for always updating the same secret, or automate versioning as needed

# Function to read and escape file contents
read_and_escape_file() {
    local file_path="$1"
    awk '{printf "%s\\n", $0}' "$file_path"
}

# Monitor /vault/secrets path and upload to GCP Secret Manager
monitor_and_upload() {
    declare -A file_modified
    for file in "${FILES[@]}"; do
        file_modified["$file"]=0
    done

    inotifywait -m -r -e create,modify "/vault/secrets" |
    while read -r directory events file; do
        if [[ " ${FILES[@]} " =~ " $file " ]]; then
            file_modified["$file"]=1
            all_modified=1
            for file in "${FILES[@]}"; do
                if [ ${file_modified["$file"]} -eq 0 ]; then
                    all_modified=0
                    break
                fi
            done

            if [ $all_modified -eq 1 ]; then
                JSON_CONTENT="{"
                for file in "${FILES[@]}"; do
                    if [ -f "$file" ]; then
                        file_content=$(read_and_escape_file "$file")
                        file_name=$(basename "$file")
                        JSON_CONTENT+="\"$file_name\":\"$file_content\","
                    fi
                done
                JSON_CONTENT="${JSON_CONTENT%,}}" # Remove trailing comma

                # Save to JSON file
                echo "$JSON_CONTENT" > "$OUTPUT_JSON"

                create_or_update_secret "$SECRET_NAME" "/vault/secrets/$OUTPUT_JSON" "$PROJECT_ID"
                echo "JSON content uploaded to GCP Secret Manager under the secret name $SECRET_NAME."

                # Reset file modification status
                for file in "${FILES[@]}"; do
                    file_modified["$file"]=0
                done
            fi
        fi
    done
}

# Use gcloud to create the secret (if it doesn't exist)
create_or_update_secret() {
    local secret_name="$1"
    local json_file="$2"
    local project_id="$3"
    
    gcloud secrets describe "$secret_name" --project="$project_id" >/dev/null 2>&1 || \
        gcloud secrets create "$secret_name" --data-file="$json_file" --replication-policy="automatic" --project="$project_id"
    
    gcloud secrets versions add "$secret_name" --data-file="$json_file" --project="$project_id"
}

# Start monitoring and uploading
monitor_and_upload "${FILES[@]}"

#!/bin/bash

# Set the namespace you want to monitor
NAMESPACE="doi-dbt"

# Loop indefinitely
while true; do
  echo "Checking for pods created in the last 5 minutes in namespace: $NAMESPACE"
  
  # Get all pods in the namespace, outputting their name and creation timestamp
  kubectl get pods -n $NAMESPACE --no-headers -o custom-columns="NAME:.metadata.name,TIME:.metadata.creationTimestamp" | while read NAME TIME; do
    # Convert pod creation timestamp and current time to seconds since epoch
    POD_TIME=$(date --date="$TIME" +%s)
    CURRENT_TIME=$(date +%s)
    
    # Calculate the difference in seconds
    DIFF=$((CURRENT_TIME - POD_TIME))
    
    # If the difference is 300 seconds (5 minutes) or less, print the pod name and get logs
    if [ $DIFF -le 300 ]; then
      echo "Pod created in the last 5 minutes: $NAME"
      echo "Fetching logs for $NAME..."
      
      # Fetch and print the logs of the pod
      # You might want to adjust the log options depending on your needs
      kubectl logs $NAME -n $NAMESPACE
      
      echo "--------------------------------------------------"
    fi
  done
  
  # Wait for a specified interval before the next check
  # Adjust the sleep duration to control how frequently you check for new pods
  echo "Waiting for the next check..."
  sleep 60
done

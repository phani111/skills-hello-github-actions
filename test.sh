#!/bin/bash

# Set the namespace you want to monitor
NAMESPACE="doi-dbt"

# Flag to check if a pod has been found
found=0

# Loop until a pod is found
while [ $found -eq 0 ]; do
  echo "Checking for pods created in the last 5 minutes in namespace: $NAMESPACE"
  
  # Get all pods in the namespace, outputting their name and creation timestamp
  kubectl get pods -n $NAMESPACE --no-headers -o custom-columns="NAME:.metadata.name,TIME:.metadata.creationTimestamp" | while read NAME TIME; do
    # Convert pod creation timestamp and current time to seconds since epoch
    POD_TIME=$(date --date="$TIME" +%s)
    CURRENT_TIME=$(date +%s)
    
    # Calculate the difference in seconds
    DIFF=$((CURRENT_TIME - POD_TIME))
    
    # If the difference is 300 seconds (5 minutes) or less, and no pod has been found yet
    if [ $DIFF -le 300 ] && [ $found -eq 0 ]; then
      echo "Pod created in the last 5 minutes: $NAME"
      echo "Fetching logs for $NAME..."
      
      # Fetch and print the logs of the pod
      kubectl logs $NAME -n $NAMESPACE
      
      # Set found flag to 1 and break the loop
      found=1
      break
    fi
  done
  
  # If a pod has been found, break the outer while loop
  if [ $found -eq 1 ]; then
    echo "Logs emitted. Exiting script."
    break
  fi
  
  # Wait for a specified interval before the next check if no pod is found
  echo "No pod found. Waiting for the next check..."
  sleep 60
done

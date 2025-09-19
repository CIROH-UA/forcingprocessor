#!/bin/bash

# Log file location - ensure directory exists and is writable
LOG_DIR="/home/ec2-user/forcingprocessor"
LOG_FILE="$LOG_DIR/docker_build_log.txt"

# Create log directory and file with proper permissions
mkdir -p "$LOG_DIR"
touch "$LOG_FILE" 2>/dev/null || LOG_FILE="/tmp/docker_build_log.txt"

# Accept tag as command line argument, default to "latest-arm64" if not provided
TAG="${1:-latest-arm64}"
BUILD_ARGS="${2:-}"

echo "Script called with TAG: $TAG, BUILD_ARGS: $BUILD_ARGS" | tee -a "$LOG_FILE"

DOCKERHUB_TOKEN="$(aws secretsmanager get-secret-value --secret-id docker_awiciroh_creds --region us-east-1 --query SecretString --output text | jq -r .DOCKERHUB_TOKEN)"
DOCKERHUB_USERNAME="awiciroh"

# Parse BUILD_ARGS to determine what to push
PUSH_DEPS="no"
PUSH_FP="no"
PUSH_DS="no"

if [ -n "$BUILD_ARGS" ]; then
    # Clean up BUILD_ARGS (remove quotes if present)
    BUILD_ARGS_CLEAN=$(echo "$BUILD_ARGS" | sed 's/^"//;s/"$//')
    # If sed removed everything, use original BUILD_ARGS
    [ -z "$BUILD_ARGS_CLEAN" ] && BUILD_ARGS_CLEAN="$BUILD_ARGS"
    echo "Processing build arguments: $BUILD_ARGS_CLEAN" | tee -a "$LOG_FILE"
    
    # Check what services to push based on build flags
    if [[ "$BUILD_ARGS_CLEAN" == *"-e"* ]]; then
        PUSH_DEPS="yes"
        echo "Will push forcingprocessor-deps" | tee -a "$LOG_FILE"
    fi
    
    if [[ "$BUILD_ARGS_CLEAN" == *"-f"* ]]; then
        PUSH_FP="yes"
        echo "Will push forcingprocessor" | tee -a "$LOG_FILE"
    fi
else
    echo "No BUILD_ARGS provided - nothing to push" | tee -a "$LOG_FILE"
    exit 0
fi

if echo "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin; then             
    echo "Docker login successful" | tee -a "$LOG_FILE"
    echo "Retagging and pushing images with tag: $TAG" | tee -a "$LOG_FILE"
    
    # Only retag and push services that were built (based on BUILD_ARGS)
    PUSH_SUCCESS=true
    PUSHED_ANYTHING=false
    
    if [ "$PUSH_DEPS" = "yes" ]; then
        echo "Retagging and pushing forcingprocessor-deps" | tee -a "$LOG_FILE"
        if docker tag awiciroh/forcingprocessor-deps:latest-arm64 awiciroh/forcingprocessor-deps:$TAG 2>&1 | tee -a "$LOG_FILE" && \
           docker push awiciroh/forcingprocessor-deps:$TAG 2>&1 | tee -a "$LOG_FILE" && \
           docker push awiciroh/forcingprocessor-deps:latest-arm64 2>&1 | tee -a "$LOG_FILE"; then
            echo "✓ Successfully pushed forcingprocessor-deps:$TAG" | tee -a "$LOG_FILE"
            PUSHED_ANYTHING=true
        else
            echo "✗ Failed to push forcingprocessor-deps:$TAG" | tee -a "$LOG_FILE"
            PUSH_SUCCESS=false
        fi
    fi
    
    if [ "$PUSH_FP" = "yes" ]; then
        echo "Retagging and pushing forcingprocessor" | tee -a "$LOG_FILE"
        if docker tag awiciroh/forcingprocessor:latest-arm64 awiciroh/forcingprocessor:$TAG 2>&1 | tee -a "$LOG_FILE" && \
           docker push awiciroh/forcingprocessor:$TAG 2>&1 | tee -a "$LOG_FILE" && \
           docker push awiciroh/forcingprocessor:latest-arm64 2>&1 | tee -a "$LOG_FILE"; then
            echo "✓ Successfully pushed forcingprocessor:$TAG" | tee -a "$LOG_FILE"
            PUSHED_ANYTHING=true
        else
            echo "✗ Failed to push forcingprocessor:$TAG" | tee -a "$LOG_FILE"
            PUSH_SUCCESS=false
        fi
    fi
    
    if [ "$PUSHED_ANYTHING" = true ]; then
        if [ "$PUSH_SUCCESS" = true ]; then
            echo "✓ All pushes completed successfully" | tee -a "$LOG_FILE"
        else
            echo "✗ Some pushes failed - check logs above" | tee -a "$LOG_FILE"
            exit 1
        fi
    else
        echo "No images to push based on build arguments" | tee -a "$LOG_FILE"
    fi
    
else
    echo "Docker login failed" | tee -a "$LOG_FILE"
    exit 1
fi
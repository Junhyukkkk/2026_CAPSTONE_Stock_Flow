#!/bin/bash

# Medi-Link EC2 Deployment Script

APP_NAME="medilink"
JAR_FILE="build/libs/PracticeVibeCoding-0.0.1-SNAPSHOT.jar"

echo "=== Medi-Link Deployment Script ==="

# 1. Build the application
echo "Building application..."
./gradlew clean build -x test

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"

# 2. Stop existing application (if running)
echo "Stopping existing application..."
pkill -f "$APP_NAME" 2>/dev/null || true
sleep 2

# 3. Start the application with production profile
echo "Starting application with production profile..."
nohup java -jar "$JAR_FILE" \
    --spring.profiles.active=prod \
    -Dspring.profiles.active=prod \
    > app.log 2>&1 &

echo "Application started! PID: $!"
echo "Check logs: tail -f app.log"

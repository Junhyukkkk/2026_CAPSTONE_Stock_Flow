# Multi-stage build for Spring Boot application
FROM gradle:8.5-jdk17 AS build

WORKDIR /app

# Copy Gradle files
COPY build.gradle settings.gradle ./
COPY gradle ./gradle

# Copy source code
COPY stockflow-core ./stockflow-core
COPY stockflow-realtime ./stockflow-realtime

# Build the application
RUN gradle :stockflow-realtime:build -x test --no-daemon

# Runtime stage
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the built JAR
COPY --from=build /app/stockflow-realtime/build/libs/*.jar app.jar

# Expose the application port
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8081/actuator/health || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]

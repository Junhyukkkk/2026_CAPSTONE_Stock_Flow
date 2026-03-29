# Multi-stage build for Spring Boot application
FROM gradle:8.5-jdk17 AS build

WORKDIR /app

# Copy Gradle files
COPY build.gradle settings.gradle ./
COPY gradle ./gradle

# Copy source code
COPY stockflow-core ./stockflow-core
COPY stockflow-realtime ./stockflow-realtime

# 컨텍스트가 잘못 잘리면 여기서 실패 (Flyway V2~ 없이 빌드되는 것 방지)
RUN ls -la stockflow-realtime/src/main/resources/db/migration/ && \
    test -f stockflow-realtime/src/main/resources/db/migration/V2__market_ticks_hypertable.sql

# Build the application
RUN gradle :stockflow-realtime:build -x test --no-daemon

# 데모용 정적 페이지가 실행 JAR(plain 제외)에 포함됐는지 확인
RUN JAR=$(ls stockflow-realtime/build/libs/*.jar | grep -v plain | head -1) && \
    unzip -l "$JAR" | grep -F 'storage-overview.html'

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

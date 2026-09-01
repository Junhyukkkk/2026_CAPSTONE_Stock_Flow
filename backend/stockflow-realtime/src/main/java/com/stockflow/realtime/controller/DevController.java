package com.stockflow.realtime.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 시연/진단용 엔드포인트.
 *
 * Sentry(GlitchTip)·Loki 로그 수집 연동이 실제로 동작하는지 눈으로 확인하기 위한 것이다.
 * stockflow.dev.enabled=false 로 끌 수 있다(기본 활성).
 */
@Slf4j
@RestController
@RequestMapping("/api/dev")
@ConditionalOnProperty(prefix = "stockflow.dev", name = "enabled", havingValue = "true", matchIfMissing = true)
@Tag(name = "Dev", description = "시연/진단용 엔드포인트 (Sentry·로그 수집 확인)")
public class DevController {

    @Operation(summary = "ERROR 로그 발생", description = "예외를 첨부한 ERROR 로그를 남긴다. Sentry 이벤트 + Loki 로그로 확인 가능.")
    @PostMapping("/log-error")
    public ResponseEntity<Map<String, Object>> logError(
            @RequestParam(defaultValue = "manual test error from /api/dev/log-error") String message) {
        log.error("[DEV] 수동 ERROR 로그: {}", message, new IllegalStateException(message));
        return ResponseEntity.ok(Map.of(
                "ok", true,
                "level", "ERROR",
                "message", message,
                "hint", "GlitchTip(http://localhost:8000) 및 Grafana > Explore > Loki 에서 확인"
        ));
    }

    @Operation(summary = "처리되지 않은 예외 발생", description = "500 응답과 함께 예외를 던진다. Sentry 가 미처리 예외를 잡는지 확인용.")
    @PostMapping("/raise-error")
    public ResponseEntity<Void> raiseError(
            @RequestParam(defaultValue = "manual test exception from /api/dev/raise-error") String message) {
        log.warn("[DEV] raise-error 호출됨: {}", message);
        throw new IllegalStateException("[DEV] " + message);
    }

    @Operation(summary = "WARN 로그 발생", description = "breadcrumb 확인용 WARN 로그.")
    @PostMapping("/log-warn")
    public ResponseEntity<Map<String, Object>> logWarn(
            @RequestParam(defaultValue = "manual test warning") String message) {
        log.warn("[DEV] 수동 WARN 로그: {}", message);
        return ResponseEntity.ok(Map.of("ok", true, "level", "WARN", "message", message));
    }
}

package com.Practice.PracticeVibeCoding.controller;

import com.Practice.PracticeVibeCoding.alpaca.AlpacaWebSocketClient;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/alpaca")
@RequiredArgsConstructor
@Tag(name = "Alpaca", description = "Alpaca WebSocket 테스트 API")
public class AlpacaTestController {

    private final AlpacaWebSocketClient alpacaWebSocketClient;

    @PostMapping("/connect")
    @Operation(summary = "WebSocket 연결", description = "Alpaca WebSocket 서버에 연결합니다")
    public ResponseEntity<Map<String, String>> connect() {
        if (alpacaWebSocketClient.isConnected()) {
            return ResponseEntity.ok(Map.of(
                    "status", "already_connected",
                    "message", "이미 연결되어 있습니다"
            ));
        }

        // 비동기로 연결 시작
        new Thread(() -> alpacaWebSocketClient.connect()).start();

        return ResponseEntity.ok(Map.of(
                "status", "connecting",
                "message", "연결을 시작합니다. 콘솔 로그를 확인하세요."
        ));
    }

    @PostMapping("/disconnect")
    @Operation(summary = "WebSocket 연결 해제", description = "Alpaca WebSocket 연결을 종료합니다")
    public ResponseEntity<Map<String, String>> disconnect() {
        if (!alpacaWebSocketClient.isConnected()) {
            return ResponseEntity.ok(Map.of(
                    "status", "not_connected",
                    "message", "연결되어 있지 않습니다"
            ));
        }

        alpacaWebSocketClient.disconnect();
        return ResponseEntity.ok(Map.of(
                "status", "disconnected",
                "message", "연결이 종료되었습니다"
        ));
    }

    @GetMapping("/status")
    @Operation(summary = "연결 상태 확인", description = "현재 WebSocket 연결 상태를 확인합니다")
    public ResponseEntity<Map<String, Object>> getStatus() {
        boolean connected = alpacaWebSocketClient.isConnected();
        return ResponseEntity.ok(Map.of(
                "connected", connected,
                "message", connected ? "연결됨" : "연결되지 않음"
        ));
    }
}

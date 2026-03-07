package com.Practice.PracticeVibeCoding.controller;

import com.Practice.PracticeVibeCoding.alpaca.AlpacaAllStocksClient;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/alpaca/all-stocks")
@RequiredArgsConstructor
@Tag(name = "Alpaca All Stocks", description = "IEX 전체 종목 WebSocket API")
public class AlpacaAllStocksController {

    private final AlpacaAllStocksClient alpacaAllStocksClient;

    @PostMapping("/connect")
    @Operation(summary = "WebSocket 연결", description = "IEX 거래소의 모든 종목 데이터를 구독합니다")
    public ResponseEntity<Map<String, String>> connect() {
        if (alpacaAllStocksClient.isConnected()) {
            return ResponseEntity.ok(Map.of(
                    "status", "already_connected",
                    "message", "이미 연결되어 있습니다"
            ));
        }

        new Thread(() -> alpacaAllStocksClient.connect()).start();

        return ResponseEntity.ok(Map.of(
                "status", "connecting",
                "message", "전체 종목 구독을 시작합니다. 콘솔 로그를 확인하세요."
        ));
    }

    @PostMapping("/disconnect")
    @Operation(summary = "WebSocket 연결 해제", description = "전체 종목 구독을 종료합니다")
    public ResponseEntity<Map<String, String>> disconnect() {
        if (!alpacaAllStocksClient.isConnected()) {
            return ResponseEntity.ok(Map.of(
                    "status", "not_connected",
                    "message", "연결되어 있지 않습니다"
            ));
        }

        alpacaAllStocksClient.disconnect();
        return ResponseEntity.ok(Map.of(
                "status", "disconnected",
                "message", "연결이 종료되었습니다"
        ));
    }

    @GetMapping("/status")
    @Operation(summary = "연결 상태 및 통계", description = "전체 종목 구독 상태와 수신 데이터 통계를 확인합니다")
    public ResponseEntity<Map<String, Object>> getStatus() {
        boolean connected = alpacaAllStocksClient.isConnected();
        return ResponseEntity.ok(Map.of(
                "connected", connected,
                "message", connected ? "연결됨" : "연결되지 않음",
                "tradeCount", alpacaAllStocksClient.getTradeCount(),
                "quoteCount", alpacaAllStocksClient.getQuoteCount()
        ));
    }
}

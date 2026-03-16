package com.stockflow.realtime.controller;

import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.service.RedisPriceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 가격 조회 API
 *
 * Redis 캐시에서 최신 가격 데이터를 조회한다.
 * DB를 조회하지 않으므로 < 1ms 응답 가능.
 */
@RestController
@RequestMapping("/api/price")
@RequiredArgsConstructor
public class PriceController {

    private final RedisPriceService redisPriceService;

    /**
     * 종목별 최신 가격 조회
     *
     * @param symbol 종목 심볼 (예: AAPL, BTCUSDT)
     * @return 최신 가격 스냅샷 (가격, 등락률 포함)
     */
    @GetMapping("/{symbol}")
    public ResponseEntity<PriceSnapshot> getLatestPrice(@PathVariable String symbol) {
        PriceSnapshot snapshot = redisPriceService.getLatestPrice(symbol.toUpperCase());

        if (snapshot == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(snapshot);
    }
}

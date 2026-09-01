package com.stockflow.realtime.controller;

import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.service.RedisPriceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

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
     * 실시간 수신 중인 전체 종목의 최신 스냅샷 조회
     *
     * Redis 에 price:latest:* 캐시가 있는 종목 목록을 반환한다.
     * 실시간 시세 화면이 초기 워치리스트를 자동으로 구성하는 데 사용한다.
     */
    @GetMapping("/active")
    public List<PriceSnapshot> getActivePrices() {
        return redisPriceService.getActivePrices();
    }

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

    /**
     * 전일 종가 조회
     *
     * @param symbol 종목 심볼
     * @return 전일 종가
     */
    @GetMapping("/{symbol}/prev-close")
    public ResponseEntity<Map<String, Object>> getPreviousClose(@PathVariable String symbol) {
        BigDecimal prevClose = redisPriceService.getPreviousClose(symbol.toUpperCase());

        if (prevClose == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(Map.of(
                "symbol", symbol.toUpperCase(),
                "previousClose", prevClose
        ));
    }

    /**
     * 전일 종가 설정
     *
     * @param symbol 종목 심볼
     * @param request 전일 종가 (price 필드)
     */
    @PutMapping("/{symbol}/prev-close")
    public ResponseEntity<Map<String, Object>> setPreviousClose(
            @PathVariable String symbol,
            @RequestBody Map<String, BigDecimal> request) {

        BigDecimal price = request.get("price");
        if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "price must be a positive number"
            ));
        }

        redisPriceService.setPreviousClose(symbol.toUpperCase(), price);

        return ResponseEntity.ok(Map.of(
                "symbol", symbol.toUpperCase(),
                "previousClose", price,
                "message", "Previous close price set successfully"
        ));
    }
}

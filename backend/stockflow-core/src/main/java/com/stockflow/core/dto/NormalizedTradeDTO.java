package com.stockflow.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NormalizedTradeDTO {

    @NotBlank
    private String source;       // "BINANCE" | "ALPACA"

    @NotBlank
    private String symbol;       // "AAPL", "BTCUSDT"

    @NotNull
    private BigDecimal price;    // 체결 가격 (BigDecimal 필수 - 금융 데이터 부동소수점 오차 방지)

    @NotNull
    private BigDecimal volume;   // 체결 수량

    @NotBlank
    private String exchange;     // 거래소 (BINANCE, IEX, NYSE 등)

    @NotNull
    private Long timestamp;      // 체결 시간 (UTC epoch ms)

    @NotNull
    private Long receivedAt;     // Producer 수신 시각 (레이턴시 측정용)

    @NotBlank
    private String marketType;   // "CRYPTO" | "STOCK"
}

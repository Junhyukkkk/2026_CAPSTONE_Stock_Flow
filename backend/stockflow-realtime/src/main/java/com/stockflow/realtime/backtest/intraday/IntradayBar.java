package com.stockflow.realtime.backtest.intraday;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 분봉 백테스트 입력 OHLCV 한 개.
 *
 * <p>일봉의 {@code LocalDate}와 섞이지 않도록 실제 봉 시작 시각을 {@link Instant}로 보관한다.
 */
public record IntradayBar(
        Instant time,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume
) {
}

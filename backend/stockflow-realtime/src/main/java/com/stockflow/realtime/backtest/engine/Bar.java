package com.stockflow.realtime.backtest.engine;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 백테스트 입력 일봉(OHLCV) 한 개.
 */
public record Bar(
        LocalDate date,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume
) {
}

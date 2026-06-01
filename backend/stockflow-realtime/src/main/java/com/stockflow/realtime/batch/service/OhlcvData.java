package com.stockflow.realtime.batch.service;

import java.math.BigDecimal;

/**
 * OHLCV 데이터를 담는 레코드.
 * 볼린저 밴드, 스토캐스틱, ATR, OBV 계산에 필요.
 */
public record OhlcvData(
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume
) {}

package com.stockflow.realtime.storage.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * 데모/점검용: Timescale {@code market_ticks} 및 1분 연속 집계 샘플.
 */
public record StorageOverviewResponse(
        long approximateTickRows,
        boolean oneMinuteAggregateAvailable,
        List<MarketTickPreviewRow> recentTicks,
        List<MarketTickOneMinuteRow> recentOneMinuteBars
) {

    public record MarketTickPreviewRow(
            long id,
            String source,
            String symbol,
            String tradeId,
            BigDecimal price,
            BigDecimal volume,
            Instant ts,
            Instant ingestedAt
    ) {
    }

    public record MarketTickOneMinuteRow(
            Instant bucket,
            String symbol,
            String source,
            BigDecimal open,
            BigDecimal high,
            BigDecimal low,
            BigDecimal close,
            BigDecimal volume
    ) {
    }
}

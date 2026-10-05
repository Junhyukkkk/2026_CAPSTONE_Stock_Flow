package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessorInterface;
import com.stockflow.realtime.service.RedisPriceService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RealtimeBatchConsumerTest {

    private final RetryableProcessorInterface retryableProcessor = mock(RetryableProcessorInterface.class);
    private final PerformanceMetrics performanceMetrics = mock(PerformanceMetrics.class);
    private final RedisPriceService redisPriceService = mock(RedisPriceService.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final RealtimeBatchConsumer consumer =
        new RealtimeBatchConsumer(retryableProcessor, performanceMetrics, redisPriceService);

    private static ConsumerRecord<String, NormalizedTradeDTO> record(String symbol, long ts, long offset) {
        NormalizedTradeDTO trade = NormalizedTradeDTO.builder()
            .symbol(symbol).price(new BigDecimal("100")).timestamp(ts).build();
        return new ConsumerRecord<>("market.normalized", 3, offset, symbol, trade);
    }

    @Test
    void success_processesBatchOnceAcksOnceAndCountsEveryRecord() {
        List<ConsumerRecord<String, NormalizedTradeDTO>> records =
            List.of(record("BTC", 1, 0), record("BTC", 2, 1), record("ETH", 1, 2));

        consumer.consumeRealtimeBatch(records, acknowledgment);

        verify(redisPriceService, times(1)).processRealtimeTradeBatch(any());
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(3)).recordSuccessWithLatency(anyLong());
        verify(performanceMetrics, never()).recordFailure();
        verify(retryableProcessor, never()).processWithRetry(any(), any(), any(), anyInt(), anyLong());
    }

    @Test
    void batchFailure_fallsBackToPerRecordPathAndAcksWhenAllSucceed() {
        doThrow(new RuntimeException("redis down")).when(redisPriceService).processRealtimeTradeBatch(any());
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), anyLong())).thenReturn(true);

        consumer.consumeRealtimeBatch(List.of(record("BTC", 1, 10), record("BTC", 2, 11)), acknowledgment);

        verify(retryableProcessor, times(2)).processWithRetry(any(), any(), any(), eq(3), anyLong());
        verify(retryableProcessor).processWithRetry(any(), any(), any(), eq(3), eq(10L));
        verify(retryableProcessor).processWithRetry(any(), any(), any(), eq(3), eq(11L));
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(2)).recordSuccessWithLatency(anyLong());
    }

    @Test
    void fallbackRecordFailure_recordsFailureAndDoesNotAck() {
        doThrow(new RuntimeException("redis down")).when(redisPriceService).processRealtimeTradeBatch(any());
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), eq(10L))).thenReturn(true);
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), eq(11L))).thenReturn(false);

        consumer.consumeRealtimeBatch(List.of(record("BTC", 1, 10), record("ETH", 2, 11)), acknowledgment);

        verify(performanceMetrics, times(1)).recordSuccessWithLatency(anyLong());
        verify(performanceMetrics, times(1)).recordFailure();
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void fallbackUsesSingleRecordProcessorBoundToRedisPriceService() {
        doThrow(new RuntimeException("boom")).when(redisPriceService).processRealtimeTradeBatch(any());
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), anyLong()))
            .thenAnswer(inv -> {
                Consumer<NormalizedTradeDTO> processor = inv.getArgument(1);
                processor.accept(inv.getArgument(0));
                return true;
            });
        ConsumerRecord<String, NormalizedTradeDTO> r = record("BTC", 1, 0);

        consumer.consumeRealtimeBatch(List.of(r), acknowledgment);

        verify(redisPriceService).processRealtimeTrade(r.value());
    }

    @Test
    void emptyBatch_acksWithoutProcessing() {
        consumer.consumeRealtimeBatch(List.of(), acknowledgment);

        verify(acknowledgment, times(1)).acknowledge();
        verify(redisPriceService, never()).processRealtimeTradeBatch(any());
    }

    // --- 플래그에 따른 빈 등록: realtime-group 을 소비하는 리스너는 항상 하나 이하 ---

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withBean(RetryableProcessorInterface.class, () -> retryableProcessor)
        .withBean(PerformanceMetrics.class, () -> performanceMetrics)
        .withBean(RedisPriceService.class, () -> redisPriceService)
        .withUserConfiguration(RealtimeConsumer.class, RealtimeBatchConsumer.class);

    @Test
    void flagDefault_onlySingleRecordListenerIsRegistered() {
        runner.run(ctx -> {
            assertThat(ctx).hasSingleBean(RealtimeConsumer.class);
            assertThat(ctx).doesNotHaveBean(RealtimeBatchConsumer.class);
        });
    }

    @Test
    void flagFalse_onlySingleRecordListenerIsRegistered() {
        runner.withPropertyValues("stockflow.opt.realtime-batch=false").run(ctx -> {
            assertThat(ctx).hasSingleBean(RealtimeConsumer.class);
            assertThat(ctx).doesNotHaveBean(RealtimeBatchConsumer.class);
        });
    }

    @Test
    void flagTrue_onlyBatchListenerIsRegistered() {
        runner.withPropertyValues("stockflow.opt.realtime-batch=true").run(ctx -> {
            assertThat(ctx).hasSingleBean(RealtimeBatchConsumer.class);
            assertThat(ctx).doesNotHaveBean(RealtimeConsumer.class);
        });
    }

    @Test
    void realtimeConsumerDisabled_registersNeitherListener() {
        runner.withPropertyValues("realtime.consumer.enabled=false").run(ctx -> {
            assertThat(ctx).doesNotHaveBean(RealtimeConsumer.class);
            assertThat(ctx).doesNotHaveBean(RealtimeBatchConsumer.class);
        });
        runner.withPropertyValues("realtime.consumer.enabled=false", "stockflow.opt.realtime-batch=true").run(ctx -> {
            assertThat(ctx).doesNotHaveBean(RealtimeConsumer.class);
            assertThat(ctx).doesNotHaveBean(RealtimeBatchConsumer.class);
        });
    }
}

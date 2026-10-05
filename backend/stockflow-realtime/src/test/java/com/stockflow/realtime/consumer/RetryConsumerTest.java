package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.dlq.DLQService;
import com.stockflow.realtime.retry.RetryTopicService;
import com.stockflow.realtime.service.RedisPriceService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RetryConsumerTest {

    @Mock RedisPriceService redisPriceService;
    @Mock DLQService dlqService;
    @Mock RetryTopicService retryTopicService;
    @Mock Acknowledgment ack;

    PerformanceMetrics metrics;
    RetryConsumer consumer;

    @BeforeEach
    void setUp() {
        metrics = new PerformanceMetrics(new SimpleMeterRegistry());
        consumer = new RetryConsumer(redisPriceService, dlqService, retryTopicService, metrics);
        ReflectionTestUtils.setField(consumer, "originalTopic", "market.normalized");
        ReflectionTestUtils.setField(consumer, "retryDelayMs", 0L);
        ReflectionTestUtils.setField(consumer, "maxRetries", 3);
    }

    private static NormalizedTradeDTO trade() {
        return NormalizedTradeDTO.builder().symbol("BTCUSDT").price(BigDecimal.TEN)
                .timestamp(System.currentTimeMillis()).build();
    }

    private static Map<String, Object> headers(Object retryCount) {
        Map<String, Object> headers = new HashMap<>();
        if (retryCount != null) {
            headers.put(RetryTopicService.HEADER_RETRY_COUNT, retryCount);
        }
        return headers;
    }

    @Test
    void successfulRetryAcksAndRecordsMetrics() {
        consumer.consumeRetryMessage(trade(), ack, headers("1".getBytes(StandardCharsets.UTF_8)), 0, 10L);

        verify(redisPriceService).processRealtimeTrade(any());
        verify(ack).acknowledge();
        assertThat(metrics.getTotalProcessed().sum()).isEqualTo(1);
        assertThat(metrics.getProcessingCount().get()).isEqualTo(1);
    }

    @Test
    void failedRetryRequeuesWithIncrementedCount() {
        doThrow(new IllegalStateException("still down")).when(redisPriceService).processRealtimeTrade(any());

        consumer.consumeRetryMessage(trade(), ack, headers("2".getBytes(StandardCharsets.UTF_8)), 0, 10L);

        verify(retryTopicService).sendToRetryTopic(any(), eq(3), any(IllegalStateException.class));
        verify(ack).acknowledge();
        verify(dlqService, never()).sendToDLQ(anyString(), anyInt(), anyLong(), any(), any(), anyString(), anyInt());
    }

    @Test
    void exhaustedRetriesGoToDlq() {
        consumer.consumeRetryMessage(trade(), ack, headers("3".getBytes(StandardCharsets.UTF_8)), 1, 99L);

        verify(dlqService).sendToDLQ(eq("market.normalized"), eq(1), eq(99L), any(), any(RuntimeException.class),
                eq("retry-test-group"), eq(3));
        verify(ack).acknowledge();
        verify(redisPriceService, never()).processRealtimeTrade(any());
        assertThat(metrics.getTotalFailed().sum()).isEqualTo(1);
    }

    @Test
    void missingOrMalformedHeaderCountsAsFirstRetry() {
        consumer.consumeRetryMessage(trade(), ack, headers(null), 0, 1L);
        consumer.consumeRetryMessage(trade(), ack, headers("abc".getBytes(StandardCharsets.UTF_8)), 0, 2L);
        consumer.consumeRetryMessage(trade(), ack, headers("not-bytes"), 0, 3L);

        verify(redisPriceService, org.mockito.Mockito.times(3)).processRealtimeTrade(any());
    }

    @Test
    void interruptedDelayStillProcessesTheMessage() {
        ReflectionTestUtils.setField(consumer, "retryDelayMs", 10_000L);
        Thread.currentThread().interrupt();

        consumer.consumeRetryMessage(trade(), ack, headers(null), 0, 1L);

        assertThat(Thread.interrupted()).isTrue(); // 인터럽트 상태가 복원되었는지 확인하고 해제
        verify(redisPriceService).processRealtimeTrade(any());
    }
}

package com.stockflow.realtime.error;

import com.stockflow.core.dto.DLQMessage;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.realtime.dlq.DLQService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DLQService 테스트
 */
@ExtendWith(MockitoExtension.class)
class DLQServiceTest {

    @Mock
    private KafkaTemplate<String, DLQMessage> kafkaTemplate;

    @Mock
    private ErrorClassifier errorClassifier;

    @InjectMocks
    private DLQService dlqService;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(dlqService, "dlqTopic", "market.dlq");

        testTrade = NormalizedTradeDTO.builder()
            .source("BINANCE")
            .symbol("BTCUSDT")
            .price(new BigDecimal("50000.00"))
            .volume(new BigDecimal("0.1"))
            .exchange("BINANCE")
            .timestamp(1_700_000_000_000L)
            .receivedAt(1_700_000_000_000L)
            .tradeId("test-trade-id-1")
            .marketType("CRYPTO")
            .build();

        when(errorClassifier.classify(any())).thenReturn(ErrorType.PROCESSING_ERROR);
        when(kafkaTemplate.send(anyString(), any(DLQMessage.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void testSendToDLQ_Success() {
        // Given
        String topic = "market.normalized";
        int partition = 0;
        long offset = 100L;
        String consumerGroup = "test-group";
        int retryCount = 3;
        Exception exception = new RuntimeException("Test error");

        // When
        dlqService.sendToDLQ(topic, partition, offset, testTrade, exception, consumerGroup, retryCount);

        // Then
        ArgumentCaptor<DLQMessage> captor = ArgumentCaptor.forClass(DLQMessage.class);
        verify(kafkaTemplate, times(1)).send(eq("market.dlq"), captor.capture());

        DLQMessage sent = captor.getValue();
        assertNotNull(sent);
        assertEquals(testTrade, sent.getOriginalMessage());
        assertEquals(exception.getMessage(), sent.getErrorMessage());
        assertEquals(consumerGroup, sent.getConsumerGroup());
        assertEquals(retryCount, sent.getRetryCount());
        assertEquals(ErrorType.PROCESSING_ERROR.name(), sent.getErrorType());
    }

    @Test
    void testSendBatchToDLQ_Success() {
        // Given
        String topic = "market.normalized";
        String consumerGroup = "test-group";
        Exception exception = new RuntimeException("Test error");

        List<NormalizedTradeDTO> trades = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            trades.add(testTrade);
        }

        // When
        dlqService.sendBatchToDLQ(topic, trades, exception, consumerGroup);

        // Then
        verify(kafkaTemplate, times(5)).send(eq("market.dlq"), any(DLQMessage.class));
    }
}

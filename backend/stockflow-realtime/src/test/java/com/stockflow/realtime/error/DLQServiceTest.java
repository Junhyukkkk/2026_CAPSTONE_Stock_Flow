package com.stockflow.realtime.error;

import com.stockflow.core.dto.DLQMessage;
import com.stockflow.core.dto.NormalizedTradeDTO;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * DLQService 테스트
 */
@ExtendWith(MockitoExtension.class)
class DLQServiceTest {

    @Mock
    private KafkaTemplate<String, DLQMessage> dlqKafkaTemplate;

    @InjectMocks
    private DLQService dlqService;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        testTrade = NormalizedTradeDTO.builder()
            .symbol("BTCUSDT")
            .price(new BigDecimal("50000.00"))
            .quantity(new BigDecimal("0.1"))
            .timestamp(Instant.now())
            .source("binance")
            .tradeId("test-trade-id-1")
            .build();
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

        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        // When
        dlqService.sendToDLQ(topic, partition, offset, testTrade, exception, consumerGroup, retryCount);

        // Then
        ArgumentCaptor<ProducerRecord<String, DLQMessage>> captor = 
            ArgumentCaptor.forClass(ProducerRecord.class);
        verify(dlqKafkaTemplate, times(1)).send(captor.capture());

        ProducerRecord<String, DLQMessage> record = captor.getValue();
        assertEquals("market.dlq", record.topic());
        assertEquals(testTrade.getSymbol(), record.key());
        assertNotNull(record.value());
        assertEquals(testTrade, record.value().getOriginalMessage());
        assertEquals(exception.getMessage(), record.value().getErrorMessage());
        assertEquals(consumerGroup, record.value().getConsumerGroup());
        assertEquals(retryCount, record.value().getRetryCount());
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

        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        // When
        dlqService.sendBatchToDLQ(topic, trades, exception, consumerGroup);

        // Then
        verify(dlqKafkaTemplate, times(5)).send(any(ProducerRecord.class));
    }
}

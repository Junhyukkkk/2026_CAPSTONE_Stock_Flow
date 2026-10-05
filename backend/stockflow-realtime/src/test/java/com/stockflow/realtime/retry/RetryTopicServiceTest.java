package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class RetryTopicServiceTest {

    KafkaTemplate<String, NormalizedTradeDTO> kafkaTemplate;
    RetryTopicService service;

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        service = new RetryTopicService(kafkaTemplate);
        ReflectionTestUtils.setField(service, "retryTopic", "market.retry");
        ReflectionTestUtils.setField(service, "originalTopic", "market.normalized");
    }

    private static NormalizedTradeDTO trade(String symbol) {
        return NormalizedTradeDTO.builder().symbol(symbol).price(BigDecimal.ONE).build();
    }

    private static String header(ProducerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void sendsRecordWithRetryMetadataHeaders() {
        SendResult<String, NormalizedTradeDTO> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(new RecordMetadata(new TopicPartition("market.retry", 1), 0, 5, 0L, 0, 0));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(result));

        service.sendToRetryTopic(trade("BTC"), 2, new RuntimeException("redis down"));

        ArgumentCaptor<ProducerRecord<String, NormalizedTradeDTO>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, NormalizedTradeDTO> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("market.retry");
        assertThat(record.key()).isEqualTo("BTC");
        assertThat(header(record, RetryTopicService.HEADER_RETRY_COUNT)).isEqualTo("2");
        assertThat(header(record, RetryTopicService.HEADER_ORIGINAL_TOPIC)).isEqualTo("market.normalized");
        assertThat(header(record, RetryTopicService.HEADER_ERROR_MESSAGE)).isEqualTo("redis down");
        assertThat(record.headers().lastHeader(RetryTopicService.HEADER_RETRY_TIMESTAMP)).isNotNull();
    }

    @Test
    void omitsErrorHeaderWhenNoError() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("send failed")));

        service.sendToRetryTopic(trade("BTC"), 1, null);

        ArgumentCaptor<ProducerRecord<String, NormalizedTradeDTO>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().headers().lastHeader(RetryTopicService.HEADER_ERROR_MESSAGE)).isNull();
    }

    @Test
    void swallowsUnexpectedExceptions() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(new IllegalStateException("producer closed"));

        service.sendToRetryTopic(trade("BTC"), 1, new RuntimeException("x"));
        // 에러 메시지가 null 이면 헤더 생성 중 NPE → 바깥 catch 에서 처리
        service.sendToRetryTopic(trade("BTC"), 1, new RuntimeException((String) null));
    }

    @Test
    void batchSendsEveryTrade() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

        service.sendBatchToRetryTopic(List.of(trade("A"), trade("B"), trade("C")), 3, new RuntimeException("x"));

        verify(kafkaTemplate, times(3)).send(any(ProducerRecord.class));
    }
}

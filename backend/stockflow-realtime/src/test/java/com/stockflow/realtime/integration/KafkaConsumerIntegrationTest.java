package com.stockflow.realtime.integration;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Kafka Consumer 통합 테스트
 *
 * EmbeddedKafka를 사용한 실제 Kafka 통신 테스트
 */
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"market.normalized", "market.dlq"})
class KafkaConsumerIntegrationTest {

    private static final String TOPIC_NAME = "market.normalized";
    private static final String GROUP_ID = "test-group";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private DefaultKafkaProducerFactory<String, NormalizedTradeDTO> producerFactory;
    private KafkaTemplate<String, NormalizedTradeDTO> producerTemplate;
    private Consumer<String, NormalizedTradeDTO> consumer;

    @BeforeEach
    void setUp() {
        String brokers = embeddedKafka.getBrokersAsString();

        // Producer 설정
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        producerTemplate = new KafkaTemplate<>(producerFactory);

        // Consumer 설정
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, NormalizedTradeDTO.class);

        DefaultKafkaConsumerFactory<String, NormalizedTradeDTO> consumerFactory =
            new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
        if (producerFactory != null) {
            producerFactory.destroy();
        }
    }

    @Test
    void testProduceAndConsume() {
        // Given
        NormalizedTradeDTO trade = NormalizedTradeDTO.builder()
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

        // When
        producerTemplate.send(TOPIC_NAME, trade.getSymbol(), trade);

        // Then
        ConsumerRecords<String, NormalizedTradeDTO> records =
            KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));

        assertEquals(1, records.count());
        ConsumerRecord<String, NormalizedTradeDTO> record = records.iterator().next();

        assertEquals(trade.getSymbol(), record.key());
        assertEquals(trade.getSymbol(), record.value().getSymbol());
        assertEquals(trade.getPrice(), record.value().getPrice());
        assertEquals(trade.getVolume(), record.value().getVolume());
        assertEquals(trade.getSource(), record.value().getSource());
        assertEquals(trade.getTradeId(), record.value().getTradeId());
    }
}

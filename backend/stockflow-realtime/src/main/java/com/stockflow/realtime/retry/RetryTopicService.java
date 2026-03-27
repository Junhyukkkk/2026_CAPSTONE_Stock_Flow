package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Retry 토픽 서비스
 *
 * 동기 재시도 대신 별도 토픽으로 실패 메시지를 전송하여
 * Consumer의 poll 간격을 초과하지 않도록 함 (리밸런싱 방지)
 *
 * test 프로필에서만 활성화
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "retry.mode", havingValue = "async")
@RequiredArgsConstructor
public class RetryTopicService {

    private final KafkaTemplate<String, NormalizedTradeDTO> kafkaTemplate;

    @Value("${kafka.topic.retry:market.retry.test}")
    private String retryTopic;

    @Value("${kafka.topic.normalized:market.normalized}")
    private String originalTopic;

    // 헤더 키
    public static final String HEADER_RETRY_COUNT = "retry-count";
    public static final String HEADER_ORIGINAL_TOPIC = "original-topic";
    public static final String HEADER_RETRY_TIMESTAMP = "retry-timestamp";
    public static final String HEADER_ERROR_MESSAGE = "error-message";

    /**
     * 실패한 메시지를 retry 토픽으로 전송
     *
     * @param trade 실패한 거래 데이터
     * @param retryCount 현재 재시도 횟수
     * @param error 발생한 에러
     */
    public void sendToRetryTopic(NormalizedTradeDTO trade, int retryCount, Throwable error) {
        try {
            ProducerRecord<String, NormalizedTradeDTO> record = new ProducerRecord<>(
                    retryTopic,
                    null,  // partition (auto)
                    trade.getSymbol(),  // key
                    trade   // value
            );

            // 헤더 추가
            record.headers().add(new RecordHeader(
                    HEADER_RETRY_COUNT,
                    String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8)
            ));
            record.headers().add(new RecordHeader(
                    HEADER_ORIGINAL_TOPIC,
                    originalTopic.getBytes(StandardCharsets.UTF_8)
            ));
            record.headers().add(new RecordHeader(
                    HEADER_RETRY_TIMESTAMP,
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)
            ));
            if (error != null) {
                record.headers().add(new RecordHeader(
                        HEADER_ERROR_MESSAGE,
                        error.getMessage().getBytes(StandardCharsets.UTF_8)
                ));
            }

            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send to retry topic: symbol={}, retryCount={}, error={}",
                            trade.getSymbol(), retryCount, ex.getMessage());
                } else {
                    log.info("Sent to retry topic: symbol={}, retryCount={}, partition={}, offset={}",
                            trade.getSymbol(), retryCount,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Exception while sending to retry topic: symbol={}, retryCount={}",
                    trade.getSymbol(), retryCount, e);
        }
    }

    /**
     * 배치 메시지를 retry 토픽으로 전송
     */
    public void sendBatchToRetryTopic(List<NormalizedTradeDTO> trades, int retryCount, Throwable error) {
        for (NormalizedTradeDTO trade : trades) {
            sendToRetryTopic(trade, retryCount, error);
        }
        log.info("Sent batch to retry topic: size={}, retryCount={}", trades.size(), retryCount);
    }
}

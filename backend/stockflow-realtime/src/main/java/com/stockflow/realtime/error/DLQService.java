package com.stockflow.realtime.error;

import com.stockflow.core.dto.DLQMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

/**
 * Dead Letter Queue (DLQ) 전송 서비스
 * 
 * 실패한 메시지를 DLQ 토픽으로 전송
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DLQService {

    private final KafkaTemplate<String, DLQMessage> kafkaTemplate;
    private final ErrorClassifier errorClassifier;

    @Value("${spring.kafka.topic.dlq:market.dlq}")
    private String dlqTopic;

    /**
     * 실패한 메시지를 DLQ로 전송
     * 
     * @param originalTopic 원본 토픽
     * @param originalPartition 원본 파티션
     * @param originalOffset 원본 오프셋
     * @param originalMessage 원본 메시지
     * @param exception 발생한 예외
     * @param consumerGroup Consumer Group 이름
     * @param retryCount 재시도 횟수
     */
    public void sendToDLQ(
            String originalTopic,
            Integer originalPartition,
            Long originalOffset,
            Object originalMessage,
            Throwable exception,
            String consumerGroup,
            Integer retryCount) {

        ErrorType errorType = errorClassifier.classify(exception);

        DLQMessage dlqMessage = DLQMessage.builder()
            .originalTopic(originalTopic)
            .originalPartition(originalPartition)
            .originalOffset(originalOffset)
            .originalMessage(originalMessage)
            .errorType(errorType.name())
            .errorMessage(exception.getMessage())
            .stackTrace(getStackTrace(exception))
            .retryCount(retryCount != null ? retryCount : 0)
            .failedAt(Instant.now())
            .consumerGroup(consumerGroup)
            .build();

        // DLQ로 전송 (비동기)
        kafkaTemplate.send(dlqTopic, dlqMessage)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.warn("DLQ message sent successfully: topic={}, partition={}, offset={}, errorType={}",
                        originalTopic, originalPartition, originalOffset, errorType);
                } else {
                    log.error("Failed to send message to DLQ: topic={}, partition={}, offset={}",
                        originalTopic, originalPartition, originalOffset, ex);
                    // DLQ 전송 실패는 심각한 문제이므로 추가 알림 필요
                }
            });
    }

    /**
     * 배치 메시지를 DLQ로 전송
     * 
     * @param originalTopic 원본 토픽
     * @param originalMessages 원본 메시지 리스트
     * @param exception 발생한 예외
     * @param consumerGroup Consumer Group 이름
     */
    public void sendBatchToDLQ(
            String originalTopic,
            java.util.List<?> originalMessages,
            Throwable exception,
            String consumerGroup) {

        ErrorType errorType = errorClassifier.classify(exception);

        // 배치의 각 메시지를 개별적으로 DLQ에 전송
        for (int i = 0; i < originalMessages.size(); i++) {
            Object message = originalMessages.get(i);
            
            DLQMessage dlqMessage = DLQMessage.builder()
                .originalTopic(originalTopic)
                .originalPartition(null) // 배치에서는 파티션 정보 없음
                .originalOffset(null)    // 배치에서는 오프셋 정보 없음
                .originalMessage(message)
                .errorType(errorType.name())
                .errorMessage(exception.getMessage())
                .stackTrace(getStackTrace(exception))
                .retryCount(0)
                .failedAt(Instant.now())
                .consumerGroup(consumerGroup)
                .metadata(Map.of("batchIndex", i, "batchSize", originalMessages.size()))
                .build();

            kafkaTemplate.send(dlqTopic, dlqMessage);
        }

        log.warn("Sent {} messages to DLQ: topic={}, errorType={}",
            originalMessages.size(), originalTopic, errorType);
    }

    /**
     * 예외의 스택 트레이스를 문자열로 변환
     */
    private String getStackTrace(Throwable exception) {
        if (exception == null) {
            return null;
        }

        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        exception.printStackTrace(pw);
        return sw.toString();
    }
}

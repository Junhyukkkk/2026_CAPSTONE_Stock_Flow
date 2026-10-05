package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessorInterface;
import com.stockflow.realtime.service.RedisPriceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 실시간 데이터 Consumer (배치 소비 버전)
 *
 * stockflow.opt.realtime-batch=true (env STOCKFLOW_OPT_REALTIME_BATCH) 일 때만 등록되며,
 * 그 경우 단건 RealtimeConsumer 는 등록되지 않아 realtime-group 을 소비하는 리스너는 항상 하나뿐이다.
 *
 * poll 단위로 심볼별 최신 1건만 파이프라인 1회로 Redis 에 쓴다. 배치 처리가 실패하면
 * 레코드별로 기존 단건 경로(RetryableProcessor)로 폴백해 재시도/DLQ 의미를 유지한다.
 */
@Slf4j
@Component
@ConditionalOnExpression("${realtime.consumer.enabled:true} and ${stockflow.opt.realtime-batch:false}")
@RequiredArgsConstructor
public class RealtimeBatchConsumer {

    private final RetryableProcessorInterface retryableProcessor;
    private final PerformanceMetrics performanceMetrics;
    private final RedisPriceService redisPriceService;

    @Value("${spring.kafka.consumer.group.realtime:realtime-group}")
    private String consumerGroup;

    @KafkaListener(
        topics = "${spring.kafka.topic.normalized:market.normalized}",
        groupId = "${spring.kafka.consumer.group.realtime:realtime-group}",
        containerFactory = "realtimeBatchKafkaListenerContainerFactory"
    )
    public void consumeRealtimeBatch(
            List<ConsumerRecord<String, NormalizedTradeDTO>> records,
            Acknowledgment acknowledgment) {

        if (records == null || records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        long startTime = System.currentTimeMillis();

        List<NormalizedTradeDTO> trades = new ArrayList<>(records.size());
        for (ConsumerRecord<String, NormalizedTradeDTO> record : records) {
            if (record.value() != null) {
                trades.add(record.value());
            }
        }

        try {
            redisPriceService.processRealtimeTradeBatch(trades);
        } catch (Exception e) {
            log.warn("Batch processing failed, falling back to per-record path: size={}", records.size(), e);
            processIndividually(records, acknowledgment, startTime);
            return;
        }

        acknowledgment.acknowledge();
        for (NormalizedTradeDTO trade : trades) {
            recordSuccess(trade);
        }
        performanceMetrics.recordProcessingTime(System.currentTimeMillis() - startTime);
        log.debug("Processed realtime batch: size={}", records.size());
    }

    /** 단건 리스너와 같은 처리·메트릭·ack 의미. 하나라도 실패하면 배치 전체를 ack 하지 않는다. */
    private void processIndividually(
            List<ConsumerRecord<String, NormalizedTradeDTO>> records,
            Acknowledgment acknowledgment,
            long startTime) {

        boolean allSucceeded = true;
        for (ConsumerRecord<String, NormalizedTradeDTO> record : records) {
            NormalizedTradeDTO trade = record.value();
            if (trade == null) {
                continue;
            }
            boolean success = retryableProcessor.processWithRetry(
                trade, redisPriceService::processRealtimeTrade, consumerGroup, record.partition(), record.offset());
            if (success) {
                recordSuccess(trade);
            } else {
                allSucceeded = false;
                performanceMetrics.recordFailure();
                log.warn("Failed to process trade: symbol={}, partition={}, offset={}",
                    trade.getSymbol(), record.partition(), record.offset());
            }
        }

        performanceMetrics.recordProcessingTime(System.currentTimeMillis() - startTime);
        if (allSucceeded) {
            acknowledgment.acknowledge();
        }
    }

    private void recordSuccess(NormalizedTradeDTO trade) {
        if (trade.getTimestamp() != null) {
            performanceMetrics.recordSuccessWithLatency(trade.getTimestamp());
        } else {
            performanceMetrics.recordSuccess();
        }
    }
}

package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.core.retry.RetryPolicy;
import com.stockflow.core.retry.RetryService;
import com.stockflow.realtime.dlq.DLQService;
import com.stockflow.realtime.transaction.IdempotencyChannels;
import com.stockflow.realtime.transaction.IdempotencyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * 저장 서비스
 *
 * 멱등성 체크, 트랜잭션 관리, DB 저장, 재시도를 한 곳에서 처리
 */
@Slf4j
@Service
public class StorageService {

    private final IdempotencyService idempotencyService;
    private final MarketTickBulkWriter marketTickBulkWriter;
    private final InstrumentRegistryService instrumentRegistryService;
    private final TransactionTemplate transactionTemplate;
    private final RetryService retryService;
    private final RetryPolicy retryPolicy;
    private final ErrorClassifier errorClassifier;
    private final DLQService dlqService;

    @Value("${spring.kafka.topic.normalized:market.normalized}")
    private String topicName;

    public StorageService(
            IdempotencyService idempotencyService,
            MarketTickBulkWriter marketTickBulkWriter,
            InstrumentRegistryService instrumentRegistryService,
            @Qualifier("storageJdbcTransactionTemplate") TransactionTemplate transactionTemplate,
            RetryService retryService,
            RetryPolicy retryPolicy,
            ErrorClassifier errorClassifier,
            DLQService dlqService) {
        this.idempotencyService = idempotencyService;
        this.marketTickBulkWriter = marketTickBulkWriter;
        this.instrumentRegistryService = instrumentRegistryService;
        this.transactionTemplate = transactionTemplate;
        this.retryService = retryService;
        this.retryPolicy = retryPolicy;
        this.errorClassifier = errorClassifier;
        this.dlqService = dlqService;
    }

    /**
     * 배치 저장 (멱등성 + 트랜잭션 + 재시도 포함)
     *
     * @param trades 저장할 거래 데이터
     * @param consumerGroup Consumer Group 이름
     * @return 성공 여부
     */
    public boolean saveBatch(List<NormalizedTradeDTO> trades, String consumerGroup) {
        try {
            saveWithRetry(trades);
            return true;
        } catch (Exception e) {
            log.error("Failed to save batch after retries: size={}", trades.size(), e);
            dlqService.sendBatchToDLQ(topicName, trades, e, consumerGroup);
            return false;
        }
    }

    private void saveWithRetry(List<NormalizedTradeDTO> trades) throws Exception {
        try {
            saveInTransaction(trades);
        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);

            if (!errorClassifier.isRetryable(errorType)) {
                throw e;
            }

            retryService.executeWithRetry(
                () -> {
                    saveInTransaction(trades);
                    return null;
                },
                errorType,
                retryPolicy
            );
        }
    }

    private void saveInTransaction(List<NormalizedTradeDTO> trades) {
        // 1. 멱등성 체크로 중복 필터링
        List<NormalizedTradeDTO> newTrades = filterDuplicates(trades);

        if (newTrades.isEmpty()) {
            log.debug("All messages already processed: size={}", trades.size());
            return;
        }

        if (newTrades.size() < trades.size()) {
            log.debug("Filtered duplicates: original={}, new={}", trades.size(), newTrades.size());
        }

        // 2. 트랜잭션 내에서 저장
        List<NormalizedTradeDTO> snapshot = new ArrayList<>(newTrades);
        transactionTemplate.executeWithoutResult(status -> {
            marketTickBulkWriter.insertBatch(newTrades);
            instrumentRegistryService.registerDistinctFromTrades(newTrades);
            markProcessedAfterCommit(snapshot);
        });
    }

    private List<NormalizedTradeDTO> filterDuplicates(List<NormalizedTradeDTO> trades) {
        return trades.stream()
            .filter(t -> !idempotencyService.isAlreadyProcessed(IdempotencyChannels.STORAGE, t))
            .toList();
    }

    private void markProcessedAfterCommit(List<NormalizedTradeDTO> trades) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    idempotencyService.markBatchAsProcessed(IdempotencyChannels.STORAGE, trades);
                }
            });
        } else {
            idempotencyService.markBatchAsProcessed(IdempotencyChannels.STORAGE, trades);
        }
    }
}

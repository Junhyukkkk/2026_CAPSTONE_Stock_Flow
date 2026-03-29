package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 실시간 경로: DB 트랜잭션 없음(Redis만). 저장 경로: JDBC 전용 TransactionTemplate으로 배치 커밋.
 */
@Slf4j
@Component
public class RealtimeTransactionManager {

    private final IdempotencyService idempotencyService;
    private final TransactionTemplate storageJdbcTransactionTemplate;

    public RealtimeTransactionManager(
            IdempotencyService idempotencyService,
            @Qualifier("storageJdbcTransactionTemplate") TransactionTemplate storageJdbcTransactionTemplate) {
        this.idempotencyService = idempotencyService;
        this.storageJdbcTransactionTemplate = storageJdbcTransactionTemplate;
    }

    public boolean processWithTransaction(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor) {

        if (idempotencyService.isAlreadyProcessed(IdempotencyChannels.REALTIME, trade)) {
            log.debug("Message already processed: source={}, tradeId={}",
                    trade.getSource(), trade.getTradeId());
            return true;
        }

        try {
            processor.accept(trade);
            registerMarkAfterCommit(IdempotencyChannels.REALTIME, List.of(trade));
            return true;
        } catch (Exception e) {
            log.error("Transaction failed: source={}, tradeId={}",
                    trade.getSource(), trade.getTradeId(), e);
            throw e;
        }
    }

    public boolean processBatchWithTransaction(
            List<NormalizedTradeDTO> trades,
            Consumer<List<NormalizedTradeDTO>> processor) {

        List<NormalizedTradeDTO> newTrades = trades.stream()
                .filter(trade -> !idempotencyService.isAlreadyProcessed(IdempotencyChannels.STORAGE, trade))
                .collect(Collectors.toList());

        if (newTrades.isEmpty()) {
            log.debug("All messages already processed: batchSize={}", trades.size());
            return true;
        }

        if (newTrades.size() < trades.size()) {
            log.debug("Filtered duplicate messages: original={}, new={}", trades.size(), newTrades.size());
        }

        try {
            List<NormalizedTradeDTO> snapshot = new ArrayList<>(newTrades);
            storageJdbcTransactionTemplate.executeWithoutResult(status -> {
                processor.accept(newTrades);
                registerMarkAfterCommit(IdempotencyChannels.STORAGE, snapshot);
            });
            return true;
        } catch (Exception e) {
            log.error("Batch transaction failed: size={}", newTrades.size(), e);
            throw e;
        }
    }

    private void registerMarkAfterCommit(String channel, List<NormalizedTradeDTO> trades) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    idempotencyService.markBatchAsProcessed(channel, trades);
                }
            });
        } else {
            idempotencyService.markBatchAsProcessed(channel, trades);
        }
    }
}

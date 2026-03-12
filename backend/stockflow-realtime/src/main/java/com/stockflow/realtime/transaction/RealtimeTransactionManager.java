package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;
import java.util.function.Consumer;

/**
 * 트랜잭션 관리자
 * 
 * 데이터 일관성 보장을 위한 트랜잭션 처리
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RealtimeTransactionManager {

    private final IdempotencyService idempotencyService;

    /**
     * 트랜잭션으로 단일 메시지 처리
     * 
     * @param trade 거래 데이터
     * @param processor 실제 처리 로직
     * @return 처리 성공 여부
     */
    @Transactional
    public boolean processWithTransaction(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor) {
        
        // Idempotency 체크
        if (idempotencyService.isAlreadyProcessed(trade)) {
            log.debug("Message already processed: source={}, tradeId={}", 
                trade.getSource(), trade.getTradeId());
            return true; // 이미 처리됨 (성공으로 간주)
        }

        try {
            // 실제 처리
            processor.accept(trade);
            
            // 처리 완료 표시 (트랜잭션 커밋 후)
            idempotencyService.markAsProcessed(trade);
            
            return true;
            
        } catch (Exception e) {
            log.error("Transaction failed: source={}, tradeId={}", 
                trade.getSource(), trade.getTradeId(), e);
            // 트랜잭션 롤백 (자동)
            throw e;
        }
    }

    /**
     * 트랜잭션으로 배치 메시지 처리
     * 
     * @param trades 거래 데이터 리스트
     * @param processor 실제 처리 로직
     * @return 처리 성공 여부
     */
    @Transactional
    public boolean processBatchWithTransaction(
            List<NormalizedTradeDTO> trades,
            Consumer<List<NormalizedTradeDTO>> processor) {
        
        // 중복 제거 (이미 처리된 메시지 필터링)
        List<NormalizedTradeDTO> newTrades = trades.stream()
            .filter(trade -> !idempotencyService.isAlreadyProcessed(trade))
            .collect(Collectors.toList());

        if (newTrades.isEmpty()) {
            log.debug("All messages already processed: batchSize={}", trades.size());
            return true; // 모두 이미 처리됨
        }

        if (newTrades.size() < trades.size()) {
            log.debug("Filtered duplicate messages: original={}, new={}", 
                trades.size(), newTrades.size());
        }

        try {
            // 실제 처리
            processor.accept(newTrades);
            
            // 처리 완료 표시 (트랜잭션 커밋 후)
            idempotencyService.markBatchAsProcessed(newTrades);
            
            return true;
            
        } catch (Exception e) {
            log.error("Batch transaction failed: size={}", newTrades.size(), e);
            // 트랜잭션 롤백 (자동)
            throw e;
        }
    }
}

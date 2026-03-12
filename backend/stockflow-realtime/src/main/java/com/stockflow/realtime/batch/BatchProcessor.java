package com.stockflow.realtime.batch;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 배치 처리기
 * 
 * 메시지를 버퍼에 모아서 배치로 처리
 * 시간 기반 또는 크기 기반으로 배치 생성
 */
@Slf4j
@Component
public class BatchProcessor {

    @Value("${spring.kafka.batch.size:100}")
    private int batchSize;

    @Value("${spring.kafka.batch.timeout-ms:1000}")
    private long batchTimeoutMs;

    /**
     * 배치로 메시지 처리
     * 
     * @param messages 메시지 리스트
     * @param processor 배치 처리 로직
     * @param <T> 메시지 타입
     */
    public <T> void processBatch(List<T> messages, Consumer<List<T>> processor) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        log.debug("Processing batch: size={}", messages.size());
        
        try {
            processor.accept(messages);
            log.debug("Successfully processed batch: size={}", messages.size());
        } catch (Exception e) {
            log.error("Error processing batch: size={}", messages.size(), e);
            throw e;
        }
    }

    /**
     * 배치 조건 확인
     * 
     * @param currentSize 현재 배치 크기
     * @param elapsedTime 경과 시간 (밀리초)
     * @return 배치 처리 여부
     */
    public boolean shouldProcessBatch(int currentSize, long elapsedTime) {
        // 크기 기반: 배치 크기가 최대 크기에 도달
        if (currentSize >= batchSize) {
            log.debug("Batch size reached: size={}", currentSize);
            return true;
        }
        
        // 시간 기반: 타임아웃 시간 경과
        if (elapsedTime >= batchTimeoutMs) {
            log.debug("Batch timeout reached: elapsed={}ms", elapsedTime);
            return true;
        }
        
        return false;
    }

    /**
     * 배치 크기 가져오기
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * 배치 타임아웃 가져오기
     */
    public long getBatchTimeoutMs() {
        return batchTimeoutMs;
    }
}

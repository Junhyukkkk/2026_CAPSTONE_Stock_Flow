package com.stockflow.core.util;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 배치 버퍼
 *
 * 메시지를 버퍼에 모아서 배치로 처리
 * Thread-safe 보장
 */
@Slf4j
@Component
public class BatchBuffer {

    private final ReentrantLock lock = new ReentrantLock();
    private final List<NormalizedTradeDTO> buffer = new ArrayList<>();
    private long lastFlushTime = System.currentTimeMillis();

    /**
     * 메시지 추가
     *
     * @param trade 거래 데이터
     * @return 현재 버퍼 크기
     */
    public int add(NormalizedTradeDTO trade) {
        lock.lock();
        try {
            buffer.add(trade);
            return buffer.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 버퍼에서 모든 메시지 가져오기 (버퍼 비우기)
     *
     * @return 메시지 리스트
     */
    public List<NormalizedTradeDTO> flush() {
        lock.lock();
        try {
            List<NormalizedTradeDTO> result = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 버퍼 크기 가져오기
     */
    public int size() {
        lock.lock();
        try {
            return buffer.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 마지막 플러시 이후 경과 시간 (밀리초)
     */
    public long getElapsedTimeSinceLastFlush() {
        lock.lock();
        try {
            return System.currentTimeMillis() - lastFlushTime;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 버퍼가 비어있는지 확인
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return buffer.isEmpty();
        } finally {
            lock.unlock();
        }
    }
}

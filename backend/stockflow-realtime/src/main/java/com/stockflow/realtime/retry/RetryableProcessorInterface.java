package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;

import java.util.List;
import java.util.function.Consumer;

/**
 * 재시도 처리기 인터페이스
 *
 * test 프로필: 비동기 Retry 토픽 방식
 * 운영 프로필: 동기 재시도 방식
 */
public interface RetryableProcessorInterface {

    boolean processWithRetry(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor,
            String consumerGroup,
            int partition,
            long offset);

    boolean processBatchWithRetry(
            List<NormalizedTradeDTO> trades,
            Consumer<List<NormalizedTradeDTO>> processor,
            String consumerGroup);
}

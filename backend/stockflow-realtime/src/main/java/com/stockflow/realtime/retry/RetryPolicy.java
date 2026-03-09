package com.stockflow.realtime.retry;

import lombok.Builder;
import lombok.Value;

/**
 * 재시도 정책 설정
 * 
 * Exponential Backoff를 사용한 재시도 전략
 */
@Value
@Builder
public class RetryPolicy {
    
    /**
     * 최대 재시도 횟수
     */
    @Builder.Default
    int maxRetries = 3;
    
    /**
     * 초기 지연 시간 (밀리초)
     */
    @Builder.Default
    long initialDelayMs = 1000; // 1초
    
    /**
     * 최대 지연 시간 (밀리초)
     */
    @Builder.Default
    long maxDelayMs = 60000; // 60초
    
    /**
     * 지연 시간 배수 (Exponential Backoff)
     */
    @Builder.Default
    double multiplier = 2.0;
    
    /**
     * 지터 (Jitter) 사용 여부
     * 랜덤 지터를 추가하여 Thundering Herd 문제 방지
     */
    @Builder.Default
    boolean useJitter = true;
    
    /**
     * 지터 비율 (0.0 ~ 1.0)
     * 예: 0.2 = ±20% 랜덤 변동
     */
    @Builder.Default
    double jitterRatio = 0.2;
}

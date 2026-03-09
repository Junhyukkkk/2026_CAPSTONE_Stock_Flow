package com.stockflow.core.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Dead Letter Queue (DLQ) 메시지 구조
 * 
 * 실패한 메시지의 원본 데이터와 에러 정보를 포함하여
 * 나중에 분석 및 재처리할 수 있도록 함
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DLQMessage {

    /**
     * 원본 토픽 이름
     */
    private String originalTopic;

    /**
     * 원본 파티션 번호
     */
    private Integer originalPartition;

    /**
     * 원본 오프셋
     */
    private Long originalOffset;

    /**
     * 원본 메시지 (실패한 메시지)
     */
    private Object originalMessage;

    /**
     * 에러 타입
     * 
     * 예시:
     * - VALIDATION_ERROR: 데이터 검증 실패
     * - PROCESSING_ERROR: 처리 중 오류
     * - STORAGE_ERROR: 저장소 연결/저장 실패
     * - TIMEOUT_ERROR: 처리 시간 초과
     * - UNKNOWN_ERROR: 알 수 없는 오류
     */
    private String errorType;

    /**
     * 에러 메시지
     */
    private String errorMessage;

    /**
     * 에러 스택 트레이스 (선택적)
     */
    private String stackTrace;

    /**
     * 재시도 횟수
     */
    private Integer retryCount;

    /**
     * 실패 시각 (UTC)
     */
    private Instant failedAt;

    /**
     * Consumer Group 이름
     */
    private String consumerGroup;

    /**
     * 추가 메타데이터 (선택적)
     */
    private Map<String, Object> metadata;
}

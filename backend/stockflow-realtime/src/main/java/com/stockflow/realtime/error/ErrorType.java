package com.stockflow.realtime.error;

/**
 * 에러 타입 열거형
 * 
 * 에러를 분류하여 적절한 처리 전략을 적용
 */
public enum ErrorType {
    
    /**
     * 데이터 검증 실패
     * - 잘못된 형식의 메시지
     * - 필수 필드 누락
     * - 데이터 범위 초과
     * 
     * 처리: DLQ 전송 (재시도 불필요)
     */
    VALIDATION_ERROR,
    
    /**
     * 처리 중 오류
     * - 비즈니스 로직 오류
     * - 데이터 변환 실패
     * 
     * 처리: DLQ 전송 (재시도 불필요)
     */
    PROCESSING_ERROR,
    
    /**
     * 저장소 연결 실패
     * - Redis 연결 실패
     * - PostgreSQL 연결 실패
     * 
     * 처리: 재시도 (일시적 오류 가능성)
     */
    STORAGE_CONNECTION_ERROR,
    
    /**
     * 저장소 저장 실패
     * - Redis 저장 실패
     * - DB 저장 실패
     * 
     * 처리: 재시도 후 DLQ 전송
     */
    STORAGE_ERROR,
    
    /**
     * 처리 시간 초과
     * - 타임아웃 발생
     * 
     * 처리: 재시도 후 DLQ 전송
     */
    TIMEOUT_ERROR,
    
    /**
     * 알 수 없는 오류
     * - 예상치 못한 예외
     * 
     * 처리: DLQ 전송
     */
    UNKNOWN_ERROR
}

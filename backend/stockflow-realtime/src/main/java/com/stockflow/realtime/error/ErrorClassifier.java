package com.stockflow.realtime.error;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

/**
 * 에러 분류기
 * 
 * 예외를 분석하여 ErrorType으로 분류
 * 재시도 가능 여부를 판단
 */
@Slf4j
@Component
public class ErrorClassifier {

    /**
     * 예외를 ErrorType으로 분류
     * 
     * @param exception 발생한 예외
     * @return ErrorType
     */
    public ErrorType classify(Throwable exception) {
        if (exception == null) {
            return ErrorType.UNKNOWN_ERROR;
        }

        // 타임아웃 오류
        if (exception instanceof TimeoutException) {
            return ErrorType.TIMEOUT_ERROR;
        }

        // 연결 오류 (재시도 가능)
        if (exception instanceof ConnectException) {
            return ErrorType.STORAGE_CONNECTION_ERROR;
        }

        // SQL 오류
        if (exception instanceof SQLException) {
            SQLException sqlException = (SQLException) exception;
            // 연결 관련 SQL 오류는 재시도 가능
            if (isConnectionError(sqlException)) {
                return ErrorType.STORAGE_CONNECTION_ERROR;
            }
            return ErrorType.STORAGE_ERROR;
        }

        // IllegalArgumentException, NullPointerException 등
        // 데이터 검증/처리 오류
        if (exception instanceof IllegalArgumentException ||
            exception instanceof NullPointerException ||
            exception instanceof ClassCastException) {
            return ErrorType.VALIDATION_ERROR;
        }

        // Redis 연결 오류 (Lettuce 예외)
        String exceptionMessage = exception.getMessage();
        if (exceptionMessage != null) {
            if (exceptionMessage.contains("Connection refused") ||
                exceptionMessage.contains("Unable to connect")) {
                return ErrorType.STORAGE_CONNECTION_ERROR;
            }
        }

        // 기본값: 처리 오류
        return ErrorType.PROCESSING_ERROR;
    }

    /**
     * 재시도 가능한 에러인지 판단
     * 
     * @param errorType 에러 타입
     * @return 재시도 가능 여부
     */
    public boolean isRetryable(ErrorType errorType) {
        return errorType == ErrorType.STORAGE_CONNECTION_ERROR ||
               errorType == ErrorType.STORAGE_ERROR ||
               errorType == ErrorType.TIMEOUT_ERROR;
    }

    /**
     * SQL 예외가 연결 오류인지 확인
     */
    private boolean isConnectionError(SQLException e) {
        String sqlState = e.getSQLState();
        // PostgreSQL 연결 오류 코드
        return sqlState != null && (
            sqlState.startsWith("08") || // Connection Exception
            sqlState.equals("08003") ||  // Connection does not exist
            sqlState.equals("08006") ||  // Connection failure
            sqlState.equals("08001")     // Unable to establish connection
        );
    }
}

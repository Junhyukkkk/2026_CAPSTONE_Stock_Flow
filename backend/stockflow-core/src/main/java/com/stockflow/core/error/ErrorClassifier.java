package com.stockflow.core.error;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

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

    private static final int MAX_CAUSE_CHAIN_DEPTH = 12;

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

        Throwable root = unwrapSpringDataCause(exception);
        ErrorType direct = classifyLeaf(root);
        if (direct != ErrorType.PROCESSING_ERROR) {
            return direct;
        }

        // 원인 체인에서 SQL/연결/검증 재탐색
        Throwable t = exception;
        for (int i = 0; i < MAX_CAUSE_CHAIN_DEPTH && t != null; i++) {
            ErrorType leaf = classifyLeaf(t);
            if (leaf != ErrorType.PROCESSING_ERROR) {
                return leaf;
            }
            t = t.getCause();
        }

        return ErrorType.PROCESSING_ERROR;
    }

    private Throwable unwrapSpringDataCause(Throwable exception) {
        Throwable t = exception;
        for (int i = 0; i < MAX_CAUSE_CHAIN_DEPTH && t != null; i++) {
            if (t instanceof DataAccessException && t.getCause() != null) {
                t = t.getCause();
                continue;
            }
            if (t instanceof TransactionException && t.getCause() != null) {
                t = t.getCause();
                continue;
            }
            break;
        }
        return t != null ? t : exception;
    }

    private ErrorType classifyLeaf(Throwable exception) {
        if (exception instanceof TimeoutException) {
            return ErrorType.TIMEOUT_ERROR;
        }

        if (exception instanceof ConnectException) {
            return ErrorType.STORAGE_CONNECTION_ERROR;
        }

        if (exception instanceof SQLException) {
            SQLException sqlException = (SQLException) exception;
            if (isConnectionError(sqlException)) {
                return ErrorType.STORAGE_CONNECTION_ERROR;
            }
            return ErrorType.STORAGE_ERROR;
        }

        if (exception instanceof IllegalArgumentException ||
            exception instanceof NullPointerException ||
            exception instanceof ClassCastException) {
            return ErrorType.VALIDATION_ERROR;
        }

        String exceptionMessage = exception.getMessage();
        if (exceptionMessage != null) {
            if (exceptionMessage.contains("Connection refused") ||
                exceptionMessage.contains("Unable to connect")) {
                return ErrorType.STORAGE_CONNECTION_ERROR;
            }
        }

        if (exception instanceof DataAccessException) {
            return ErrorType.STORAGE_ERROR;
        }

        if (exception instanceof TransactionException) {
            return ErrorType.STORAGE_ERROR;
        }

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
               errorType == ErrorType.TIMEOUT_ERROR ||
               errorType == ErrorType.VALIDATION_ERROR;  // 실험용: 리밸런싱 재현
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

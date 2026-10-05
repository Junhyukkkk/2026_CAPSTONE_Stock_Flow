package com.stockflow.core.error;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.net.ConnectException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** classify(): 예외 종류·원인 체인에 따른 ErrorType 분류. */
class ErrorClassifierClassifyTest {

    private final ErrorClassifier classifier = new ErrorClassifier();

    @Test
    void nullIsUnknown() {
        assertThat(classifier.classify(null)).isEqualTo(ErrorType.UNKNOWN_ERROR);
    }

    @Test
    void timeoutAndConnectExceptions() {
        assertThat(classifier.classify(new TimeoutException("slow"))).isEqualTo(ErrorType.TIMEOUT_ERROR);
        assertThat(classifier.classify(new ConnectException("down"))).isEqualTo(ErrorType.STORAGE_CONNECTION_ERROR);
    }

    @Test
    void sqlExceptionDependsOnSqlState() {
        assertThat(classifier.classify(new SQLException("conn", "08006"))).isEqualTo(ErrorType.STORAGE_CONNECTION_ERROR);
        assertThat(classifier.classify(new SQLException("dup", "23505"))).isEqualTo(ErrorType.STORAGE_ERROR);
        assertThat(classifier.classify(new SQLException("no state"))).isEqualTo(ErrorType.STORAGE_ERROR);
    }

    @Test
    void programmingErrorsAreValidationErrors() {
        assertThat(classifier.classify(new IllegalArgumentException("bad"))).isEqualTo(ErrorType.VALIDATION_ERROR);
        assertThat(classifier.classify(new NullPointerException())).isEqualTo(ErrorType.VALIDATION_ERROR);
        assertThat(classifier.classify(new ClassCastException())).isEqualTo(ErrorType.VALIDATION_ERROR);
    }

    @Test
    void messageBasedConnectionDetection() {
        assertThat(classifier.classify(new RuntimeException("Connection refused: host")))
                .isEqualTo(ErrorType.STORAGE_CONNECTION_ERROR);
        assertThat(classifier.classify(new RuntimeException("Unable to connect to redis")))
                .isEqualTo(ErrorType.STORAGE_CONNECTION_ERROR);
    }

    @Test
    void springDataAccessWithoutCauseIsStorageError() {
        assertThat(classifier.classify(new DataIntegrityViolationException("dup"))).isEqualTo(ErrorType.STORAGE_ERROR);
        assertThat(classifier.classify(new CannotCreateTransactionException("tx"))).isEqualTo(ErrorType.STORAGE_ERROR);
    }

    @Test
    void springWrappersAreUnwrappedToTheirCause() {
        assertThat(classifier.classify(new DataAccessResourceFailureException("wrap", new SQLException("c", "08001"))))
                .isEqualTo(ErrorType.STORAGE_CONNECTION_ERROR);
        assertThat(classifier.classify(new CannotCreateTransactionException("wrap", new TimeoutException("t"))))
                .isEqualTo(ErrorType.TIMEOUT_ERROR);
    }

    @Test
    void genericWrapperIsClassifiedByItsCauseChain() {
        Throwable wrapped = new RuntimeException("outer", new RuntimeException("mid", new SQLException("x", "08003")));
        assertThat(classifier.classify(wrapped)).isEqualTo(ErrorType.STORAGE_CONNECTION_ERROR);
    }

    @Test
    void unrelatedExceptionIsProcessingError() {
        assertThat(classifier.classify(new IllegalStateException("boom"))).isEqualTo(ErrorType.PROCESSING_ERROR);
        assertThat(classifier.classify(new RuntimeException((String) null))).isEqualTo(ErrorType.PROCESSING_ERROR);
    }
}

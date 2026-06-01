package com.stockflow.realtime.backtest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/**
 * 백테스트 컨트롤러 전용 예외 매핑.
 * 다른 도메인 컨트롤러에 영향을 주지 않도록 backtest 패키지로 범위를 한정한다.
 */
@RestControllerAdvice(basePackageClasses = BacktestController.class)
public class BacktestExceptionHandler {

    @ExceptionHandler(BacktestRunService.NoDataException.class)
    public ProblemDetail handleNoData(BacktestRunService.NoDataException ex) {
        return problem(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleBadRequest(IllegalArgumentException ex) {
        return problem(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    private ProblemDetail problem(HttpStatus status, String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setProperty("timestamp", Instant.now());
        return pd;
    }
}

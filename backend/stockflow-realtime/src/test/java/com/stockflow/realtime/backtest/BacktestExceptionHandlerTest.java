package com.stockflow.realtime.backtest;

import com.stockflow.realtime.prediction.PredictionService.PredictionServiceException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

import static org.assertj.core.api.Assertions.assertThat;

class BacktestExceptionHandlerTest {

    private final BacktestExceptionHandler handler = new BacktestExceptionHandler();

    @Test
    void noDataMapsTo404() {
        ProblemDetail pd = handler.handleNoData(new BacktestRunService.NoDataException("none"));

        assertThat(pd.getStatus()).isEqualTo(404);
        assertThat(pd.getDetail()).isEqualTo("none");
        assertThat(pd.getProperties()).containsKey("timestamp");
    }

    @Test
    void illegalArgumentMapsTo400() {
        assertThat(handler.handleBadRequest(new IllegalArgumentException("bad")).getStatus()).isEqualTo(400);
    }

    @Test
    void predictionServiceExceptionKeepsItsStatus() {
        ProblemDetail pd = handler.handlePredictionService(
                new PredictionServiceException(HttpStatus.BAD_GATEWAY, "upstream"));

        assertThat(pd.getStatus()).isEqualTo(502);
        assertThat(pd.getDetail()).isEqualTo("upstream");
    }
}

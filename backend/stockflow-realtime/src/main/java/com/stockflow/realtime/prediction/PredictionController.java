package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
import com.stockflow.realtime.prediction.PredictionService.PredictionServiceException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Validated
@RestController
@RequestMapping("/api/predictions")
@RequiredArgsConstructor
@Tag(name = "Prediction", description = "FastAPI 예측 서비스 연동 API")
public class PredictionController {

    private final PredictionService predictionService;

    @GetMapping("/{symbol}/compare")
    @Operation(summary = "예측 모델 비교", description = "ARIMA, 로그수익률 ARIMA, Chronos-Bolt 예측을 비교합니다.")
    public JsonNode compare(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "1m")
            @Pattern(regexp = "1m|1d", message = "interval must be 1m or 1d") String interval,
            @RequestParam(defaultValue = "10")
            @Min(1) @Max(100) int horizon,
            @RequestParam(required = false) String source) {
        return predictionService.compare(symbol, interval, horizon, source);
    }

    @ExceptionHandler(PredictionServiceException.class)
    public ResponseEntity<Map<String, String>> handlePredictionServiceException(
            PredictionServiceException exception) {
        return ResponseEntity.status(exception.getStatus())
                .body(Map.of("message", exception.getMessage()));
    }
}

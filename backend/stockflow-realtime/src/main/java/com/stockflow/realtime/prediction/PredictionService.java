package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
import com.stockflow.realtime.stock.SymbolSourceResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.time.Duration;
import java.util.Optional;

@Slf4j
@Service
public class PredictionService {

    private final RestClient client;
    private final PredictionHistoryRecorder historyRecorder;
    private final boolean historyEnabled;
    private final SymbolSourceResolver symbolSourceResolver;

    public PredictionService(
            RestClient.Builder builder,
            @Value("${analysis.base-url}") String baseUrl,
            @Value("${analysis.connect-timeout-ms:3000}") int connectTimeoutMs,
            @Value("${analysis.read-timeout-ms:120000}") int readTimeoutMs,
            PredictionHistoryRecorder historyRecorder,
            @Value("${prediction.history.enabled:true}") boolean historyEnabled,
            SymbolSourceResolver symbolSourceResolver) {
        this.historyRecorder = historyRecorder;
        this.symbolSourceResolver = symbolSourceResolver;
        this.historyEnabled = historyEnabled;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        this.client = builder
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }

    public JsonNode compare(String symbol, String interval, int horizon, String requestedSource) {
        // 분봉은 같은 심볼의 여러 출처가 섞이지 않도록 서버가 출처를 정한다. 일봉(symbol_daily_ohlcv)은 출처 값이
        // ohlcv_1m 과 다를 수 있어 건드리지 않는다. 이력에도 실제 사용한 출처를 저장한다.
        String source = (requestedSource == null || requestedSource.isBlank()) && "1m".equals(interval)
                ? symbolSourceResolver.latestSource(symbol).orElse(requestedSource)
                : requestedSource;
        try {
            long startedNanos = System.nanoTime();
            JsonNode response = client.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/predict/{symbol}/compare")
                            .queryParam("interval", interval)
                            .queryParam("horizon", horizon)
                            .queryParamIfPresent("source", Optional.ofNullable(source)
                                    .filter(value -> !value.isBlank()))
                            .build(symbol.toUpperCase()))
                    .retrieve()
                    .body(JsonNode.class);
            if (response == null) {
                throw new PredictionServiceException(
                        HttpStatus.BAD_GATEWAY, "예측 서비스가 빈 응답을 반환했습니다.");
            }
            recordHistory(symbol, interval, horizon, source, response,
                    (System.nanoTime() - startedNanos) / 1_000_000L);
            return response;
        } catch (HttpClientErrorException.NotFound e) {
            throw new PredictionServiceException(
                    HttpStatus.NOT_FOUND, "예측에 필요한 시세 데이터가 부족합니다.", e);
        } catch (RestClientResponseException e) {
            throw new PredictionServiceException(
                    HttpStatus.BAD_GATEWAY, "예측 서비스 응답을 처리하지 못했습니다.", e);
        } catch (RestClientException e) {
            throw new PredictionServiceException(
                    HttpStatus.SERVICE_UNAVAILABLE, "예측 서비스에 연결할 수 없습니다.", e);
        }
    }

    /** 저장은 부가 기능이므로 어떤 실패도 compare 응답에 영향을 주지 않는다. */
    private void recordHistory(String symbol, String interval, int horizon, String source,
                               JsonNode response, long latencyMs) {
        if (!historyEnabled) {
            return;
        }
        try {
            historyRecorder.record(symbol, interval, horizon, source, response, latencyMs);
        } catch (RuntimeException e) {
            log.warn("예측 이력 저장 요청 실패(무시): symbol={} interval={}", symbol, interval, e);
        }
    }

    public PredictionSignalResponse backtestSignals(PredictionSignalRequest request) {
        try {
            PredictionSignalResponse response = client.post()
                    .uri("/backtest/prediction-signals")
                    .body(request)
                    .retrieve()
                    .body(PredictionSignalResponse.class);
            if (response == null) {
                throw new PredictionServiceException(
                        HttpStatus.BAD_GATEWAY, "예측 서비스가 빈 신호 응답을 반환했습니다.");
            }
            return response;
        } catch (HttpClientErrorException.NotFound e) {
            throw new PredictionServiceException(
                    HttpStatus.NOT_FOUND, "예측 백테스트에 필요한 일봉 데이터가 부족합니다.", e);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 422) {
                throw new IllegalArgumentException(
                        "예측 신호 요청 조건이 올바르지 않습니다: " + e.getResponseBodyAsString(), e);
            }
            throw new PredictionServiceException(
                    HttpStatus.BAD_GATEWAY, "예측 신호 응답을 처리하지 못했습니다.", e);
        } catch (RestClientException e) {
            throw new PredictionServiceException(
                    HttpStatus.SERVICE_UNAVAILABLE, "예측 서비스에 연결할 수 없습니다.", e);
        }
    }

    public IntradayPredictionSignalResponse backtestIntradaySignals(IntradayPredictionSignalRequest request) {
        try {
            IntradayPredictionSignalResponse response = client.post()
                    .uri("/backtest/intraday-prediction-signals")
                    .body(request)
                    .retrieve()
                    .body(IntradayPredictionSignalResponse.class);
            if (response == null) {
                throw new PredictionServiceException(
                        HttpStatus.BAD_GATEWAY, "예측 서비스가 빈 1분봉 신호 응답을 반환했습니다.");
            }
            return response;
        } catch (HttpClientErrorException.NotFound e) {
            throw new PredictionServiceException(
                    HttpStatus.NOT_FOUND, "예측 백테스트에 필요한 1분봉 데이터가 부족합니다.", e);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 422) {
                throw new IllegalArgumentException(
                        "1분봉 예측 신호 요청 조건이 올바르지 않습니다: " + e.getResponseBodyAsString(), e);
            }
            throw new PredictionServiceException(
                    HttpStatus.BAD_GATEWAY, "1분봉 예측 신호 응답을 처리하지 못했습니다.", e);
        } catch (RestClientException e) {
            throw new PredictionServiceException(
                    HttpStatus.SERVICE_UNAVAILABLE, "예측 서비스에 연결할 수 없습니다.", e);
        }
    }

    public static class PredictionServiceException extends RuntimeException {
        private final HttpStatus status;

        public PredictionServiceException(HttpStatus status, String message) {
            super(message);
            this.status = status;
        }

        public PredictionServiceException(HttpStatus status, String message, Throwable cause) {
            super(message, cause);
            this.status = status;
        }

        public HttpStatus getStatus() {
            return status;
        }
    }
}

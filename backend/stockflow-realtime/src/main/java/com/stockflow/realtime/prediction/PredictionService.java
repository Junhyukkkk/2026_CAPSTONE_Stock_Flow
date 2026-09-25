package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
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

@Service
public class PredictionService {

    private final RestClient client;

    public PredictionService(
            RestClient.Builder builder,
            @Value("${analysis.base-url}") String baseUrl,
            @Value("${analysis.connect-timeout-ms:3000}") int connectTimeoutMs,
            @Value("${analysis.read-timeout-ms:120000}") int readTimeoutMs) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        this.client = builder
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }

    public JsonNode compare(String symbol, String interval, int horizon, String source) {
        try {
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

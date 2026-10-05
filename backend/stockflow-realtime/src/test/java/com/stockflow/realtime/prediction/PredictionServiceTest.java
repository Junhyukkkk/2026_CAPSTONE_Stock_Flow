package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
import com.stockflow.realtime.prediction.PredictionService.PredictionServiceException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 실제 로컬 HTTP 서버(JDK HttpServer)를 상대로 오류 매핑을 검증한다. */
class PredictionServiceTest {

    private HttpServer server;
    private PredictionService service;
    private final AtomicReference<String> lastRequest = new AtomicReference<>();
    private volatile int status = 200;
    private volatile String body = "{}";

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            lastRequest.set(exchange.getRequestMethod() + " " + exchange.getRequestURI()
                    + " " + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                exchange.getResponseBody().write(bytes);
            }
            exchange.close();
        });
        server.start();
        service = new PredictionService(RestClient.builder(),
                "http://127.0.0.1:" + server.getAddress().getPort(), 1000, 3000);
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private PredictionServiceException expectFailure(Runnable call) {
        Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(PredictionServiceException.class, call::run);
        return (PredictionServiceException) thrown;
    }

    @Test
    void compareBuildsUriAndParsesJson() {
        body = "{\"best\":\"ARIMA\"}";

        JsonNode node = service.compare("btcusdt", "1d", 5, "binance");

        assertThat(node.get("best").asText()).isEqualTo("ARIMA");
        assertThat(lastRequest.get()).startsWith("GET /predict/BTCUSDT/compare?")
                .contains("interval=1d").contains("horizon=5").contains("source=binance");
    }

    @Test
    void compareOmitsBlankSource() {
        body = "{}";
        service.compare("btcusdt", "1d", 5, " ");
        assertThat(lastRequest.get()).doesNotContain("source=");
        service.compare("btcusdt", "1d", 5, null);
        assertThat(lastRequest.get()).doesNotContain("source=");
    }

    @Test
    void compareMapsErrorStatuses() {
        status = 404;
        assertThat(expectFailure(() -> service.compare("x", "1d", 5, null)).getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
        status = 500;
        assertThat(expectFailure(() -> service.compare("x", "1d", 5, null)).getStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);
    }

    @Test
    void compareReportsEmptyBodyAndUnreachableServer() {
        status = 200;
        body = "";
        assertThat(expectFailure(() -> service.compare("x", "1d", 5, null)).getStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);

        server.stop(0);
        assertThat(expectFailure(() -> service.compare("x", "1d", 5, null)).getStatus())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    private static PredictionSignalRequest dailyRequest() {
        return new PredictionSignalRequest("BTCUSDT", "ARIMA", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 5),
                "BINANCE", 50, 5, 200, 20, 0.5, 10, 5);
    }

    private static IntradayPredictionSignalRequest minuteRequest() {
        Instant t = Instant.parse("2025-01-01T00:00:00Z");
        return new IntradayPredictionSignalRequest("BTCUSDT", "ARIMA", t, t.plusSeconds(60), "BINANCE",
                50, 5, 200, 20, 0.5, 10, 5);
    }

    @Test
    void backtestSignalsPostsRequestAndParsesResponse() {
        body = "{\"symbol\":\"BTCUSDT\",\"model\":\"ARIMA\",\"from_date\":\"2025-01-01\",\"to_date\":\"2025-01-05\","
                + "\"signal_count\":1,\"buy_count\":1,\"hold_count\":0,\"sell_count\":0,\"signals\":[]}";

        PredictionSignalResponse response = service.backtestSignals(dailyRequest());

        assertThat(response.buyCount()).isEqualTo(1);
        assertThat(lastRequest.get()).startsWith("POST /backtest/prediction-signals ");
    }

    @Test
    void backtestSignalsMapsErrors() {
        status = 404;
        assertThat(expectFailure(() -> service.backtestSignals(dailyRequest())).getStatus()).isEqualTo(HttpStatus.NOT_FOUND);

        status = 422;
        body = "{\"detail\":\"bad\"}";
        assertThatThrownBy(() -> service.backtestSignals(dailyRequest()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bad");

        status = 400;
        assertThat(expectFailure(() -> service.backtestSignals(dailyRequest())).getStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);

        status = 200;
        body = "null";
        assertThat(expectFailure(() -> service.backtestSignals(dailyRequest())).getStatus()).isEqualTo(HttpStatus.BAD_GATEWAY);

        server.stop(0);
        assertThat(expectFailure(() -> service.backtestSignals(dailyRequest())).getStatus())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void backtestIntradaySignalsPostsRequestAndMapsErrors() {
        body = "{\"symbol\":\"BTCUSDT\",\"model\":\"ARIMA\",\"signal_count\":0,\"buy_count\":0,\"hold_count\":0,"
                + "\"sell_count\":0,\"signals\":[]}";
        assertThat(service.backtestIntradaySignals(minuteRequest()).symbol()).isEqualTo("BTCUSDT");
        assertThat(lastRequest.get()).startsWith("POST /backtest/intraday-prediction-signals ");

        status = 404;
        assertThat(expectFailure(() -> service.backtestIntradaySignals(minuteRequest())).getStatus())
                .isEqualTo(HttpStatus.NOT_FOUND);
        status = 422;
        assertThatThrownBy(() -> service.backtestIntradaySignals(minuteRequest()))
                .isInstanceOf(IllegalArgumentException.class);
        status = 400;
        assertThat(expectFailure(() -> service.backtestIntradaySignals(minuteRequest())).getStatus())
                .isEqualTo(HttpStatus.BAD_GATEWAY);
        // 5xx 는 HttpClientErrorException 이 아니므로 RestClientException 분기(503)로 매핑된다.
        status = 500;
        assertThat(expectFailure(() -> service.backtestIntradaySignals(minuteRequest())).getStatus())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        status = 200;
        body = "null";
        assertThat(expectFailure(() -> service.backtestIntradaySignals(minuteRequest())).getStatus())
                .isEqualTo(HttpStatus.BAD_GATEWAY);
        server.stop(0);
        assertThat(expectFailure(() -> service.backtestIntradaySignals(minuteRequest())).getStatus())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }
}

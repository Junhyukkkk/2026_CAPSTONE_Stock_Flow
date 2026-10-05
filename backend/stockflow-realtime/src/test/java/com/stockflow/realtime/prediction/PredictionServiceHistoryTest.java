package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.prediction.PredictionService.PredictionServiceException;
import com.stockflow.realtime.stock.SymbolSourceResolver;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PredictionServiceHistoryTest {

    private static final String BODY =
            "{\"symbol\":\"BTCUSDT\",\"last_ts\":\"2026-10-05T03:07:00+00:00\",\"last_value\":1.0,\"models\":[]}";

    private HttpServer server;
    private final AtomicInteger status = new AtomicInteger(200);
    private volatile String body = BODY;
    private PredictionHistoryRecorder recorder;
    private SymbolSourceResolver resolver;
    private volatile String lastQuery;

    @BeforeEach
    void setUp() throws Exception {
        recorder = mock(PredictionHistoryRecorder.class);
        resolver = mock(SymbolSourceResolver.class);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/predict", exchange -> {
            lastQuery = exchange.getRequestURI().getRawQuery();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            if (bytes.length == 0) {
                exchange.sendResponseHeaders(status.get(), -1);
            } else {
                exchange.sendResponseHeaders(status.get(), bytes.length);
                exchange.getResponseBody().write(bytes);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private PredictionService service(boolean historyEnabled) {
        return new PredictionService(RestClient.builder(),
                "http://127.0.0.1:" + server.getAddress().getPort(), 1000, 5000,
                recorder, historyEnabled, resolver);
    }

    @Test
    void 플래그가_false면_recorder를_호출하지_않는다() {
        JsonNode result = service(false).compare("btcusdt", "1m", 3, null);

        assertThat(result.path("symbol").asText()).isEqualTo("BTCUSDT");
        verifyNoInteractions(recorder);
    }

    @Test
    void 정상_응답이면_반환값은_그대로이고_recorder에_한번_전달된다() throws Exception {
        JsonNode result = service(true).compare("btcusdt", "1m", 3, "binance");

        assertThat(result).isEqualTo(new ObjectMapper().readTree(BODY));
        verify(recorder).record(eq("btcusdt"), eq("1m"), eq(3), eq("binance"), eq(result), anyLong());
    }

    @Test
    void recorder가_예외를_던져도_응답은_정상_반환된다() {
        doThrow(new IllegalStateException("boom"))
                .when(recorder).record(any(), any(), anyInt(), any(), any(), anyLong());

        JsonNode result = service(true).compare("btcusdt", "1m", 3, null);

        assertThat(result.path("symbol").asText()).isEqualTo("BTCUSDT");
    }

    @Test
    void 분석_서버_오류_경로에서는_recorder를_호출하지_않고_예외_매핑이_유지된다() {
        PredictionService service = service(true);

        status.set(404);
        assertThatThrownBy(() -> service.compare("btcusdt", "1m", 3, null))
                .isInstanceOfSatisfying(PredictionServiceException.class,
                        e -> assertThat(e.getStatus()).isEqualTo(HttpStatus.NOT_FOUND));

        status.set(500);
        assertThatThrownBy(() -> service.compare("btcusdt", "1m", 3, null))
                .isInstanceOfSatisfying(PredictionServiceException.class,
                        e -> assertThat(e.getStatus()).isEqualTo(HttpStatus.BAD_GATEWAY));

        status.set(200);
        body = "";
        assertThatThrownBy(() -> service.compare("btcusdt", "1m", 3, null))
                .isInstanceOfSatisfying(PredictionServiceException.class,
                        e -> assertThat(e.getStatus()).isEqualTo(HttpStatus.BAD_GATEWAY));

        verify(recorder, never()).record(any(), any(), anyInt(), any(), any(), anyLong());
    }

    @Test
    void source가_없으면_서버가_고른_출처가_분석_호출과_이력에_쓰인다() {
        when(resolver.latestSource("aapl")).thenReturn(Optional.of("ALPACA"));

        JsonNode result = service(true).compare("aapl", "1m", 3, null);

        assertThat(lastQuery).contains("source=ALPACA");
        verify(recorder).record(eq("aapl"), eq("1m"), eq(3), eq("ALPACA"), eq(result), anyLong());
    }

    @Test
    void 명시한_source는_resolver를_거치지_않고_우선한다() {
        JsonNode result = service(true).compare("aapl", "1m", 3, "SIMULATOR");

        assertThat(lastQuery).contains("source=SIMULATOR");
        verifyNoInteractions(resolver);
        verify(recorder).record(eq("aapl"), eq("1m"), eq(3), eq("SIMULATOR"), eq(result), anyLong());
    }

    @Test
    void resolver가_비어_있으면_기존처럼_출처_없이_호출한다() {
        when(resolver.latestSource("aapl")).thenReturn(Optional.empty());

        JsonNode result = service(true).compare("aapl", "1m", 3, " ");

        assertThat(lastQuery).doesNotContain("source");
        verify(recorder).record(eq("aapl"), eq("1m"), eq(3), eq(" "), eq(result), anyLong());
    }

    @Test
    void 일봉은_출처를_자동으로_정하지_않는다() {
        service(true).compare("aapl", "1d", 3, null);

        assertThat(lastQuery).doesNotContain("source");
        verifyNoInteractions(resolver);
    }
}

package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class PredictionControllerHistoryTest {

    private final PredictionHistoryRepository repository = mock(PredictionHistoryRepository.class);
    private final PredictionService service = mock(PredictionService.class);

    /** Spring Boot 기본값(날짜를 ISO 문자열로)과 같은 직렬화 설정. */
    private MockMvc mvc(boolean enabled) {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return MockMvcBuilders.standaloneSetup(controller(enabled))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(mapper))
                .build();
    }

    private PredictionController controller(boolean enabled) {
        return new PredictionController(service, repository, enabled);
    }

    @Test
    void 심볼을_대문자로_바꾸고_기본_limit_20으로_조회한다() throws Exception {
        when(repository.findRecent(anyString(), anyString(), anyInt())).thenReturn(List.of());
        MockMvc mvc = mvc(true);

        mvc.perform(get("/api/predictions/btcusdt/history"))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));

        verify(repository).findRecent("BTCUSDT", "1m", 20);
    }

    @Test
    void interval과_limit을_그대로_전달하고_응답_JSON을_만든다() throws Exception {
        PredictionHistoryResponse run = new PredictionHistoryResponse(
                7L, "BTCUSDT", "1d", 3, "", Instant.parse("2026-10-05T00:00:00Z"), 100.5,
                Instant.parse("2026-10-05T03:11:00Z"), 42,
                List.of(new PredictionHistoryResponse.ModelForecast("ARIMA", null, 1.5, 2.5, List.of(
                        new PredictionHistoryResponse.Point(Instant.parse("2026-10-06T00:00:00Z"), 101.0, null, 102.0)))));
        when(repository.findRecent("BTCUSDT", "1d", 5)).thenReturn(List.of(run));
        MockMvc mvc = mvc(true);

        mvc.perform(get("/api/predictions/BTCUSDT/history").param("interval", "1d").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].runId").value(7))
                .andExpect(jsonPath("$[0].baseTs").value("2026-10-05T00:00:00Z"))
                .andExpect(jsonPath("$[0].latencyMs").value(42))
                .andExpect(jsonPath("$[0].models[0].model").value("ARIMA"))
                .andExpect(jsonPath("$[0].models[0].mae").value(1.5))
                .andExpect(jsonPath("$[0].models[0].points[0].yhat").value(101.0))
                .andExpect(jsonPath("$[0].models[0].points[0].yhatUpper").value(102.0));
    }

    @Test
    void 저장_기능이_꺼져_있으면_저장소를_조회하지_않고_빈_배열을_반환한다() throws Exception {
        MockMvc mvc = mvc(false);

        mvc.perform(get("/api/predictions/BTCUSDT/history"))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));

        verifyNoInteractions(repository);
    }

    /** 컨트롤러의 @Validated 는 AOP 프록시로 동작하므로 실제 프록시를 만들어 검증한다. */
    private PredictionController validatedProxy() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.registerBean(MethodValidationPostProcessor.class);
        ctx.registerBean(PredictionController.class, () -> controller(true));
        ctx.refresh();
        return ctx.getBean(PredictionController.class);
    }

    @Test
    void limit은_1에서_200_사이여야_한다() {
        PredictionController proxy = validatedProxy();

        assertThatThrownBy(() -> proxy.history("BTCUSDT", "1m", 0))
                .isInstanceOf(ConstraintViolationException.class);
        assertThatThrownBy(() -> proxy.history("BTCUSDT", "1m", 201))
                .isInstanceOf(ConstraintViolationException.class);
        assertThat(proxy.history("BTCUSDT", "1m", 1)).isEmpty();
        assertThat(proxy.history("BTCUSDT", "1m", 200)).isEmpty();
    }

    @Test
    void interval은_1m_또는_1d만_허용한다() {
        PredictionController proxy = validatedProxy();

        assertThatThrownBy(() -> proxy.history("BTCUSDT", "5m", 20))
                .isInstanceOf(ConstraintViolationException.class);
        assertThat(proxy.history("BTCUSDT", "1d", 20)).isEmpty();
    }
}

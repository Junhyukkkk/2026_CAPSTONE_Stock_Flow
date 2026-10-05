package com.stockflow.realtime.backtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.backtest.dto.StrategyRequest;
import com.stockflow.realtime.backtest.dto.StrategyResponse;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BacktestStrategyServiceTest {

    @Mock BacktestStrategyRepository repository;
    @InjectMocks BacktestStrategyService service;

    private static StrategyRequest request(String json) {
        try {
            return new ObjectMapper().readValue(json, StrategyRequest.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static StrategyRow row(long id) {
        return new StrategyRow(id, "name", "BTCUSDT", "RSI", Map.of("period", 14),
                BigDecimal.valueOf(10000), Instant.EPOCH, Instant.EPOCH);
    }

    @Test
    void createUsesDefaultInitialCash() {
        when(repository.insert(eq("n"), eq("btc"), eq(StrategyType.RSI), any(), eq(BigDecimal.valueOf(10000))))
                .thenReturn(row(1));

        StrategyResponse response = service.create(request(
                "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"rsi\"}"));

        assertThat(response.getId()).isEqualTo(1L);
        assertThat(response.getStrategyType()).isEqualTo("RSI");
        assertThat(response.getParams()).containsEntry("period", 14);
    }

    @Test
    void createValidatesStrategyAndParameters() {
        assertThatThrownBy(() -> service.create(request(
                "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"RSI\",\"initialCash\":0}")))
                .hasMessageContaining("initialCash must be positive");
        assertThatThrownBy(() -> service.create(request(
                "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"MA_CROSSOVER\",\"params\":{\"shortPeriod\":9,\"longPeriod\":3}}")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create(request(
                "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"PREDICTION\",\"params\":{\"model\":\"X\"}}")))
                .hasMessageContaining("model must be");
        assertThatThrownBy(() -> service.create(request(
                "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"???\"}")))
                .hasMessageContaining("Unknown strategyType");
        verify(repository, never()).insert(any(), any(), any(), any(), any());
    }

    @Test
    void createAcceptsPredictionStrategy() {
        when(repository.insert(any(), any(), eq(StrategyType.PREDICTION), any(), any())).thenReturn(row(2));

        assertThat(service.create(request(
                "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"PREDICTION\",\"initialCash\":500}")).getId())
                .isEqualTo(2L);
    }

    @Test
    void listGetUpdateDelete() {
        when(repository.findAll("BTC")).thenReturn(List.of(row(1), row(2)));
        when(repository.findById(1L)).thenReturn(Optional.of(row(1)));
        when(repository.findById(9L)).thenReturn(Optional.empty());
        when(repository.update(eq(1L), any(), any(), any(), any(), any())).thenReturn(Optional.of(row(1)));
        when(repository.update(eq(9L), any(), any(), any(), any(), any())).thenReturn(Optional.empty());
        when(repository.delete(1L)).thenReturn(true);
        when(repository.delete(9L)).thenReturn(false);

        assertThat(service.list("BTC")).hasSize(2);
        assertThat(service.get(1L)).isPresent();
        assertThat(service.get(9L)).isEmpty();
        String body = "{\"name\":\"n\",\"symbol\":\"btc\",\"strategyType\":\"BUY_AND_HOLD\",\"initialCash\":100}";
        assertThat(service.update(1L, request(body))).isPresent();
        assertThat(service.update(9L, request(body))).isEmpty();
        assertThat(service.delete(1L)).isTrue();
        assertThat(service.delete(9L)).isFalse();
    }
}

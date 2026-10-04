package com.stockflow.realtime.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.core.metrics.PipelineStageMetrics;
import com.stockflow.core.metrics.PipelineStageMetrics.Stage;
import com.stockflow.realtime.redis.PriceKeys;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis Pub/Sub 메시지 리스너
 *
 * Redis 채널(price:{symbol})에서 메시지를 받아
 * WebSocket(/topic/price/{symbol})으로 전달
 *
 * 계측:
 * - stockflow_stage_seconds{stage="ws.dispatch"}: WebSocket 송출에 걸린 시간
 * - stockflow_e2e_latency_websocket: 거래소 ts -> WebSocket 송출까지의 E2E
 *   (RedisPriceService 시점의 stockflow_e2e_latency와 비교하면 PUBLISH->WS 홉 비용이 나온다)
 * - stockflow_ws_dispatch_threads: 디스패치에 사용된 서로 다른 스레드 수
 *   (고정 스레드풀(stockflow.opt.ws-task-executor, 기본 켜짐)이 적용됐는지 확인하는 값
 *    — 끄면 메시지마다 스레드가 새로 생성돼 이 수가 계속 는다)
 */
@Slf4j
@Component
public class RedisMessageListener implements MessageListener {

    private final SimpMessagingTemplate messagingTemplate;
    private final PipelineStageMetrics stageMetrics;
    private final ObjectMapper objectMapper;

    private final DistributionSummary e2eAtWebsocket;
    private final Set<String> dispatchThreads = ConcurrentHashMap.newKeySet();

    // N 이면 N번째 메시지마다 1번만 E2E 를 측정한다 (1 이하 = 매번, 기존 동작)
    private final long e2eSampleEvery;
    private final AtomicLong e2eSampleCounter = new AtomicLong();

    public RedisMessageListener(
            SimpMessagingTemplate messagingTemplate,
            PipelineStageMetrics stageMetrics,
            ObjectMapper objectMapper,
            MeterRegistry registry,
            @Value("${stockflow.e2e.sample-every:1}") long e2eSampleEvery) {
        this.e2eSampleEvery = e2eSampleEvery;
        this.messagingTemplate = messagingTemplate;
        this.stageMetrics = stageMetrics;
        this.objectMapper = objectMapper;

        this.e2eAtWebsocket = DistributionSummary.builder("stockflow.e2e.latency.websocket")
                .description("End-to-end latency measured at WebSocket send, in milliseconds")
                .baseUnit("ms")
                .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                .register(registry);

        Gauge.builder("stockflow.ws.dispatch.threads", dispatchThreads, Set::size)
                .description("Distinct threads used to dispatch Redis Pub/Sub messages to WebSocket")
                .register(registry);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String channel = new String(message.getChannel());
            String body = new String(message.getBody());

            // 채널에서 symbol 추출 (price:AAPL -> AAPL)
            String symbol = PriceKeys.symbolFromChannel(channel);
            if (symbol == null) {
                log.warn("Invalid channel format: {}", channel);
                return;
            }

            dispatchThreads.add(Thread.currentThread().getName());

            // WebSocket으로 전달
            String destination = PriceKeys.wsPriceTopic(symbol);
            long dispatchStart = stageMetrics.start();
            messagingTemplate.convertAndSend(destination, body);
            stageMetrics.record(Stage.WS_DISPATCH, dispatchStart);

            // 송출 완료 후 계측 (측정 비용이 ws.dispatch에 섞이지 않도록 밖에서 수행)
            if (shouldSampleE2E()) {
                recordE2ELatency(body);
            }

            log.trace("Forwarded to WebSocket: channel={}, destination={}", channel, destination);

        } catch (Exception e) {
            log.error("Failed to process Redis message", e);
        }
    }

    private boolean shouldSampleE2E() {
        // 균등 추출이라 샘플링해도 지연 분포(p50~p99)는 유지된다 (count 만 1/N)
        return e2eSampleEvery <= 1 || e2eSampleCounter.incrementAndGet() % e2eSampleEvery == 0;
    }

    private void recordE2ELatency(String body) {
        try {
            JsonNode node = objectMapper.readTree(body);
            long timestamp = node.path("timestamp").asLong(0);
            if (timestamp <= 0) {
                return;
            }
            long latency = System.currentTimeMillis() - PerformanceMetrics.normalizeToMillis(timestamp);
            e2eAtWebsocket.record(latency);
        } catch (Exception e) {
            log.trace("Failed to record WebSocket E2E latency", e);
        }
    }
}

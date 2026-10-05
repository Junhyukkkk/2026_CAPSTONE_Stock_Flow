package com.stockflow.core.metrics;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineStageMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final PipelineStageMetrics metrics = new PipelineStageMetrics(registry);

    private Timer timer(String stage) {
        return registry.get("stockflow.stage").tag("stage", stage).timer();
    }

    @Test
    void recordStoresOneSamplePerCall() {
        metrics.record(PipelineStageMetrics.Stage.REDIS_SET_LATEST, metrics.start());
        metrics.record(PipelineStageMetrics.Stage.REDIS_SET_LATEST, metrics.start());

        assertThat(timer(PipelineStageMetrics.Stage.REDIS_SET_LATEST).count()).isEqualTo(2);
    }

    @Test
    void timeSupplierReturnsValueAndRecords() {
        String result = metrics.time(PipelineStageMetrics.Stage.SNAPSHOT_SERIALIZE, () -> "ok");

        assertThat(result).isEqualTo("ok");
        assertThat(timer(PipelineStageMetrics.Stage.SNAPSHOT_SERIALIZE).count()).isEqualTo(1);
    }

    @Test
    void timeRunnableRunsAndRecords() {
        AtomicBoolean ran = new AtomicBoolean();

        metrics.time(PipelineStageMetrics.Stage.STORAGE_DB_INSERT, () -> ran.set(true));

        assertThat(ran).isTrue();
        assertThat(timer(PipelineStageMetrics.Stage.STORAGE_DB_INSERT).count()).isEqualTo(1);
    }

    @Test
    void recordsEvenWhenOperationThrows() {
        assertThatThrownBy(() -> metrics.time(PipelineStageMetrics.Stage.REDIS_PUBLISH, () -> {
            throw new IllegalStateException("fail");
        })).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> metrics.time(PipelineStageMetrics.Stage.REDIS_PUBLISH, (java.util.function.Supplier<String>) () -> {
            throw new IllegalStateException("fail");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(timer(PipelineStageMetrics.Stage.REDIS_PUBLISH).count()).isEqualTo(2);
    }
}

package com.stockflow.realtime.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 성능 최적화 토글은 실측으로 검증된 뒤 운영 기본값이 됐다
 * (부하 테스트 보고서 "개선 내용": 스위치가 있었으나 어느 배포에서도 켜진 적이 없었음).
 * 기본값이 다시 false 로 돌아가 조용히 느린 경로로 배포되는 일을 막는다.
 */
class OptimizationPropertiesTest {

    @Test
    void allOptimizationsAreOnByDefault() {
        OptimizationProperties opt = new OptimizationProperties();

        assertThat(opt.isInstrumentCache()).as("instrumentCache").isTrue();
        assertThat(opt.isPrevCloseLocalCache()).as("prevCloseLocalCache").isTrue();
        assertThat(opt.isRedisPipeline()).as("redisPipeline").isTrue();
        assertThat(opt.isStorageIdempotencyPipeline()).as("storageIdempotencyPipeline").isTrue();
        assertThat(opt.isWsTaskExecutor()).as("wsTaskExecutor").isTrue();
    }

    @Test
    void refreshAndTtlDefaultsAreOneMinute() {
        OptimizationProperties opt = new OptimizationProperties();

        assertThat(opt.getInstrumentCacheRefreshMs()).isEqualTo(60_000L);
        assertThat(opt.getPrevCloseLocalCacheTtlMs()).isEqualTo(60_000L);
    }
}

package com.stockflow.realtime.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 성능 개선안 토글
 *
 * 병목 검증 실험에서 개선안을 하나씩 켜고 끄며 효과를 분리 측정하기 위한 플래그.
 * 기본값은 모두 false — 켜지 않으면 기존 동작과 동일하다.
 *
 * 환경변수: STOCKFLOW_OPT_INSTRUMENT_CACHE=true 형태
 */
@Component
@ConfigurationProperties(prefix = "stockflow.opt")
@Data
public class OptimizationProperties {

    /**
     * register_instrument 호출을 결과셋을 받는 방식으로 처리한다.
     *
     * false 로 두면 jdbcTemplate.update() 로 호출하던 기존 동작을 재현한다.
     * SELECT 는 항상 결과셋을 반환하므로 update() 는 호출마다 예외를 던진다
     * (함수는 실행되지만 예외 생성 + WARN 로깅 비용이 매번 발생).
     * 개선 전후 비교 측정을 위해 남겨둔 플래그이며 기본값은 정상 동작이다.
     */
    private boolean instrumentRegistryFix = true;

    /** instruments 마스터 재등록을 메모리 캐시로 건너뛴다 */
    private boolean instrumentCache = false;

    /** 재등록 주기 (last_seen_at 갱신 간격) */
    private long instrumentCacheRefreshMs = 60_000L;

    /** 전일 종가를 로컬 캐시에서 읽는다 (miss도 캐싱) */
    private boolean prevCloseLocalCache = false;

    /** 전일 종가 로컬 캐시 TTL */
    private long prevCloseLocalCacheTtlMs = 60_000L;

    /** 최신가 SET 과 Pub/Sub PUBLISH 를 파이프라인 1회 왕복으로 묶는다 */
    private boolean redisPipeline = false;

    /** 저장 경로 멱등성 체크/마킹을 배치 파이프라인으로 묶는다 */
    private boolean storageIdempotencyPipeline = false;

    /** Redis Pub/Sub 리스너에 고정 스레드풀을 지정한다 (메시지당 스레드 생성 방지) */
    private boolean wsTaskExecutor = false;
}

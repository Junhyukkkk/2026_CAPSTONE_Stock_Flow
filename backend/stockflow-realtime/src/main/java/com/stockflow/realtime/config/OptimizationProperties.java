package com.stockflow.realtime.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 성능 개선안 토글
 *
 * 병목 검증 실험에서 개선안을 하나씩 켜고 끄며 효과를 분리 측정하기 위한 플래그.
 * 전부 실측으로 검증돼 운영 기본값이 됐으므로 기본값은 모두 true 다
 * (부하 테스트 보고서 2026-09-08 "개선 내용" 3·4·5번). 개선 전 동작과 비교 측정할 때만
 * 개별 플래그를 false 로 내린다.
 *
 * 환경변수: STOCKFLOW_OPT_INSTRUMENT_CACHE=false 형태
 */
@Component
@ConfigurationProperties(prefix = "stockflow.opt")
@Data
public class OptimizationProperties {

    /** instruments 마스터 재등록을 메모리 캐시로 건너뛴다 */
    private boolean instrumentCache = true;

    /** 재등록 주기 (last_seen_at 갱신 간격) */
    private long instrumentCacheRefreshMs = 60_000L;

    /** 전일 종가를 로컬 캐시에서 읽는다 (miss도 캐싱) */
    private boolean prevCloseLocalCache = true;

    /** 전일 종가 로컬 캐시 TTL */
    private long prevCloseLocalCacheTtlMs = 60_000L;

    /** 최신가 SET 과 Pub/Sub PUBLISH 를 파이프라인 1회 왕복으로 묶는다 */
    private boolean redisPipeline = true;

    /** 저장 경로 멱등성 체크/마킹을 배치 파이프라인으로 묶는다 */
    private boolean storageIdempotencyPipeline = true;

    /** Redis Pub/Sub 리스너에 고정 스레드풀을 지정한다 (메시지당 스레드 생성 방지) */
    private boolean wsTaskExecutor = true;
}

package com.stockflow.realtime.transaction;

/**
 * Kafka consumer 그룹별 Redis 멱등 키 네임스페이스.
 * realtime / storage 가 같은 키를 쓰면 한쪽이 먼저 처리한 뒤 다른 쪽이 전부 스킵되거나 경쟁 상태가 난다.
 */
public final class IdempotencyChannels {

    public static final String REALTIME = "realtime";
    public static final String STORAGE = "storage";

    private IdempotencyChannels() {
    }
}

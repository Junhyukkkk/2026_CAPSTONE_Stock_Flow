package com.stockflow.realtime.config;

import com.stockflow.core.retry.RetryPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RetryPolicyConfig {

    @Value("${retry.initial-delay-ms:1000}")
    private long initialDelayMs;

    @Value("${retry.multiplier:2.0}")
    private double multiplier;

    @Value("${retry.max-retries:3}")
    private int maxRetries;

    @Value("${retry.max-delay-ms:60000}")
    private long maxDelayMs;

    @Value("${retry.use-jitter:true}")
    private boolean useJitter;

    @Value("${retry.jitter-ratio:0.2}")
    private double jitterRatio;

    @Bean
    public RetryPolicy retryPolicy() {
        return RetryPolicy.builder()
                .initialDelayMs(initialDelayMs)
                .multiplier(multiplier)
                .maxRetries(maxRetries)
                .maxDelayMs(maxDelayMs)
                .useJitter(useJitter)
                .jitterRatio(jitterRatio)
                .build();
    }
}

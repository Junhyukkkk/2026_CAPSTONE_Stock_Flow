package com.stockflow.realtime.prediction;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class PredictionHistoryConfig {

    /** 큐가 차면 AbortPolicy 로 RejectedExecutionException 이 나고, Recorder 가 이를 drop 으로 처리한다. */
    @Bean(name = "predictionHistoryExecutor", destroyMethod = "shutdown")
    public ThreadPoolExecutor predictionHistoryExecutor(
            @Value("${prediction.history.threads:1}") int threads,
            @Value("${prediction.history.queue-capacity:1000}") int queueCapacity) {
        AtomicInteger seq = new AtomicInteger();
        return new ThreadPoolExecutor(
                threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread thread = new Thread(runnable, "prediction-history-" + seq.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }
}

package com.stockflow.realtime;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.stockflow")
@EnableScheduling
public class StockFlowRealtimeApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockFlowRealtimeApplication.class, args);
    }
}

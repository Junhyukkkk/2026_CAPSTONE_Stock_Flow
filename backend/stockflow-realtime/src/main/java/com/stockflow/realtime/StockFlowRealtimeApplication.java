package com.stockflow.realtime;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.stockflow")
public class StockFlowRealtimeApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockFlowRealtimeApplication.class, args);
    }
}

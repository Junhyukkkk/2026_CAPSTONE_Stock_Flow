package com.stockflow.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.stockflow")
public class StockFlowBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockFlowBatchApplication.class, args);
    }
}

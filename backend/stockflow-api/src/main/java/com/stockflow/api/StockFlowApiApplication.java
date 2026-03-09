package com.stockflow.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.stockflow")
public class StockFlowApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockFlowApiApplication.class, args);
    }
}

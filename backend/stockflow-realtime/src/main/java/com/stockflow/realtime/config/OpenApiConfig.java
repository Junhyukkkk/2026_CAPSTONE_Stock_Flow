package com.stockflow.realtime.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI stockFlowOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("StockFlow API")
                        .description("실시간 주식/암호화폐 데이터 스트리밍 플랫폼 REST API")
                        .version("v1.0.0"));
    }
}

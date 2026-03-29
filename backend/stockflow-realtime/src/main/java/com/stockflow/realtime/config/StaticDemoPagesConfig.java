package com.stockflow.realtime.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 정적 데모 HTML을 브라우저에서 찾기 쉽게 하는 리다이렉트.
 */
@Configuration
public class StaticDemoPagesConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/storage-overview", "/storage-overview.html");
    }
}

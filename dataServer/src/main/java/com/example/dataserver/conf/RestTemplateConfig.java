package com.example.dataserver.conf;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestTemplateConfig {
    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(5000); // 连接超时（5s，大于文档心跳超时30s）
        requestFactory.setReadTimeout(10000); // 读取超时（10s）
        return new RestTemplate(requestFactory);
    }
}
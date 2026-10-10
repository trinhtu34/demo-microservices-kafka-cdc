package com.demo.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfiguration {

    @Bean
    CommonErrorHandler kafkaDatabaseRetryHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}

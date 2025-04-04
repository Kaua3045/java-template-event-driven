package com.kaua.event.driven.infrastructure.configurations;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@ComponentScan(basePackages = "com.kaua.event.driven")
@EnableJpaRepositories(basePackages = "com.kaua.event.driven.infrastructure")
@EnableScheduling
public class WebServerConfig {
}

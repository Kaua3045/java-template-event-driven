package com.kaua.event.driven.infrastructure.configurations;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration(proxyBeanMethods = false)
@ComponentScan(basePackages = "com.kaua.event.driven")
@EnableJpaRepositories(basePackages = "com.kaua.event.driven.infrastructure")
public class WebServerConfig {
}

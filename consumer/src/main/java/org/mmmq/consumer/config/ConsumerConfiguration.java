package org.mmmq.consumer.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan(basePackages = "org.mmmq.consumer")
@EnableConfigurationProperties(ConsumerProperties.class)
public class ConsumerConfiguration {
}

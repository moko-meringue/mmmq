package org.mmmq.broker.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@EnableConfigurationProperties({
        StorageProperties.class,
        SegmentProperties.class
})
@ComponentScan(basePackages = "org.mmmq.broker")
class BrokerConfiguration {

}

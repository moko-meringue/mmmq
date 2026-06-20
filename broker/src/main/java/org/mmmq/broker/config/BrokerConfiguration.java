package org.mmmq.broker.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@EnableConfigurationProperties({
        StorageProperties.class,
        SegmentProperties.class
})
@Import(DispatcherBeanRegistrar.class)
@ComponentScan(basePackages = "org.mmmq.broker")
class BrokerConfiguration {

}

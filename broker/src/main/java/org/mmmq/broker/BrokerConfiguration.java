package org.mmmq.broker;

import org.mmmq.broker.persistence.PersistenceProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan(basePackages = "org.mmmq.broker")
@EnableConfigurationProperties(PersistenceProperties.class)
class BrokerConfiguration {

}

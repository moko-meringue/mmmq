package org.mmmq.broker.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@EnableConfigurationProperties({
        StorageProperties.class,
        SegmentProperties.class
}) // 두 properties 레코드를 @ConfigurationProperties 빈으로 등록
@ComponentScan(basePackages = "org.mmmq.broker") // org.mmmq.broker 패키지 하위의 @Component 클래스를 Spring 빈으로 등록
class BrokerConfiguration {

}

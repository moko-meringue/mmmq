package org.mmmq.broker.dispatcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mmmq.broker.persistence.PersistenceProperties;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

public class DispatcherBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(DispatcherBeanRegistrar.class);

    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        Path path = resolveDispatchersFile();

        if (!Files.exists(path)) {
            createEmptyFile(path);
            log.info("Dispatcher file not found. Created empty file at {}.", path);
        }

        List<DispatcherDefinition> definitions = readDefinitions(path);

        Set<String> seen = new HashSet<>();
        definitions.forEach(definition -> {
            if (!seen.add(definition.consumerId())) {
                throw new IllegalStateException(
                        "Duplicate consumerId '" + definition.consumerId() + "' in dispatcher file: " + path);
            }
        });

        definitions.forEach(definition -> register(definition, registry));
    }

    private Path resolveDispatchersFile() {
        // ConfigurationProperties 빈 바인딩 이전 단계이므로 Binder로 동일 레코드를 직접 바인딩한다.
        return Binder.get(environment)
                .bind(PersistenceProperties.PREFIX, PersistenceProperties.class)
                .orElseGet(() -> new PersistenceProperties(null, null))
                .dispatchersFile();
    }

    private void createEmptyFile(Path path) {
        try {
            Files.createDirectories(path.toAbsolutePath().getParent());
            Files.writeString(path, "[]");
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create dispatcher file: " + path, exception);
        }
    }

    private void register(DispatcherDefinition definition, BeanDefinitionRegistry registry) {
        AbstractBeanDefinition beanDefinition = BeanDefinitionBuilder
                .genericBeanDefinition(Dispatcher.class)
                .addConstructorArgValue(definition.toHost())
                .addConstructorArgValue(new ConsumerId(definition.consumerId()))
                .addConstructorArgValue(new TopicPattern(definition.pattern()))
                .getBeanDefinition();
        BeanDefinitionReaderUtils.registerWithGeneratedName(beanDefinition, registry);
    }

    private List<DispatcherDefinition> readDefinitions(Path path) {
        try {
            DispatcherDefinition[] definitions = new ObjectMapper()
                    .readValue(Files.readAllBytes(path), DispatcherDefinition[].class);
            return List.of(definitions);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read dispatcher file: " + path, exception);
        }
    }
}

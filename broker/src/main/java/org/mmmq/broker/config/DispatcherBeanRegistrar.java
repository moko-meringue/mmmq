package org.mmmq.broker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.DispatcherDefinition;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

class DispatcherBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(DispatcherBeanRegistrar.class);

    private static final String FILE_PROPERTY = "mmmq.broker.dispatchers.file";
    private static final String DEFAULT_FILE = "./dispatchers.json";

    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        Path path = Path.of(environment.getProperty(FILE_PROPERTY, DEFAULT_FILE));

        ensureExists(path);

        List<DispatcherDefinition> definitions = readDefinitions(path);

        Set<String> consumerIds = new HashSet<>();
        definitions.forEach(definition -> {
            if (!consumerIds.add(definition.consumerId())) {
                throw new IllegalStateException(
                        "Duplicate consumerId '" + definition.consumerId() + "' in dispatcher file: " + path);
            }
        });

        definitions.forEach(definition -> register(definition, registry));
    }

    private void register(DispatcherDefinition definition, BeanDefinitionRegistry registry) {
        BeanDefinitionReaderUtils.registerWithGeneratedName(
                BeanDefinitionBuilder.genericBeanDefinition(Dispatcher.class)
                        .addConstructorArgValue(definition.host().toHost())
                        .addConstructorArgValue(new ConsumerId(definition.consumerId()))
                        .addConstructorArgValue(new TopicPattern(definition.pattern()))
                        .getBeanDefinition(),
                registry);
    }

    private void ensureExists(Path path) {
        if (Files.exists(path)) {
            return;
        }
        try {
            Files.writeString(path, "[]");
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create dispatcher file: " + path, exception);
        }
        log.info("Dispatcher file not found. Created empty file at {}.", path);
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

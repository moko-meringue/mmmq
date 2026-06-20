package org.mmmq.broker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.DispatcherDefinition;
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

        if (!Files.exists(path)) {
            log.warn("Dispatcher file not found at {}. No dispatchers registered.", path);
            return;
        }

        readDispatchers(path).forEach(dispatcher ->
                BeanDefinitionReaderUtils.registerWithGeneratedName(
                        BeanDefinitionBuilder.genericBeanDefinition(Dispatcher.class, () -> dispatcher)
                                .getBeanDefinition(),
                        registry
                ));
    }

    private List<Dispatcher> readDispatchers(Path path) {
        try {
            DispatcherDefinition[] definitions = new ObjectMapper()
                    .readValue(Files.readAllBytes(path), DispatcherDefinition[].class);
            return Arrays.stream(definitions)
                    .map(DispatcherDefinition::toDispatcher)
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read dispatcher file: " + path, exception);
        }
    }
}

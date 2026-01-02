package org.mmmq.broker.dispatcher.binding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Topic;

import static org.assertj.core.api.Assertions.assertThat;

class BindingCacheTest {

    BindingCache bindingCache;

    @BeforeEach
    void setUp() {
        bindingCache = new BindingCache();
    }

    @Test
    @DisplayName("캐시에 데이터를 삽입할 수 있다")
    void putTest() {
        var topic = new Topic("test");
        bindingCache.put(topic);
        assertThat(bindingCache.cache.contains(topic)).isTrue();
    }

    @Test
    @DisplayName("캐시에 없는 데이터는 매칭되지 않는다")
    void matchesTest() {
        var topic = new Topic("test");
        assertThat(bindingCache.matches(topic)).isFalse();
    }
}

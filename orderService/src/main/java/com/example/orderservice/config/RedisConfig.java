package com.example.orderservice.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;
import io.lettuce.core.api.StatefulConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

@Slf4j
@EnableCaching
@Configuration
public class RedisConfig {

    @Value("${spring.data.redis.host}")
    private String redisHost;

    @Value("${spring.data.redis.port}")
    private int redisPort;

    // ── Connection ────────────────────────────────────────────────────────────

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration serverConfig =
                new RedisStandaloneConfiguration(redisHost, redisPort);

        GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(8);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        LettucePoolingClientConfiguration clientConfig =
                LettucePoolingClientConfiguration.builder()
                        .commandTimeout(Duration.ofMillis(2000))
                        .poolConfig(poolConfig)
                        .build();

        return new LettuceConnectionFactory(serverConfig, clientConfig);
    }

    // ── Serializer ────────────────────────────────────────────────────────────

    /**
     * Shared Jackson2JsonRedisSerializer configured for Object values.
     *
     * activateDefaultTyping embeds a "@class" property in the JSON so that
     * deserialization can reconstruct the original type (e.g. OrderResponse)
     * rather than returning a LinkedHashMap.  BasicPolymorphicTypeValidator
     * restricts the trusted type namespace to com.example.* + java.* to
     * prevent arbitrary-class deserialization attacks.
     */
    @Bean
    public JacksonJsonRedisSerializer<Object> jacksonJsonRedisSerializer() {
        PolymorphicTypeValidator typeValidator = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .allowIfSubType("com.example.")
                .allowIfSubType("java.")
                .allowIfSubType("java.util.")
                .build();

        ObjectMapper mapper = new ObjectMapper()
                .rebuild()
                .activateDefaultTyping(
                        typeValidator,
                        DefaultTyping.NON_FINAL,
                        JsonTypeInfo.As.PROPERTY)
                .build();

        return new JacksonJsonRedisSerializer<>(mapper, Object.class);
    }

    // ── RedisTemplate ─────────────────────────────────────────────────────────

    @Bean
    public RedisTemplate<String, Object> redisTemplate(
            RedisConnectionFactory connectionFactory,
            JacksonJsonRedisSerializer<Object> valueSerializer) {

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        StringRedisSerializer keySerializer = new StringRedisSerializer();
        template.setKeySerializer(keySerializer);
        template.setHashKeySerializer(keySerializer);
        template.setValueSerializer(valueSerializer);
        template.setHashValueSerializer(valueSerializer);

        // Participates in @Transactional boundaries when present.
        // Note: reads inside a transaction are queued and return null until commit —
        // cache-aside reads in OrderCacheService run outside transactions by design.
        template.setEnableTransactionSupport(true);
        template.afterPropertiesSet();

        return template;
    }

    // ── CacheManager ──────────────────────────────────────────────────────────

    @Bean
    public CacheManager cacheManager(
            RedisConnectionFactory connectionFactory,
            JacksonJsonRedisSerializer<Object> valueSerializer) {

        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(10))
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(valueSerializer))
                .disableCachingNullValues();

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(config)
                .build();
    }
}

package com.example.demo.Config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaCommonConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String jaasConfig;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getJaasConfig() {
        return jaasConfig;
    }
}

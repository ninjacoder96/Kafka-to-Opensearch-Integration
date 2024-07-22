package com.example.demo.kafka;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final RestHighLevelClient openSearchClient;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String jaasConfig;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.topic}")
    private String topic;

    @Value("${opensearch.host}")
    private String openSearchHost;

    @Value("${opensearch.port}")
    private int openSearchPort;

    @Value("${opensearch.scheme}")
    private String openSearchScheme;

    @Value("${opensearch.username}")
    private String openSearchUsername;

    @Value("${opensearch.password}")
    private String openSearchPassword;

    public KafkaConsumerService(@Value("${opensearch.host}") String openSearchHost,
                                @Value("${opensearch.port}") int openSearchPort,
                                @Value("${opensearch.scheme}") String openSearchScheme,
                                @Value("${opensearch.username}") String openSearchUsername,
                                @Value("${opensearch.password}") String openSearchPassword) {
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(openSearchUsername, openSearchPassword));

        this.openSearchClient = new RestHighLevelClient(
                RestClient.builder(
                                new HttpHost(openSearchHost, openSearchPort, openSearchScheme))
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        })
        );
    }

    @EventListener
    public void startConsumer(ContextRefreshedEvent event) {
        new Thread(this::runConsumer).start();
    }

    private void runConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", jaasConfig);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received record: {}", record.value());
                    try {
                        IndexRequest request = new IndexRequest("appsetting-index")
                                .source(record.value(), XContentType.JSON);
                        openSearchClient.index(request, RequestOptions.DEFAULT);
                        logger.info("Successfully indexed record: {}", record.value());
                    } catch (Exception e) {
                        logger.error("Error indexing record: {}", record.value(), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in Kafka consumer", e);
        } finally {
            try {
                openSearchClient.close();
            } catch (IOException e) {
                logger.error("Error closing OpenSearch client", e);
            }
        }
    }
}

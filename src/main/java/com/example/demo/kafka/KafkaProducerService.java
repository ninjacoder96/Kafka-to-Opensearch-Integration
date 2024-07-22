package com.example.demo.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }

    public void sendLogMessage(String service, String message, int responseCode) {
        String logMessage = String.format("{\"service\":\"%s\", \"message\":\"%s\", \"response_code\":%d, \"timestamp\":\"%s\"}",
                service, message, responseCode, Instant.now().toString());
        sendMessage("appsetting-topic", logMessage);
    }
}

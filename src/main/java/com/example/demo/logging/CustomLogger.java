package com.example.demo.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.example.demo.kafka.KafkaProducerService;

@Component
public class CustomLogger {

    private static final Logger logger = LoggerFactory.getLogger(CustomLogger.class);
    private final KafkaProducerService kafkaProducerService;

    public CustomLogger(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    public void log(String service, String message, int responseCode) {
        String logMessage = String.format("{\"service\":\"%s\", \"message\":\"%s\", \"response_code\":%d, \"timestamp\":\"%s\"}",
                service, message, responseCode, java.time.Instant.now().toString());
        kafkaProducerService.sendMessage("appsetting-topic", logMessage);
        logger.info(logMessage);
    }
}

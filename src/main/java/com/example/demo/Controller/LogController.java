package com.example.demo.Controller;

import com.example.demo.kafka.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LogController {

    private final KafkaProducerService kafkaProducerService;

    @Autowired
    public LogController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @GetMapping("/log")
    public String log() {
        kafkaProducerService.sendLogMessage("appsetting-service", "This is a test log message", 500);
        return "Log message sent";
    }
}

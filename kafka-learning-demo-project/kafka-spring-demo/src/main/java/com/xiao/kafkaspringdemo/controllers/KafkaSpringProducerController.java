package com.xiao.kafkaspringdemo.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class KafkaSpringProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaSpringProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/spring-producer")
    public String sendMessage(@RequestParam String message) {

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("spring_topic", message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.warn("Unable to deliver message [{}] {}", message, throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Message [{}] delivered with with offset {}", message, result.getRecordMetadata().offset());
            }
        });

        return "Successfully send";
    }
}

package com.xiao.kafkaproducerdemoproject.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


import static com.xiao.kafkaproducerdemoproject.services.ResourcesPool.MESSAGE_GENERATOR;
import static com.xiao.kafkaproducerdemoproject.services.ResourcesPool.MESSAGE_QUEUE;

@RestController
public class MessageGeneratorController {

    private ExecutorService executorService;
    private final RestTemplate restTemplate;

    public MessageGeneratorController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @GetMapping("/message-generate/start")
    public String startGenerating() {

        setUp();

        executorService.execute(MESSAGE_GENERATOR);

        return "Message generator start...";
    }

    @GetMapping("/message-generate/stop")
    public String stopGenerating() {

        MESSAGE_GENERATOR.setFlag(false);

        executorService.shutdown();

        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return "Message generator stopped...";
    }

    private void setUp() {

        executorService = Executors.newSingleThreadExecutor();

        MESSAGE_GENERATOR.setMessageQueue(MESSAGE_QUEUE);
        MESSAGE_GENERATOR.setPoolNum(1000);
        MESSAGE_GENERATOR.setPeriod(100);
        MESSAGE_GENERATOR.setFlag(true);
        MESSAGE_GENERATOR.setRestTemplate(restTemplate);
    }

    @GetMapping("/message-generate/take")
    public String takeMessage() throws InterruptedException {

        while (MESSAGE_GENERATOR.isFlag() || !MESSAGE_QUEUE.isEmpty()) {

            String message = MESSAGE_QUEUE.poll(10, TimeUnit.MILLISECONDS);

            if (message != null) {
                System.out.println(message);
            }

            Thread.sleep(500);
        }

        return "Generator stopped and all the message have been taken...";
    }
}

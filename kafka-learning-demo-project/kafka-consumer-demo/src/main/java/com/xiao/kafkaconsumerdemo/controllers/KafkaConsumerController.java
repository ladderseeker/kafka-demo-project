package com.xiao.kafkaconsumerdemo.controllers;

import com.xiao.kafkaconsumerdemo.services.EsQueryService;
import com.xiao.kafkaconsumerdemo.services.KafkaConsumerProcessor;
import com.xiao.kafkaconsumerdemo.services.KafkaConsumerService;
import io.netty.util.concurrent.EventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RestController
public class KafkaConsumerController {

    private final KafkaConsumerService kafkaConsumerService;
    private final RestHighLevelClient elasticsearchClient;

    private final EsQueryService esQueryService;
    private KafkaConsumerProcessor kafkaConsumerProcessor;

    private ExecutorService executorService;

    public KafkaConsumerController(KafkaConsumerService kafkaConsumerService, EsQueryService esQueryService, RestHighLevelClient elasticsearchClient) {
        this.kafkaConsumerService = kafkaConsumerService;
        this.esQueryService = esQueryService;
        this.elasticsearchClient = elasticsearchClient;
    }

    @PostMapping("/kafka-es-consumer/start")
    public String consumeToElasticsearch(@RequestParam String indexName,
                                         @RequestParam String bootstrapServer,
                                         @RequestParam String topic,
                                         @RequestParam String groupId) throws InterruptedException {


        KafkaConsumer<String, String> consumer = kafkaConsumerService.createConsumer(bootstrapServer, groupId);

        kafkaConsumerProcessor = new KafkaConsumerProcessor(esQueryService, elasticsearchClient, indexName, consumer, Arrays.asList(topic));

        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(kafkaConsumerProcessor);

        return "Start consuming...";
    }

    @GetMapping("/kafka-es-consumer/stop")
    public String stopConsuming() {

        kafkaConsumerProcessor.setConsume(false);

        executorService.shutdown();

        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return "Stop Consuming";
    }
}

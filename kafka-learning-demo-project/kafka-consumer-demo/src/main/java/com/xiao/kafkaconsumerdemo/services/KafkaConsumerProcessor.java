package com.xiao.kafkaconsumerdemo.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

@Slf4j
public class KafkaConsumerProcessor implements Runnable {

    private volatile boolean isConsume;

    private final EsQueryService esQueryService;
    private final RestHighLevelClient client;
    private final String indexName;

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topicList;

    public KafkaConsumerProcessor(EsQueryService esQueryService, RestHighLevelClient client, String indexName, KafkaConsumer<String, String> consumer, List<String> topicList) {
        this.esQueryService = esQueryService;
        this.client = client;
        this.indexName = indexName;
        this.consumer = consumer;
        this.topicList = topicList;

        this.isConsume = true;
    }

    @Override
    public void run() {

        // subscribe consumer to our topics
        consumer.subscribe(topicList);

        // poll for new data
        while (isConsume) {

            // poll the data with 100 ms timeout
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // If no record, don't enter into for-loop and sleep for some time
            if (!records.isEmpty()) {
                log.info("Records {} received", records.count());
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                // kafka generic id
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                log.info("Key: {}", record.key());
                log.info("value: {}", record.value());
                log.info("Partition: {}", record.partition());
                log.info("Offset: {}", record.offset());

//                // index to elasticsearch with single request per record
//                String result = esQueryService.indexJsonString(client, id, indexName, record.value());
//                log.info("id & status : {}", result);

                // index to elasticsearch with bulk api
                IndexRequest indexRequest = new IndexRequest(indexName);
                indexRequest.id(id);
                indexRequest.source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!isConsume) {
                    log.info("Last process record id is {}", id);
                    break;
                }
            }

            // Test block is break, do not commit the offset
            // In real world if batch has been processed, commit the offset.
            if (isConsume) {

                try {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                    bulkResponse.forEach((bulkItemResponse) -> {
                        switch (bulkItemResponse.getOpType()) {
                            case INDEX:
                            case CREATE:
                                log.info("Created...");
                                break;
                            case UPDATE:
                                log.info("Updated...");
                                break;
                            case DELETE:
                                log.info("Deleted...");
                        }
                    });

                } catch (IOException e) {
                    e.printStackTrace();
                }

                log.info("Committing offsets...");
                consumer.commitAsync();
                log.info("Offset have benn committed...");
            }

            // 先睡为敬
            // Sleep for observation
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        consumer.close();
    }

    public void setConsume(boolean consume) {
        isConsume = consume;
    }
}

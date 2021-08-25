package com.xiao.kafkaproducerdemoproject.controllers;

import com.xiao.kafkaproducerdemoproject.services.VehiclePassMessageGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.xiao.kafkaproducerdemoproject.services.ResourcesPool.MESSAGE_GENERATOR;
import static com.xiao.kafkaproducerdemoproject.services.ResourcesPool.MESSAGE_QUEUE;

@Slf4j
@RestController
public class KafkaController {

    private final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private final static String KAFKA_TOPIC = "vehicle_pass_random";

    private final LinkedBlockingDeque<String> messageQueue = MESSAGE_QUEUE;
    private final VehiclePassMessageGenerator messageGenerator = MESSAGE_GENERATOR;


    @GetMapping("/kafka/produce")
    public String produce() {

        Producer<String, String> producer = createProducer();

        while (messageGenerator.isFlag() && !messageQueue.isEmpty()) {

            try {
                String message = messageQueue.poll(2000, TimeUnit.MILLISECONDS);

                if (message != null) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KAFKA_TOPIC, null, message);
                    producer.send(producerRecord, (recordMetadata, exception) -> {
                        if (exception != null) {
                            log.error("Producer sending error...", exception);
                        }
                    });

                    Thread.sleep(200);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("closing producer....");
        producer.close();

        return "Produce stopped....";
    }

    public Producer<String, String> createProducer() {

        // Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Kafka 2.8 >= 1.1, use 5, otherwise 1

        // High throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "2000");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        return new KafkaProducer<String, String>(properties);
    }


}

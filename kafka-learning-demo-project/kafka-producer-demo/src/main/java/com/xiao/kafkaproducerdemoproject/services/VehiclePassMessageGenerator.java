package com.xiao.kafkaproducerdemoproject.services;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.web.client.RestTemplate;

import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

@Getter
@Setter
@NoArgsConstructor
public class VehiclePassMessageGenerator implements Runnable {

    private RestTemplate restTemplate;
    private LinkedBlockingDeque<String> messageQueue;
    private int poolNum;
    private long period;
    private volatile boolean flag;

    @Override
    public void run() {

        Random random = new Random();

        JsonNode jsonNode = restTemplate.getForEntity(
                "http://10.194.227.215:6688/vehicle-pass/puller/random?indexName=index-tailor-vehicle-pass-es-sink-tangshan&num=" + poolNum,
                JsonNode.class).getBody();

        while (flag) {

            try {
                String message = jsonNode.get(random.nextInt(poolNum-1)).toString();
                messageQueue.put(message);
                Thread.sleep(period);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}

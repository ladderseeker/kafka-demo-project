package com.xiao.kafkaproducerdemoproject.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.Random;

@RestController
public class HelloWorldController {

    private final RestTemplate restTemplate;
    private final Random random = new Random();

    public HelloWorldController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;

    }

    @GetMapping({"", "/"})
    public String sayHello() throws InterruptedException {

        boolean ok = true;

        while (ok) {
            System.out.println(getRandomVehiclePass(100));
            Thread.sleep(1000);
        }

        return "Hello....";
    }

    public String getRandomVehiclePass(int poolNum) {

        int pos = random.nextInt(poolNum - 1);

        return restTemplate.getForEntity(
                "http://10.194.227.215:6688/vehicle-pass/puller/random?indexName=index-tailor-vehicle-pass-es-sink-tangshan&num=" + poolNum,
                JsonNode.class).getBody().get(pos).toString();
    }


}

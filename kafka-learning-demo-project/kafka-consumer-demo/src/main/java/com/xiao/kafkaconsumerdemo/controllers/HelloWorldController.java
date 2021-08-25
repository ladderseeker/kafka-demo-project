package com.xiao.kafkaconsumerdemo.controllers;

import com.xiao.kafkaconsumerdemo.services.EsQueryService;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class HelloWorldController {

    private final RestHighLevelClient elasticsearchClient;
    private final EsQueryService esQueryService;

    public HelloWorldController(RestHighLevelClient elasticsearchClient, EsQueryService esQueryService) {
        this.elasticsearchClient = elasticsearchClient;
        this.esQueryService = esQueryService;
    }

    @GetMapping({"", "/"})
    public String helloQuery() {

        Random random = new Random();

        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        String id = esQueryService.indexJsonString(elasticsearchClient, Integer.toString(random.nextInt(10000)), "using-kafka-consumer-test", jsonString);

        return "Creating id is " + id;
    }
}

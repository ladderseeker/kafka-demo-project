package com.xiao.kafkaconsumerdemo.configs;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

import java.time.Duration;

/**
 * @author XIAO JIN
 * @date 2021/05/31/ 19:33
 */
@Configuration
public class RestClientConfig extends AbstractElasticsearchConfiguration {

    @Value("${spring.elasticsearch.rest.uris}")
    private String uris;

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {

        final ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo(uris.split(","))
                .withConnectTimeout(Duration.ofSeconds(60))
                .withSocketTimeout(Duration.ofSeconds(360))
                .build();

        return RestClients.create(clientConfiguration).rest();
    }
}

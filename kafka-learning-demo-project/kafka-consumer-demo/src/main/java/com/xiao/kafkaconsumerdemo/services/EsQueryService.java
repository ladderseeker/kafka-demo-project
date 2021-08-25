package com.xiao.kafkaconsumerdemo.services;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class EsQueryService {

    public String indexJsonString(RestHighLevelClient restHighLevelClient,String id, String indexName, String content) {

        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id(id);
        indexRequest.source(content, XContentType.JSON);

        String status = "none";
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            id = indexResponse.getId();

            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                status = "created";

            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                status = "updated";

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return id + ": " +status;
    }
}

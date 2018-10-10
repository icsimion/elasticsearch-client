package com.sim;

import com.sim.ingest.IngestService;
import com.sim.request.index.IndexReqType;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Created by iliesimion.
 */
public class ESMain {

    public static void main(String[] args) {
        RestHighLevelClient client = createClient();

        IngestService ingestService = new IngestService();
        ingestService.setClient(client);
        ingestService.setIndex("movies_small");
        ingestService.setMappingType("doc");

        client = ingestService.ingest("movies_small.csv", IndexReqType.MOVIES);

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static RestHighLevelClient createClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );

        return client;
    }

}

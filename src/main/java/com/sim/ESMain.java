package com.sim;

import com.sim.ingest.IngestBulkService;
import com.sim.ingest.IngestService;
import com.sim.request.index.IndexReqType;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 * Created by iliesimion.
 */
public class ESMain {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        CreateIndexResponse index = createIndex(client, "movies");

        // ingest individual rows
//        IngestService ingestService = new IngestService();
//        ingestService.setClient(client);
//        ingestService.setIndex(index);
//        ingestService.setMappingType("doc");
//        client = ingestService.ingest("movies_small.csv", IndexReqType.MOVIES);

        // ingest bulk - movies
        IngestBulkService ingestBulkService = new IngestBulkService();
        ingestBulkService.setClient(client);
        ingestBulkService.setIndex(index);
        ingestBulkService.setMappingType("doc");
        client = ingestBulkService.ingest("movies.csv", IndexReqType.MOVIES);

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

    private static CreateIndexResponse createIndex(final RestHighLevelClient client, final String index) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        createIndexRequest.index(index);
        return client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

    }

}

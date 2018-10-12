package com.sim.service;

import com.sim.ingest.IngestBulkService;
import com.sim.ingest.IngestService;
import com.sim.request.index.IndexReqType;
import com.sim.request.search.SearchType;
import com.sim.search.SearchService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by iliesimion.
 */
public class ESService {
    RestHighLevelClient client;
    CreateIndexResponse index;
    private IndexReqType type;
    private SearchType searchType;

    public void runAndCloseClient(final Consumer consumer, final Object arg) {
        consumer.accept(arg);
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void createClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
        this.client = client;
    }

    public void createIndex(final String index) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        createIndexRequest.index(index);
        this.index = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    public void ingestIndividual(final String filename) {
        IngestService ingestService = new IngestService();
        ingestService.setClient(client);
        ingestService.setIndex(index);
        ingestService.setMappingType("doc");
        ingestService.ingest(filename, type);
    }

    public void ingestBulk(final String filename) {
        IngestBulkService ingestBulkService = new IngestBulkService();
        ingestBulkService.setClient(client);
        ingestBulkService.setIndex(index);
        ingestBulkService.setMappingType("doc");
        ingestBulkService.ingest(filename, type);
    }

    public void search(final Map<String, String> searchParams) {
        SearchService searchService = new SearchService();
        searchService.setClient(client);
        SearchRequest request = searchService.createSearchRequest(searchType, searchParams);
        searchService.searchAndPrintResults(request);
    }

    public void setType(IndexReqType type) {
        this.type = type;
    }

    public void setSearchType(SearchType searchType) {
        this.searchType = searchType;
    }
}

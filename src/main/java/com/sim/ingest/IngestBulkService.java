package com.sim.ingest;

import com.sim.request.index.IndexReqBuilder;
import com.sim.request.index.IndexReqType;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by iliesimion.
 */
public class IngestBulkService {
    private static final Logger LOG = Logger.getLogger(IngestBulkService.class.getName());
    private String FILE_LOCATION = "src/main/resources/movies/";

    private RestHighLevelClient client;

    private CreateIndexResponse index;
    private String mappingType;

    public RestHighLevelClient ingest(final String sourceFile, final IndexReqType reqType) {

        try {
            List<String> lines = FileUtils.readLines(new File(FILE_LOCATION + sourceFile));

            BulkRequest bulkRequest = new BulkRequest();

            // Iterate the result to process each line of the file.
            lines.stream().skip(1).forEach(line -> {
                IndexRequest request = new IndexReqBuilder()
                        .withIndex(index.index())
                        .withMappingType(mappingType)
                        .withReqType(reqType)
                        .withSource(line)
                        .build();
                bulkRequest.add(request);
            });

            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println(bulkResponse);
        } catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage());
        }

        return client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    public void setMappingType(String mappingType) {
        this.mappingType = mappingType;
    }

    public void setIndex(CreateIndexResponse index) {
        this.index = index;
    }
}

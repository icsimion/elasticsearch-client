package com.sim.search;

import com.sim.request.search.SearchType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by iliesimion.
 */
public class SearchService {
    private static final Logger LOG = Logger.getLogger(SearchService.class.getName());

    private RestHighLevelClient client;

    public SearchRequest createSearchRequest(final SearchType type, final Map<String, String> searchParams) {
        switch (type) {
            case FUZZY:
                return createSearchFuzzyRequest(searchParams);
            case SIMPLE:
                return createSearchSimpleRequest(searchParams);
            default:
                return createSearchSimpleRequest(searchParams);

        }
    }

    private SearchRequest createSearchSimpleRequest(final Map<String, String> searchParams) {
        String index = searchParams.get("index");
        String field = searchParams.get("field");
        String term = searchParams.get("term");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery(field, term));
        //        Set the from option that determines the result index to start searching from. Defaults to 0.
        sourceBuilder.from(0);
        //        Set the size option that determines the number of search hits to return. Defaults to 10.
        sourceBuilder.size(5);
        //        Set an optional timeout that controls how long the search is allowed to take.
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }

    private SearchRequest createSearchFuzzyRequest(final Map<String, String> searchParams) {
        String index = searchParams.get("index");
        String field = searchParams.get("field");
        String term = searchParams.get("term");
        Integer prefixLength = Integer.parseInt(searchParams.get("prefixLength"));
        Integer maxExpansions = Integer.parseInt(searchParams.get("maxExpansions"));


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, term)
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(prefixLength)
                .maxExpansions(maxExpansions);
        sourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }

    public SearchResponse searchAndPrintResults(final SearchRequest searchRequest) {
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
            System.out.println(searchRequest);
        } catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage());
        }

        return searchResponse;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }
}

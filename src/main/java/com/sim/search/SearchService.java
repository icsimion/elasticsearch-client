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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

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
            case BOOL:
                return createBoolSearchRequest(searchParams);
            case AGGREGATION:
                return createAggregationSearchRequest(searchParams);
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
        QueryBuilder matchQueryBuilder = QueryBuilders.fuzzyQuery(field, term)
                .fuzziness(Fuzziness.TWO)
                .prefixLength(prefixLength)
                .maxExpansions(maxExpansions);
        sourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }

    private SearchRequest createBoolSearchRequest(final Map<String, String> searchParams) {
        String index = searchParams.get("index");
        String field = searchParams.get("field");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .size(50); //default is 10

        QueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()

                // must not
                .mustNot(termQuery("year", 2000))
                // range
                .must(QueryBuilders.rangeQuery("year").from(1980).to(2018))

                // at least one of these clauses must match, like logical OR.
                // if the clauses match, they increase the _score; otherwise, they have no effect.
                .must(QueryBuilders.termQuery(field, "party"))
                .should(QueryBuilders.termQuery(field, "boat"))
                .should(QueryBuilders.termQuery(field, "trip"))
                .should(QueryBuilders.termQuery(field, "action"))
                .should(QueryBuilders.termQuery(field, "hero"))

                // the clause must appear in matching documents. These clauses must match, like logical AND.
                .must(QueryBuilders.termQuery("genre", "comedy"));

        sourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }

    private SearchRequest createAggregationSearchRequest(final Map<String, String> searchParams) {
        String index = searchParams.get("index");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .size(500);

        // results for user 1
        sourceBuilder.query(QueryBuilders.matchQuery("userId", "1"));

        // Add aggregations
        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("rating")
                        .field("rating")
                        .order(BucketOrder.key(false)) //order by key, desc
                        .subAggregation(
                                AggregationBuilders.avg("avg_rating")
                                        .field("rating")
                        );

        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }

    public SearchResponse search(final SearchRequest searchRequest) {
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("Number of results: " + searchResponse.getHits().getHits().length);
        } catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage());
        }

        return searchResponse;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }
}

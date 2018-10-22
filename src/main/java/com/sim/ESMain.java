package com.sim;

import com.sim.request.index.IndexReqType;
import com.sim.request.search.SearchType;
import com.sim.service.ESMappingService;
import com.sim.service.ESService;
import org.elasticsearch.action.search.SearchResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by iliesimion.
 */
public class ESMain {
    public static ESMappingService esMappingService;
    public static ESService esService;

    public static void main(String[] args) throws IOException {
        // service for ingest
        esMappingService = new ESMappingService();
        esService = new ESService();
        esService.createClient();

        esService.setType(IndexReqType.MOVIES);

//        moviesOps("s2");

//        ratingsOps("a1");

    }

    private static void moviesOps(String opID) throws IOException {
        switch (opID) {
            case "i1": {
                System.out.println("--------------Ingest individual rows---------------");
                esService.createIndex("movies_small", esMappingService.readMappingFile("movie_mapping.json"));
                Consumer<String> consumerInd = (filename) -> esService.ingestIndividual(filename);
                esService.runAndCloseClient(consumerInd, "movies_small.csv");

                break;
            }
            case "i2": {
                System.out.println("--------------Ingest bulk---------------");
                esService.createIndex("movies", esMappingService.readMappingFile("movie_mapping.json"));
                Consumer<String> consumerBulk = (filename) -> esService.ingestBulk(filename);
                esService.runAndCloseClient(consumerBulk, "movies.csv");

                break;
            }
            case "s1": {
                System.out.println("--------------Simple search---------------");
                Map<String, String> params = new HashMap<>();
                params.put("index", "movies");
                params.put("field", "title");
                params.put("term", "star");

                esService.setSearchType(SearchType.SIMPLE);
                Consumer<Map<String, String>> consumer = (parameters) -> {
                    SearchResponse response = esService.search(parameters);
                    esService.printSearchResults(response);
                };
                esService.runAndCloseClient(consumer, params);

                break;
            }
            case "s2": {
                System.out.println("--------------Fuzzy search---------------");
                Map<String, String> params = new HashMap<>();
                params.put("index", "movies");
                params.put("field", "title");
                params.put("term", "stra");
                params.put("prefixLength", "1");
                params.put("maxExpansions", "3");

                esService.setSearchType(SearchType.FUZZY);
                Consumer<Map<String, String>> consumer = (parameters) -> {
                    SearchResponse response = esService.search(parameters);
                    esService.printSearchResults(response);
                };
                esService.runAndCloseClient(consumer, params);

                break;
            }
            case "s3": {
                System.out.println("--------------Bool search---------------");
                Map<String, String> params = new HashMap<>();
                params.put("index", "movies");
                params.put("field", "title");
                params.put("term", "trip");

                esService.setSearchType(SearchType.BOOL);
                Consumer<Map<String, String>> consumer = (parameters) -> {
                    SearchResponse response = esService.search(parameters);
                    esService.printSearchResults(response);
                };
                esService.runAndCloseClient(consumer, params);

                break;
            }
            default: {
                System.out.println("Default");
            }
        }


    }


    private static void ratingsOps(final String opID) throws IOException {
        switch (opID) {
            case "i1": {
                System.out.println("--------------Ingest bulk - ratings---------------");
                esService.setType(IndexReqType.RATINGS);
                esService.createIndex("ratings", esMappingService.readMappingFile("rating_mapping.json"));
                Consumer<String> consumerBulk = (filename) -> esService.ingestBulk(filename);
                esService.runAndCloseClient(consumerBulk, "ratings.csv");

                break;
            }
            case "a1": {
                System.out.println("--------------Aggregation---------------");
                Map<String, String> paramsBool = new HashMap<>();
                paramsBool.put("index", "ratings");

                esService.setSearchType(SearchType.AGGREGATION);
                Consumer<Map<String, String>> consumerFuzzy = (params) -> {
                    SearchResponse response = esService.search(params);
                    esService.printAggregationResults(response, "rating");
                };
                esService.runAndCloseClient(consumerFuzzy, paramsBool);

                break;
            }
            default: {
                System.out.println("Default");
            }

        }


    }

}

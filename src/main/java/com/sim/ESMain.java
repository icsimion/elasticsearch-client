package com.sim;

import com.sim.request.index.IndexReqType;
import com.sim.request.search.SearchType;
import com.sim.service.ESService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by iliesimion.
 */
public class ESMain {

    public static void main(String[] args) throws IOException {
        // service for ingest
        ESService esService = new ESService();
        esService.createClient();
        esService.setType(IndexReqType.MOVIES);

        // ingest individual rows
//        esService.createIndex("movies_small");
//        Consumer<String> consumerInd = (filename) -> esService.ingestIndividual(filename);
//        esService.runAndCloseClient(consumerInd, "movies_small.csv");

        // ingest bulk - movies
//        esService.createIndex("movies");
//        Consumer<String> consumerBulk = (filename) -> esService.ingestBulk(filename);
//        esService.runAndCloseClient(consumerBulk, "movies.csv");

        // search
        System.out.println("--------------Simple search---------------");
//        Map<String, String> paramsSimple = new HashMap<>();
//        paramsSimple.put("index", "movies");
//        paramsSimple.put("field", "title");
//        paramsSimple.put("term", "star");
//
//        esService.setSearchType(SearchType.SIMPLE);
//        Consumer<Map<String, String>> consumerSimple = (params) -> esService.search(params);
//        esService.runAndCloseClient(consumerSimple, paramsSimple);

        System.out.println("--------------Fuzzy search---------------");
        Map<String, String> paramsFuzzy = new HashMap<>();
        paramsFuzzy.put("index", "movies");
        paramsFuzzy.put("field", "title");
        paramsFuzzy.put("term", "stra");
        paramsFuzzy.put("prefixLength", "1");
        paramsFuzzy.put("maxExpansions", "3");

        esService.setSearchType(SearchType.FUZZY);
        Consumer<Map<String, String>> consumerFuzzy = (params) -> esService.search(params);
        esService.runAndCloseClient(consumerFuzzy, paramsFuzzy);
    }

}

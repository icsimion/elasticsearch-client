package com.sim.request.index.type;

import com.sim.request.index.IndexReq;
import org.elasticsearch.common.collect.Tuple;

import java.util.HashMap;
import java.util.Map;

import static com.sim.request.DocSourceGenerator.*;

/**
 * Created by iliesimion.
 */
public class MovieRequest extends IndexReq {

    public MovieRequest() {
        setFields(new String[]{"id", "title", "year", "genre"});
    }

    public void populateSource(final String source) {
        String[] fields = getFields();

        Map<String, Object> jsonMap = new HashMap<>();
        String[] values = source.split(CSV_SPLIT_CHAR);

        //id
        String id = values[0];
        jsonMap.put(fields[0], id);

        //title & year
        Tuple<String, Integer> tuple = getTitleYear(source);
        jsonMap.put(fields[1], tuple.v1());
        jsonMap.put(fields[2], tuple.v2());

        //genres
        jsonMap.put(fields[3], splitArrayField(values[values.length - 1]));

        setSource(jsonMap);
        setId(id);
    }

}

package com.sim.request.index.type;

import com.sim.request.index.IndexReq;

import java.util.HashMap;
import java.util.Map;

import static com.sim.request.DocSourceUtils.*;

/**
 * Created by iliesimion.
 */
public class RatingRequest extends IndexReq {

    public RatingRequest() {
        setFields(new String[]{"userId", "movieId", "rating", "timestamp"});
    }

    public void populateSource(final String source) {
        String[] fields = getFields();

        Map<String, Object> jsonMap = new HashMap<>();
        String[] values = source.split(CSV_SPLIT_CHAR);

        //userId
        jsonMap.put(fields[0], values[0]);
        jsonMap.put(fields[1], values[1]);
        jsonMap.put(fields[2], values[2]);
        jsonMap.put(fields[3], values[3]);

        setSource(jsonMap);
    }

}

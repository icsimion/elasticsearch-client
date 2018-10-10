package com.sim.request;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by iliesimion
 */
public class DocSourceGenerator {
    private final String CSV_SPLIT_CHAR = ",";

    private String[] fields;

    /**
     * Constructor
     * @param fields
     */
    public DocSourceGenerator(final String[] fields) {
        this.fields = fields;
    }

    /**
     * Creates doc source as a map
     *
     * @param csvRow
     * @return
     */
    public Map createJSONSource(final String csvRow) {

        String[] values = csvRow.split(CSV_SPLIT_CHAR);
        if (values.length == 0) {
            return null;
        }

        return createJsonMap(values);
    }

    private Map createJsonMap(final String[] values) {
        Map<String, String> jsonMap = new HashMap<>();

        for (int i = 0; i < values.length; i++) {
            jsonMap.put(fields[i], values[i]);
        }
        return jsonMap;
    }
}

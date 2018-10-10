package com.sim.request.index;

import com.sim.request.DocSourceGenerator;

import java.util.Map;

/**
 * Created by iliesimion on 10/9/2018.
 */
public abstract class IndexReq {
    private String id;
    private Map<String, String> source;
    private String[] fields;

    public void populateSource(final String source) {
        DocSourceGenerator docSourceGenerator = new DocSourceGenerator(fields);
        Map jsonSource = docSourceGenerator.createJSONSource(source);
        this.source = jsonSource;
        this.id = (String) jsonSource.get("id");
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getSource() {
        return source;
    }


    public void setFields(String[] fields) {
        this.fields = fields;
    }
}

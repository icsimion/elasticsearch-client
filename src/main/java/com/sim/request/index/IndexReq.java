package com.sim.request.index;

import com.sim.request.DocSourceGenerator;

import java.util.Map;

/**
 * Created by iliesimion on 10/9/2018.
 */
public abstract class IndexReq {
    private String id;
    private Map<String, Object> source;
    private String[] fields;

    public abstract void populateSource(final String source);

    public String getId() {
        return id;
    }

    public Map<String, Object> getSource() {
        return source;
    }


    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public String[] getFields() {
        return fields;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    public void setId(String id) {
        this.id = id;
    }

}

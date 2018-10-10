package com.sim.request.index;

import org.elasticsearch.action.index.IndexRequest;

/**
 * Created by iliesimion.
 */
public class IndexReqBuilder {
    private IndexReq indexReq;

    private String index;
    private String mappingType;


    public IndexReqBuilder withIndex(final String index) {
        this.index = index;
        return this;
    }

    public IndexReqBuilder withMappingType(final String mappingType) {
        this.mappingType = mappingType;
        return this;
    }

    public IndexReqBuilder withReqType(final IndexReqType type) {
        this.indexReq = IndexReqFactory.build(type);
        return this;
    }

    public IndexReqBuilder withSource(final String source) {
        this.indexReq.populateSource(source);
        return this;
    }

    public IndexRequest build() {
        IndexRequest request = new IndexRequest(
                index,
                this.mappingType,
                this.indexReq.getId());
        request.source(this.indexReq.getSource());
        return request;
    }
}

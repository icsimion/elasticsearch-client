package com.sim.request.index.type;

import com.sim.request.index.IndexReq;

/**
 * Created by iliesimion.
 */
public class MovieRequest extends IndexReq {

    public MovieRequest() {
        setFields(new String[]{"id", "title", "genres"});
    }
}

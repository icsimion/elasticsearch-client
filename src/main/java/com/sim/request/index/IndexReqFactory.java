package com.sim.request.index;

import com.sim.request.index.type.MovieRequest;

/**
 * Created by iliesimion.
 */
public abstract class IndexReqFactory {
    public static IndexReq build(final IndexReqType type) {
        IndexReq indexRequest = null;
        switch (type) {
            case MOVIES:
                indexRequest = new MovieRequest();
                break;
            default:
                break;
        }
        return indexRequest;
    }

}

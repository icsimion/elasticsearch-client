package com.sim.request.index;

import com.sim.request.index.type.MovieRequest;
import com.sim.request.index.type.RatingRequest;

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
            case RATINGS:
                indexRequest = new RatingRequest();
                break;
            default:
                break;
        }
        return indexRequest;
    }

}

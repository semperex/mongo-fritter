package com.semperex.mongo_fritter.model;

import org.bson.codecs.pojo.annotations.BsonId;

public interface Model<IdT> {

    @BsonId
    void setId(IdT id);

    @BsonId
    IdT getId();

}

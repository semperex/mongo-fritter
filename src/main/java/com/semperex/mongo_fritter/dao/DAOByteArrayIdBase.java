package com.semperex.mongo_fritter.dao;

import com.semperex.mongo_fritter.model.Model;
import com.semperex.mongo_fritter.util.MongoDAOUtil;

public abstract class DAOByteArrayIdBase<T extends Model<byte[]>> extends DAOBase<T, byte[]> {

    @Override
    public T create() throws DAOException {
        final T model;
        try {
            model = newPrimaryModelInstance();
        } catch (Exception e) {
            throw new DAOException(e);
        }

        try {
            return MongoDAOUtil.insertOneWithUniqueUUIDBytes(
                    getPrimaryCollection(),
                    getPrimaryIdFieldName(),
                    model);
        } catch (Exception e) {
            throw new DAOException(e);
        }
    }


}

package com.semperex.mongo_fritter.test;

import com.semperex.mongo_fritter.data_source.DataSource;
import com.semperex.mongo_fritter.data_source.DataSourceBuilder;
import com.semperex.mongo_fritter.storage.manager.DataSourceRegistrationStorageManagerException;
import com.semperex.mongo_fritter.storage.manager.StorageManager;

import java.util.List;

public class StorageManagerTestBase extends AbstractFlapdoodleTestBase {

    protected void setupStorage(final String dataSourceName) throws DataSourceRegistrationStorageManagerException {

         final DataSource dataSource = DataSourceBuilder.aDataSource()
                .withName(dataSourceName)
                .withServerAddresses(List.of(new DataSource.ServerAddress(getMongoDBHostName(),getMongoDBPort())))
                .withDatabaseName(getMongoDBDatabaseName())
                .build();

        StorageManager.getInstance().registerDataSource( dataSource );

    }

}

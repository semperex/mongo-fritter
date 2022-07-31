package mongo_fritter.test;

import mongo_fritter.data_source.DataSource;
import mongo_fritter.data_source.DataSourceBuilder;
import mongo_fritter.storage.manager.DataSourceRegistrationStorageManagerException;
import mongo_fritter.storage.manager.StorageManager;

import java.util.List;

public class StorageManagerTestBase extends AbstractFlapdoodleTestBase {

    protected void setupStorage(final String dataSourceName) throws DataSourceRegistrationStorageManagerException {
        final StorageManager storageManager;
        {
             final DataSource dataSource = DataSourceBuilder.aDataSource()
                    .withName(dataSourceName)
                    .withServerAddresses(List.of(new DataSource.ServerAddress(getMongoDBHostName(),getMongoDBPort())))
                    .withDatabaseName(getMongoDBDatabaseName())
                    .build();

            storageManager = StorageManager.getInstance();

            storageManager.registerDataSource(dataSource);
        }
    }

}

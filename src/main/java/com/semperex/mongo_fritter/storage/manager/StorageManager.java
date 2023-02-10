package com.semperex.mongo_fritter.storage.manager;

import com.semperex.mongo_fritter.storage.Storage;
import com.semperex.mongo_fritter.data_source.DataSource;
import com.semperex.mongo_fritter.storage.StorageBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StorageManager {

    private final Map<String, Storage> storages = Collections.synchronizedMap(new HashMap<>());

    private final String defaultStorageName = "default";

    private static final StorageManager INSTANCE = new StorageManager();

    public static final StorageManager getInstance() {
        return INSTANCE;
    }

    private StorageManager() {
        if (StringUtils.isBlank(defaultStorageName)) throw new IllegalStateException();
    }

    public Storage getStorage(final String dataSourceName) throws DataSourceNotRegisteredStorageManagerException
    {
        if (StringUtils.isBlank(dataSourceName)) throw new IllegalArgumentException();

        {
            final Storage storage = storages.get(dataSourceName);
            if (storage != null) return storage;
        }
        synchronized(this) {
            final Storage storage = storages.get(dataSourceName);
            if (storage != null) return storage;

            throw new DataSourceNotRegisteredStorageManagerException("data source not registered, name: " + dataSourceName);
        }
    }

    public Storage getStorage() throws DataSourceNotRegisteredStorageManagerException {
        return getStorage(defaultStorageName);
    }

    public Storage registerDataSource(final DataSource dataSource) throws DataSourceRegistrationStorageManagerException
    {
        if (dataSource == null) throw new IllegalArgumentException();

        final String dataSourceName = dataSource.getName() != null ? dataSource.getName() : defaultStorageName;
        // if (StringUtils.isBlank(dataSourceName)) throw new IllegalStateException();
        if (storages.containsKey(dataSourceName)) throw new DataSourceRegistrationStorageManagerException("already registered data source name: " + dataSourceName);

        final Storage storage = newStorage(dataSource);
        storages.put( dataSourceName, storage );
        return storage;
    }

    protected Storage newStorage(final DataSource dataSource)
    {
        if (dataSource == null) throw new IllegalArgumentException();

        final StorageBuilder storageBuilder = StorageBuilder.aStorage();
        if (dataSource.getDatabaseName() != null) storageBuilder.withDatabaseName(dataSource.getDatabaseName());
        if (dataSource.getUsername() != null) storageBuilder.withUsername(dataSource.getUsername());
        if (dataSource.getPassword() != null) storageBuilder.withPassword(dataSource.getPassword());
        if (dataSource.getServerAddresses() != null) {
            for (final DataSource.ServerAddress serverAddress : dataSource.getServerAddresses()) {
                storageBuilder.withDatabaseServer(serverAddress.getHostName(), serverAddress.getPort());
            }
        }
        if (dataSource.getReplicaSetName() != null) storageBuilder.withReplicaSetName(dataSource.getReplicaSetName());
        if (dataSource.getClusterName() != null) storageBuilder.withClusterName(dataSource.getClusterName());

        return storageBuilder.build();
    }

}

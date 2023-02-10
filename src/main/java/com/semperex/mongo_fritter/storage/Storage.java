package com.semperex.mongo_fritter.storage;

import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.semperex.mongo_fritter.util.MongoDBPOJOConnectionCreator;
import com.semperex.mongo_fritter.util.MongoDBPOJOConnectionCreatorBuilder;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Storage
{

    private static final Logger log = LoggerFactory.getLogger(Storage.class);

    private MongoDatabase database;

    private final MongoDBPOJOConnectionCreatorBuilder connectionBuilder = MongoDBPOJOConnectionCreatorBuilder.builder();

    public void addCodec(final Codec codec) {
        Objects.requireNonNull(codec);

        connectionBuilder.withCodec(codec);
    }

    public void addPackageName(final String packageName) {
        if (StringUtils.isBlank(packageName)) throw new IllegalArgumentException();

        connectionBuilder.withPojoPackageName(packageName);
    }

    /**
     * INTERNAL USE ONLY.
     *
     * @return
     */
    public MongoDatabase refresh() {
        final MongoDBPOJOConnectionCreator connectionCreator = connectionBuilder.build();
        this.database = connectionCreator.connectAndGetDatabaseOnly();

        return database;
    }

    Storage(
            final String databaseName,
            final String username,
            final String password,
            final String replicaSetName,
            final String clusterName,
            final String[] hostNames,
            final int[] ports)
        throws StorageException
    {
        if (databaseName != null && StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();
        if (databaseName != null ) {
            connectionBuilder.withDatabaseName(databaseName);
        }

        if (username != null) {
            if (StringUtils.isBlank(username)) throw new IllegalArgumentException();
            connectionBuilder.withUsername(username);
        }

        if (password != null) {
            if (StringUtils.isBlank(password)) throw new IllegalArgumentException();
            connectionBuilder.withPassword(password);
        }

        if (replicaSetName != null) {
            if (StringUtils.isBlank(replicaSetName)) throw new IllegalArgumentException();
            connectionBuilder.withReplicateSetName(replicaSetName);
        }

        if (clusterName != null) {
            if (StringUtils.isBlank(clusterName)) throw new IllegalArgumentException();
            connectionBuilder.withClusterName(clusterName);
        }

        if (hostNames != null) {
            if (hostNames.length == 0) throw new IllegalArgumentException();
            for (final String hostName : hostNames) {
                if (StringUtils.isBlank(hostName)) throw new IllegalArgumentException();
            }
        }

        if (ports != null) {
            if (ports.length == 0) throw new IllegalArgumentException();
            for (final int port : ports) {
                if (port < 0) throw new IllegalArgumentException();
                if (port == 0) throw new IllegalArgumentException();
            }
        }

        if (hostNames != null && ports == null) throw new IllegalStateException();
        if (hostNames == null && ports != null) throw new IllegalStateException();

        if (hostNames != null) {
            if (hostNames.length != ports.length) throw new IllegalArgumentException();
        }

        if (hostNames == null) {
            if (ports != null) throw new IllegalStateException();
            connectionBuilder.withDefaultServerAddresses();
        }  else {
            final List<ServerAddress> serverAddresses = new ArrayList<>();
            for (int i = 0; i < hostNames.length; i++) {
                final ServerAddress serverAddress = new ServerAddress( hostNames[i], ports[i] );
                serverAddresses.add( serverAddress );
            }
            connectionBuilder.withServerAddresses( Collections.unmodifiableList( serverAddresses ) );
        }

        final MongoDatabase database = refresh();
        this.database = database;
    }

    public SortedSet<String> getCollectionNames()
    {
        final SortedSet<String> collectionNames = new TreeSet<>();
        for (final String collectionName : database.listCollectionNames())
            collectionNames.add(collectionName);
        return Collections.unmodifiableSortedSet( collectionNames );
    }

    public MongoDatabase getDatabase() {
        return database;
    }

}

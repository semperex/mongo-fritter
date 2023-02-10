package com.semperex.mongo_fritter.storage;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public final class StorageBuilder {
    private String databaseName;

    private String username;

    private String password;

    private String replicaSetName;

    private String clusterName;

    private final List<String> hostNames = new ArrayList<>();

    private final IntList ports = new IntArrayList();

    private StorageBuilder() {
    }

    public static StorageBuilder aStorage() {
        return new StorageBuilder();
    }

    public StorageBuilder withDatabaseName(final String databaseName) {
        if (StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();
        this.databaseName = databaseName;
        return this;
    }

    public StorageBuilder withUsername(final String username) {
        if (StringUtils.isBlank(username)) throw new IllegalArgumentException();
        this.username = username;
        return this;
    }

    public StorageBuilder withPassword(final String password) {
        if (StringUtils.isBlank(password)) throw new IllegalArgumentException();
        this.password = password;
        return this;
    }

    public StorageBuilder withReplicaSetName(final String replicaSetName) {
        if (StringUtils.isBlank(replicaSetName)) throw new IllegalArgumentException();
        this.replicaSetName = replicaSetName;
        return this;
    }

    public StorageBuilder withClusterName(final String clusterName) {
        if (StringUtils.isBlank(clusterName)) throw new IllegalArgumentException();
        this.clusterName = clusterName;
        return this;
    }

    public StorageBuilder withDatabaseServer(final String hostName, final int port) {
        if (StringUtils.isBlank(hostName)) throw new IllegalArgumentException();
        if (port < 0) throw new IllegalArgumentException();
        if (port == 0) throw new IllegalArgumentException();

        hostNames.add(hostName);
        ports.add(port);

        return this;
    }

    public StorageBuilder withDefaultDatabaseServers() {
        hostNames.clear();
        ports.clear();
        return this;
    }

    public Storage build() {
        try {
            return new Storage(
                    databaseName,
                    username,
                    password,
                    replicaSetName,
                    clusterName,
                    hostNames.isEmpty() ? null : hostNames.toArray(new String[]{}),
                    ports.isEmpty() ? null : ports.toIntArray()
            );
        } catch (StorageException e) {
            throw new RuntimeException(e); // TODO
        }
    }

}

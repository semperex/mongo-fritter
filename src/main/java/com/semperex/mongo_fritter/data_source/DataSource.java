package com.semperex.mongo_fritter.data_source;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataSource {

    public static final class ServerAddress {
        private final String hostName;
        private final int port;

        public ServerAddress(final String hostName, final int port) {
            if (StringUtils.isBlank(hostName)) throw new IllegalArgumentException();
            if (port < 0) throw new IllegalArgumentException();
            if (port == 0) throw new IllegalArgumentException();

            this.hostName = hostName;
            this.port = port;
        }

        public String getHostName() {
            return hostName;
        }

        public int getPort() {
            return port;
        }

    }

    private String name;

    private List<ServerAddress> serverAddresses;

    private String databaseName;

    private String username;

    private String password;

    private String replicaSetName;

    private String clusterName;

    public DataSource(
            final String name,
            final List<ServerAddress> serverAddresses,
            final String databaseName,
            final String username,
            final String password,
            final String replicaSetName,
            final String clusterName)
    {
        if (name != null && StringUtils.isBlank(name)) throw new IllegalArgumentException();
        //        if (serverAddresses != null) throw new IllegalArgumentException();
        //for (final ServerAddress serverAddress : serverAddresses) {
        //    if (serverAddress == null) throw new IllegalArgumentException();
        //}
        if (databaseName != null && StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();
        if (username != null && StringUtils.isBlank(username)) throw new IllegalArgumentException();
        if (password != null && StringUtils.isBlank(password)) throw new IllegalArgumentException();
        if (replicaSetName != null && StringUtils.isBlank(replicaSetName)) throw new IllegalArgumentException();
        if (clusterName != null && StringUtils.isBlank(clusterName)) throw new IllegalArgumentException();

        this.name = name;
        this.serverAddresses = serverAddresses != null ? Collections.unmodifiableList( List.copyOf( serverAddresses ) ) : null;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
        this.replicaSetName = replicaSetName;
        this.clusterName = clusterName;
    }

    public String getName() {
        return name;
    }

    public List<ServerAddress> getServerAddresses() {
        return serverAddresses;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getReplicaSetName() {
        return replicaSetName;
    }

    public String getClusterName() {
        return clusterName;
    }

}

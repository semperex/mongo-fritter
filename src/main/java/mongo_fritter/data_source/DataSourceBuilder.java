package mongo_fritter.data_source;

import java.util.List;

public final class DataSourceBuilder {

    private String name;
    private List<DataSource.ServerAddress> serverAddresses;
    private String databaseName;
    private String username;
    private String password;
    private String replicaSetName;
    private String clusterName;

    private DataSourceBuilder() {
    }

    public static DataSourceBuilder aDataSource() {
        return new DataSourceBuilder();
    }

    public DataSourceBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public DataSourceBuilder withServerAddresses(List<DataSource.ServerAddress> serverAddresses) {
        this.serverAddresses = serverAddresses;
        return this;
    }

    public DataSourceBuilder withDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public DataSourceBuilder withUsername(String username) {
        this.username = username;
        return this;
    }

    public DataSourceBuilder withPassword(String password) {
        this.password = password;
        return this;
    }

    public DataSourceBuilder withReplicaSetName(String replicaSetName) {
        this.replicaSetName = replicaSetName;
        return this;
    }

    public DataSourceBuilder withClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public DataSource build() {
        return new DataSource(name, serverAddresses, databaseName, username, password, replicaSetName, clusterName);
    }

}

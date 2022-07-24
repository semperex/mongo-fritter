package mongo_fritter.util;

import com.mongodb.ServerAddress;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.Codec;

import java.util.*;

public final class MongoDBPOJOConnectionCreatorBuilder {
    private List<String> pojoPackageNames = new ArrayList<>();
    private List<Codec> codecs = new ArrayList<>();
    private String databaseName = null;
    private String username = null;
    private String password = null;
    private List<ServerAddress> serverAddresses = new ArrayList<>();
    private String replicaSetName = null;

    private String clusterName = null;

    private MongoDBPOJOConnectionCreatorBuilder() {
    }

    public static MongoDBPOJOConnectionCreatorBuilder builder() {
        return new MongoDBPOJOConnectionCreatorBuilder();
    }

    public MongoDBPOJOConnectionCreatorBuilder withPojoPackageNames(final List<String> pojoPackageNames) {
        if (!Collections.disjoint(this.pojoPackageNames, pojoPackageNames)) throw new IllegalArgumentException("duplicate");
        this.pojoPackageNames.addAll( pojoPackageNames );
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withPojoPackageName(final String pojoPackageName) {
        if ( StringUtils.isBlank( pojoPackageName ) ) throw new IllegalArgumentException();
        return withPojoPackageNames(List.of(pojoPackageName));
    }

    public MongoDBPOJOConnectionCreatorBuilder withCodecs(final List<Codec> codecs) {
        if (!Collections.disjoint(this.codecs, codecs)) throw new IllegalArgumentException("duplicate");
        this.codecs.addAll( codecs );
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withCodec(final Codec codec) {
        Objects.requireNonNull( codec );
        return withCodecs(List.of(codec));
    }

    public MongoDBPOJOConnectionCreatorBuilder withDatabaseName(final String databaseName) {
        if (StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();
        this.databaseName = databaseName;
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withUsername( final String username ) {
        if (StringUtils.isBlank(username)) throw new IllegalArgumentException();
        this.username = username;
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withPassword( final String password ) {
        if (StringUtils.isBlank(password)) throw new IllegalArgumentException();
        this.password = password;
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withServerAddresses(final List<ServerAddress> serverAddresses) {
        if (!Collections.disjoint(this.serverAddresses, serverAddresses)) throw new IllegalArgumentException("duplicate");
        this.serverAddresses.addAll( serverAddresses );
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withServerAddress(final ServerAddress serverAddress) {
        Objects.requireNonNull( serverAddress );
        return withServerAddresses(List.of(serverAddress));
    }

    public MongoDBPOJOConnectionCreatorBuilder withDefaultServerAddresses() {
        return withServerAddresses(
            Arrays.asList(
                    new ServerAddress("localhost", 27017),
                    new ServerAddress("127.0.0.1", 27017),
                    new ServerAddress("172.17.0.1", 27017) // for Docker on a Linux (Ubuntu) host
            )
        );
    }

    public MongoDBPOJOConnectionCreatorBuilder withLocalServerAddress() {
        return withServerAddresses(
                Arrays.asList(
                        new ServerAddress("localhost", 27017)
                )
        );
    }

    public MongoDBPOJOConnectionCreatorBuilder withReplicateSetName(final String replicaSetName) {
        if (StringUtils.isBlank(replicaSetName)) throw new IllegalArgumentException();
        this.replicaSetName = replicaSetName;
        return this;
    }

    public MongoDBPOJOConnectionCreatorBuilder withClusterName(final String clusterName) {
        if (StringUtils.isBlank(clusterName)) throw new IllegalArgumentException();
        this.clusterName = clusterName;
        return this;
    }

    public MongoDBPOJOConnectionCreator build() {
        return new MongoDBPOJOConnectionCreator(pojoPackageNames, codecs, databaseName, username, password, serverAddresses, replicaSetName, clusterName);
    }
}

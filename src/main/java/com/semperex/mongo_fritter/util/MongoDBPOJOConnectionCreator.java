package com.semperex.mongo_fritter.util;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.ServerSelector;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MongoDBPOJOConnectionCreator {

    private static final Logger log = LoggerFactory.getLogger(MongoDBPOJOConnectionCreator.class);

    private boolean autoCreateDatabase = true;

    private final String usernameEnvName = "MONGODB_USERNAME";
    private final String passwordEnvName = "MONGODB_PASSWORD";

    private String databaseName;
    private String username;
    private String password;
    private List<ServerAddress> serverAddresses;
    private String replicaSetName;
    private String clusterName;

    private List<String> pojoPackageNames;
    private List<Codec> codecs;

    MongoDBPOJOConnectionCreator(final List<String> pojoPackageNames, final List<Codec> codecs, final String databaseName, final String username, final String password, final List<ServerAddress> serverAddresses, final String replicaSetName, String clusterName) {
        Objects.requireNonNull( pojoPackageNames );
        Objects.requireNonNull( codecs );
        Objects.requireNonNull( databaseName );
        if (StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();

        this.pojoPackageNames = pojoPackageNames;
        this.codecs = codecs;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
        this.serverAddresses = serverAddresses;
        this.replicaSetName = replicaSetName;
        this.clusterName = clusterName;
    }

    public static class MongoClientAndDatabase {
        private final MongoClient mongoClient;
        private final MongoDatabase database;

        private MongoClientAndDatabase(final MongoClient mongoClient, final MongoDatabase database) {
            Objects.requireNonNull(mongoClient);
            Objects.requireNonNull(database);

            this.mongoClient = mongoClient;
            this.database = database;
        }

        public MongoClient getClient() {
            return mongoClient;
        }

        public MongoDatabase getDatabase() {
            return database;
        }

    }

    @Deprecated
    public MongoDatabase connectAndGetDatabaseOnly() {
        final MongoClientAndDatabase c = connect();
        return c.getDatabase();
    }

    public MongoClientAndDatabase connect()
    {
        MongoDatabase db = null;

        final CodecProvider pojoCodecProvider;
        {
            final PojoCodecProvider.Builder builder = PojoCodecProvider.builder();
            for (final String pojoPackage : pojoPackageNames) {
                builder.register( pojoPackage );
            }
            pojoCodecProvider = builder.build();
        }

        final CodecRegistry pojoCodecRegistry;
        {
            final List<CodecRegistry> codecRegistries = new ArrayList<>();
            codecRegistries.add( com.mongodb.MongoClient.getDefaultCodecRegistry() );
            for (final Codec codec : codecs) {
                codecRegistries.add( CodecRegistries.fromCodecs( codec ) );
            }
            codecRegistries.add( CodecRegistries.fromProviders(pojoCodecProvider) );

            pojoCodecRegistry = CodecRegistries.fromRegistries( codecRegistries );
        }

        MongoCredential credential = null;

        final Map<String, String> env = System.getenv();

        String _username = StringUtils.isBlank(username) ? env.get(usernameEnvName) : null;
        String _password = StringUtils.isBlank(password) ? env.get(passwordEnvName) : null;

        if (_username != null) {
            if (StringUtils.isBlank(_username)) throw new IllegalStateException();
            if (StringUtils.isBlank(_password)) throw new IllegalStateException();
            credential = MongoCredential.createCredential( _username, databaseName, _password.toCharArray() );
        }

        final String usernamePasswordString;
        {
            if ( ! StringUtils.isBlank( _username ) ) {
                if ( StringUtils.isBlank( _password ) ) throw new UnsupportedOperationException();
                usernamePasswordString = _username + ":" + _password + "@";
            } else {
                usernamePasswordString = "";
            }
        }

        final String connectionString =
                "mongodb+srv://"
                        + usernamePasswordString
                        + clusterName // "cluster0.clwdy.mongodb.net"
                        + "/" + databaseName
                        + "?retryWrites=true&w=majority";

        // System.out.println("connection string: " + connectionString);

        MongoClientSettings mongoClientSettings = null;
        {
            MongoClientSettings.Builder mongoClientSetingsBuilder = MongoClientSettings.builder();

            if (connectionString != null) mongoClientSetingsBuilder.applyConnectionString(new ConnectionString(connectionString));
            if (pojoCodecRegistry != null) mongoClientSetingsBuilder.codecRegistry(pojoCodecRegistry);
            // if (credential != null) mongoClientSetingsBuilder.credential(credential); // we skip this because credentials are part of the connection string
            if (serverAddresses != null && ! serverAddresses.isEmpty()) mongoClientSetingsBuilder.applyToClusterSettings((b) -> b.hosts(serverAddresses));
            if (replicaSetName != null) mongoClientSetingsBuilder.applyToClusterSettings((b) -> b.requiredReplicaSetName(replicaSetName));

            mongoClientSettings = mongoClientSetingsBuilder.build();
        }

        final MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        db = mongoClient.getDatabase( databaseName );
        return new MongoClientAndDatabase(mongoClient, db);

    }
//
//        final MongoClientOptions options;
//        {
//            MongoClientOptions.Builder optsBuilder = MongoClientOptions.builder()
//                    .codecRegistry(pojoCodecRegistry)
//        //            .serverSelector(new WritableServerSelector());
//                    .serverSelector(new ServerSelector() {
//                        @Override
//                        public List<ServerDescription> select(final ClusterDescription clusterDescription) {
//                            for (int i = 0; i < 30; i++) {
//                                for (ServerDescription serverDescription : clusterDescription.getServerDescriptions()) {
//                                    final ServerConnectionState state = serverDescription.getState();
//                                    if (!ServerConnectionState.CONNECTED.equals(state)) continue;
//                                    return Arrays.asList(serverDescription);
//                                }
//                                try {
//                                    Thread.sleep(1000);
//                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
//                                }
//                            }
//                            return null;
//                        }
//                    });
//                    //.serverSelector(new PrimaryServerSelector());
//
//            if (replicaSetName != null) {
//                optsBuilder = optsBuilder.requiredReplicaSetName(replicaSetName);
//            }
//
//            options = optsBuilder.build();
//        }
//
//        final MongoClient mongoClient =
//                credential != null ?
//                        new MongoClient( serverAddresses, credential, options )
//                        : new MongoClient( serverAddresses, options );
//
////        try {
////            Thread.sleep( 10_000L );
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
//
////        for (String listDatabaseName : mongoClient.listDatabaseNames()) {
////            System.out.println("database name: " + listDatabaseName);
////        }
//
//        db = mongoClient.getDatabase( databaseName );
//
//        return new MongoClientAndDatabase(mongoClient, db);
//

//        {
//            for (ServerAddress serverAddress : serverAddresses) {
//            try {
//                final MongoClient client = credential != null ?
//                        new MongoClient(serverAddress, credential, options) :
//                        new MongoClient(serverAddress, options);
//
//                db = client.getDatabase(databaseName);
//
//
//                // db.getCollection("wave3").find().limit(1);
//                // StorageService.getInstance().getWave3DAO().findActive( c -> {} );
//                db.listCollectionNames();
//                log.info("success connecting to Mongo DB with server address: {}", serverAddress);
//                break;
//            } catch (Exception e) {
//                log.debug("failure connecting to Mongo DB with server address: {}", serverAddress);
//                log.trace("exception: {}", e);
//            }
//        }
//        if (db == null) throw new IllegalStateException();
//        return db;

}
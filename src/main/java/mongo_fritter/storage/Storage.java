package mongo_fritter.storage;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
\
public class Storage
{

    private static final Logger log = LoggerFactory.getLogger(Storage.class);

    private final String databaseName;

    private final MongoDatabase database;

    private List<ServerAddress> serverAddresses = new ArrayList<>();

    private final List<String> packageNames = Collections.synchronizedList(new ArrayList<>());
    private final List<Codec> codecs = new ArrayList<>();

    public void addCodec(final Codec codec) {
        Objects.requireNonNull(codec);

        if (!codecs.contains(codec)) {

            // check for same class
            final Class cl = codec.getClass();
            for (Codec codec1 : codecs) {
                if (cl.equals(codec1.getClass())) {
                    log.warn( "another codec already exists with the same class but was not detected as equal (this may not be a problem, or equals method may need to be implemented; most users can safely turn off this warning): ?", cl );
                }
            }

            codecs.add(codec);
        }
    }

    public void addPackageName(final String packageName) {
        if (StringUtils.isBlank(packageName)) throw new IllegalArgumentException();

        if (packageNames.contains(packageName)) {
            log.debug("package already added; this may not be a problem");
        }

        packageNames.add(packageName);
    }

    private MongoDatabase connect()
    {

        final CodecProvider pojoCodecProvider;
        {
            final PojoCodecProvider.Builder builder = PojoCodecProvider.builder();
            packageNames.forEach(builder::register);
            pojoCodecProvider = builder.build();
        }

        final CodecRegistry pojoCodecRegistry;
        {
            final List<CodecRegistry> codecRegistries = new ArrayList<>();
            codecRegistries.add( MongoClient.getDefaultCodecRegistry() );
            for (final Codec codec : codecs) {
                codecRegistries.add( CodecRegistries.fromCodecs(codec) );
            }
            codecRegistries.add( CodecRegistries.fromProviders(pojoCodecProvider) );

            pojoCodecRegistry = CodecRegistries.fromRegistries(
                    codecRegistries.toArray(new CodecRegistry[]{})
            );
        }

        // final String username = System.getenv("MONGODB_USERNAME");
        // final String databaseName = System.getenv("MONGODB_DATABASE");
        // final String password = System.getenv("MONGODB_PASSWORD");

        // final MongoCredential credential =
        //        MongoCredential.createCredential(username, databaseName, password.toCharArray());

        final MongoClientOptions options = MongoClientOptions.builder()
                .codecRegistry(pojoCodecRegistry)
                .build();

        // final MongoClient client =
        //        new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017)), credential, options);

        final ServerAddress[] serverAddresses = new ServerAddress[] {
                new ServerAddress("localhost", 27017),
                new ServerAddress("127.0.0.1", 27017),
                new ServerAddress("172.17.0.1", 27017) // for Docker on a Linux (Ubuntu) host
        };

        MongoDatabase db = null;

        for (ServerAddress serverAddress : serverAddresses) {
            try {
                final MongoClient client = new MongoClient(serverAddress, options);
                db = client.getDatabase(databaseName);
                // db.getCollection("wave3").find().limit(1);
                // StorageService.getInstance().getWave3DAO().findActive( c -> {} );
                db.listCollectionNames();
                log.info("success connecting to Mongo DB with server address: {}", serverAddress);
                break;
            } catch (Exception e) {
                log.debug("failure connecting to Mongo DB with server address: {}", serverAddress);
                log.trace("exception: {}", e);
            }
        }
        if (db == null) throw new IllegalStateException();
        return db;
    }

    Storage(final String databaseName, final String[] hostNames, final int[] ports) throws StorageException {
        if (databaseName != null && StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();
        if (databaseName != null ) {
            this.databaseName = databaseName;
        } else {
            final String envVarDatabaseName = System.getenv("MONGODB_DATABASE_NAME");
            if (envVarDatabaseName == null) {
                throw new NoDatabaseNameSpecifiedStorageException("could not get database name");
            } else if (StringUtils.isBlank(envVarDatabaseName)) {
                throw new NoDatabaseNameSpecifiedStorageException("could not get database name");
            }
            this.databaseName = envVarDatabaseName;
        }
        if (hostNames == null) throw new IllegalArgumentException();
        if (hostNames.length == 0) throw new IllegalArgumentException();
        for (final String hostName : hostNames) {
            if (StringUtils.isBlank(hostName)) throw new IllegalArgumentException();
        }
        if (ports == null) throw new IllegalArgumentException();
        if (ports.length == 0) throw new IllegalArgumentException();
        for (final int port : ports) {
            if (port < 0) throw new IllegalArgumentException();
            if (port == 0) throw new IllegalArgumentException();
        }
        if (hostNames.length != ports.length) throw new IllegalArgumentException();


        for (int i = 0; i < hostNames.length; i++) {
            final ServerAddress serverAddress = new ServerAddress( hostNames[i], ports[i] );
            serverAddresses.add( serverAddress );
        }

        final MongoDatabase database = connect();
        this.database = database;
    }

    public SortedSet<String> getCollectionNames()
    {
        final SortedSet<String> collectionNames = new TreeSet<>();
        for (final String collectionName : database.listCollectionNames())
            collectionNames.add(collectionName);
        return collectionNames;
    }

    public MongoDatabase getDatabase() {
        return database;
    }

}

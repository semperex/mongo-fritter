package mongo_fritter.test;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AbstractFlapdoodleTestBase {

    private static final Logger log = LoggerFactory.getLogger(AbstractFlapdoodleTestBase.class);

    private final String mongoDBHostName = "localhost";

    private final int mongoDBPort;
    {
        try {
            mongoDBPort = Network.getPreferredFreeServerPort( 27042 );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final String mongoDBDatabaseName = "local";

    private volatile MongodExecutable mongodExecutable;

    protected String getMongoDBHostName() {
        return mongoDBHostName;
    }

    protected int getMongoDBPort() {
        return mongoDBPort;
    }

    protected String getMongoDBDatabaseName() {
        return mongoDBDatabaseName;
    }

    protected MongodExecutable getMongodExecutable() {
        return mongodExecutable;
    }

    protected synchronized void createServer() throws IOException {
        final MongodStarter starter = MongodStarter.getDefaultInstance();

        final MongodConfig mongodConfig = MongodConfig.builder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(mongoDBPort, Network.localhostIsIPv6()))
                .build();

        try {
            mongodExecutable = starter.prepare(mongodConfig);
            final MongodProcess mongod = mongodExecutable.start();

//            try (final MongoClient mongo = new MongoClient(mongoDBHostName, mongoDBPort)) {
//                final DB db = mongo.getDB(mongoDBDatabaseName);
//                final DBCollection col = db.createCollection("testCol", new BasicDBObject());
//                col.save(new BasicDBObject("testDoc", new Date()));
//            }

        } finally {
        }
    }

    protected synchronized void destroyServer() {
        if (mongodExecutable != null)
            mongodExecutable.stop();
    }


}

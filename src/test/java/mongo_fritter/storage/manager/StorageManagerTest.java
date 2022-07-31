package mongo_fritter.storage.manager;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import mongo_fritter.dao.DAOException;
import mongo_fritter.dao.StorageBackedDAOBase;
import mongo_fritter.data_source.DataSource;
import mongo_fritter.data_source.DataSourceBuilder;
import mongo_fritter.model.AbstractModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.*;

public class StorageManagerTest {

    private static final Logger log = LoggerFactory.getLogger(StorageManagerTest.class);

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

    public static class ModelImpl extends AbstractModel<Long> {
        public ModelImpl(Long id) {
            super(id);
        }

        public ModelImpl() {
        }

    }

    public static class DAOImpl extends StorageBackedDAOBase<ModelImpl,Long> {

        public DAOImpl(final String primaryCollectionName, final String dataSourceName) throws DAOException {
            super(primaryCollectionName, dataSourceName);
        }

        public DAOImpl() throws DAOException {
        }

        @Override
        public Class getModelClass() {
            return ModelImpl.class;
        }

        @Override
        public Class getGenericDAOClass() {
            return this.getClass();
        }

    }


    @BeforeMethod
    public void setUp() throws IOException {

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

    @AfterMethod
    public void tearDown() {

        if (mongodExecutable != null)
            mongodExecutable.stop();

    }

    @Test
    public void testLoadDatabase() {
        final String dataSourceName = "test_data_source";

        final StorageManager storageManager;
        {
            StorageManager.getInstance();

            final DataSourceBuilder dataSourceBuilder = DataSourceBuilder.aDataSource()
                    .withName(dataSourceName)
                    .withServerAddresses(List.of(new DataSource.ServerAddress(mongoDBHostName,mongoDBPort)))
                    .withDatabaseName(mongoDBDatabaseName);

            final DataSource dataSource = dataSourceBuilder.build();

            storageManager = StorageManager.getInstance();

            storageManager.newStorage(dataSource);
        }

    }

    @Test
    public void testLoadDatabaseAndInteract1() throws DataSourceNotRegisteredStorageManagerException, DataSourceRegistrationStorageManagerException, DAOException {
        final String dataSourceName = "test_data_source";

        final StorageManager storageManager;
        {
            StorageManager.getInstance();

            final DataSourceBuilder dataSourceBuilder = DataSourceBuilder.aDataSource()
                    .withName(dataSourceName)
                    .withServerAddresses(List.of(new DataSource.ServerAddress(mongoDBHostName,mongoDBPort)))
                    .withDatabaseName(mongoDBDatabaseName);

            final DataSource dataSource = dataSourceBuilder.build();

            storageManager = StorageManager.getInstance();

            storageManager.registerDataSource(dataSource);
        }

        final DAOImpl daoImpl = new DAOImpl(null, dataSourceName);

        daoImpl.create(new ModelImpl(1L));

        final ModelImpl daoImplRetrieved = daoImpl.findById(1L);

        log.info("retrieved: {}", daoImplRetrieved);

        Assert.assertEquals( daoImplRetrieved.getId(), 1L );

    }

    @Test
    public void testLoadDatabaseAndInteract2() throws DataSourceNotRegisteredStorageManagerException, DataSourceRegistrationStorageManagerException, DAOException {

        final StorageManager storageManager;
        {
            StorageManager.getInstance();

            final DataSourceBuilder dataSourceBuilder = DataSourceBuilder.aDataSource()
                    .withServerAddresses(List.of(new DataSource.ServerAddress(mongoDBHostName,mongoDBPort)))
                    .withDatabaseName(mongoDBDatabaseName);

            final DataSource dataSource = dataSourceBuilder.build();

            storageManager = StorageManager.getInstance();

            storageManager.registerDataSource(dataSource);
        }

        final DAOImpl daoImpl = new DAOImpl();

        daoImpl.create(new ModelImpl(1L));

        final ModelImpl daoImplRetrieved = daoImpl.findById(1L);

        log.info("retrieved: {}", daoImplRetrieved);

        Assert.assertEquals( daoImplRetrieved.getId(), 1L );

    }

}
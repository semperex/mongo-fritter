package mongo_fritter.storage.manager;

import de.flapdoodle.os.common.io.IO;
import mongo_fritter.dao.DAOException;
import mongo_fritter.dao.StorageBackedDAOBase;
import mongo_fritter.data_source.DataSource;
import mongo_fritter.data_source.DataSourceBuilder;
import mongo_fritter.model.AbstractModel;
import mongo_fritter.test.AbstractFlapdoodleTestBase;
import mongo_fritter.test.StorageManagerTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.*;

public class StorageManagerTest extends StorageManagerTestBase {

    private static final Logger log = LoggerFactory.getLogger(StorageManagerTest.class);

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

    @BeforeClass
    public void beforeclass() throws IOException, DataSourceRegistrationStorageManagerException {
        final String dataSourceName = null;
        setupStorage(dataSourceName);
    }

    @BeforeMethod
    public void setUp() throws IOException {
        createServer();
    }

    @AfterMethod
    public void tearDown() {
        destroyServer();
    }

    @Test
    public void testLoadDatabase() throws DataSourceRegistrationStorageManagerException {
        final String dataSourceName = "test_data_source_1";
        setupStorage(dataSourceName);
    }

    @Test
    public void testLoadDatabaseAndInteract1() throws DataSourceNotRegisteredStorageManagerException, DataSourceRegistrationStorageManagerException, DAOException {
        final String dataSourceName = "test_data_source_2";
        setupStorage(dataSourceName);

        final DAOImpl daoImpl = new DAOImpl(null, dataSourceName);

        daoImpl.create(new ModelImpl(1L));

        final ModelImpl daoImplRetrieved = daoImpl.findById(1L);

        log.info("retrieved: {}", daoImplRetrieved);

        Assert.assertEquals( daoImplRetrieved.getId(), 1L );

    }

    @Test
    public void testLoadDatabaseAndInteract2() throws DataSourceNotRegisteredStorageManagerException, DataSourceRegistrationStorageManagerException, DAOException {
        final DAOImpl daoImpl = new DAOImpl();

        daoImpl.create(new ModelImpl(1L));

        final ModelImpl daoImplRetrieved = daoImpl.findById(1L);

        log.info("retrieved: {}", daoImplRetrieved);

        Assert.assertEquals( daoImplRetrieved.getId(), 1L );

    }

}
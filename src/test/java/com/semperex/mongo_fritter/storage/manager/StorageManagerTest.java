package com.semperex.mongo_fritter.storage.manager;

import com.semperex.mongo_fritter.dao.DAOException;
import com.semperex.mongo_fritter.test.StorageManagerTestBase;
import com.semperex.mongo_fritter.dao.StorageBackedDAOBase;
import com.semperex.mongo_fritter.model.AbstractModel;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;

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


    public static class DAOImpl2 extends StorageBackedDAOBase<ModelImpl,Long> {

        public DAOImpl2(final String primaryCollectionName, final String dataSourceName) throws DAOException {
            super(primaryCollectionName, dataSourceName);
        }

        public DAOImpl2() throws DAOException {
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

    public static class Model2Codec implements Codec {
        @Override
        public Object decode(BsonReader reader, DecoderContext decoderContext) {
            return null;
        }

        @Override
        public void encode(BsonWriter writer, Object value, EncoderContext encoderContext) {

        }

        @Override
        public Class getEncoderClass() {
            return null;
        }
    }

    @BeforeClass
    public void setUp() throws IOException, DataSourceRegistrationStorageManagerException {
        createServer();

//        final String dataSourceName = null;
//        setupStorage(dataSourceName);
    }

    @AfterClass
    public void tearDown() {
        destroyServer();
    }

    @Test(enabled = false)
    public void testLoadDatabase() throws DataSourceRegistrationStorageManagerException {
        final String dataSourceName = "test_data_source_1";
        setupStorage(dataSourceName);
    }

    @Test(enabled = false)
    public void testLoadDatabaseAndInteract1() throws DataSourceNotRegisteredStorageManagerException, DataSourceRegistrationStorageManagerException, DAOException {
        final String dataSourceName = "test_data_source_2";
        setupStorage(dataSourceName);

        final DAOImpl daoImpl = new DAOImpl(null, dataSourceName);

        final long id = 1L;

        daoImpl.create(new ModelImpl(id));

        final ModelImpl daoImplRetrieved = daoImpl.findById(id);

        log.info("retrieved: {}", daoImplRetrieved);

        Assert.assertEquals( daoImplRetrieved.getId(), id );

    }

    @Test(enabled = false)
    public void testLoadDatabaseAndInteract2() throws DataSourceNotRegisteredStorageManagerException, DataSourceRegistrationStorageManagerException, DAOException {
        final String dataSourceName = "test_data_source_3";
        setupStorage(dataSourceName);

        final DAOImpl daoImpl = new DAOImpl();;

        final long id = 2L;

        daoImpl.create(new ModelImpl(id));

        final ModelImpl daoImplRetrieved = daoImpl.findById(id);

        log.info("retrieved: {}", daoImplRetrieved);

        Assert.assertEquals( daoImplRetrieved.getId(), id );

    }

}
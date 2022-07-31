package mongo_fritter.dao;

import com.mongodb.client.MongoDatabase;
import mongo_fritter.model.Model;
import mongo_fritter.storage.Storage;
import mongo_fritter.storage.manager.DataSourceNotRegisteredStorageManagerException;
import mongo_fritter.storage.manager.StorageManager;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.Codec;

import java.util.Collection;

public abstract class StorageBackedDAOBase<T extends Model<IdT>, IdT> extends DAOBase<T,IdT> {

    private static final StorageManager storageManager = StorageManager.getInstance();

    private final String dataSourceName;
    private final Storage storage;

    public StorageBackedDAOBase(final String primaryCollectionName, final String dataSourceName) throws DAOException {
        super(primaryCollectionName);

        if (dataSourceName != null && StringUtils.isBlank(dataSourceName)) throw new IllegalArgumentException();

        this.dataSourceName = dataSourceName;
        try {
            this.storage = dataSourceName != null ?
                    storageManager.getStorage( dataSourceName ) :
                    storageManager.getStorage();
        } catch (DataSourceNotRegisteredStorageManagerException e) {
            throw new DAOException(e);
        }

        final Collection<? extends Codec> codecClasses = getCodecs();
        if (codecClasses != null) {
            for (final Codec codec : codecClasses) {
                storage.addCodec(codec);
            }
        }

        final String packageName = getModelClass().getPackage().getName();
        storage.addPackageName(packageName);

        storage.refresh();
    }

    public StorageBackedDAOBase(final String primaryCollectionName) throws DAOException {
        this(primaryCollectionName, null);
    }

    public StorageBackedDAOBase() throws DAOException {
        this(null, null);
    }

    @Override
    protected MongoDatabase getDatabase() {
        return storage.getDatabase();
    }

}

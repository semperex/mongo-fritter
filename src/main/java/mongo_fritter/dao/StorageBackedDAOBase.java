package mongo_fritter.dao;

import com.mongodb.client.MongoDatabase;
import mongo_fritter.model.Model;
import mongo_fritter.storage.Storage;
import mongo_fritter.storage.StorageManager;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.Codec;

import java.util.Collection;

public abstract class StorageBackedDAOBase<T extends Model<IdT>, IdT> extends DAOBase<T,IdT> {

    private static final StorageManager storageManager = StorageManager.getInstance();

    private final String storageName;
    private final Storage storage;

    public StorageBackedDAOBase(final String primaryCollectionName, final String storageName) {
        super(primaryCollectionName);

        if (storageName != null && StringUtils.isBlank(storageName)) throw new IllegalArgumentException();

        this.storageName = storageName;
        this.storage = storageName != null ?
                storageManager.getStorage( storageName ) :
                storageManager.getStorage();

        final Collection<? extends Codec> codecClasses = getCodecs();
        if (codecClasses != null) {
            for (final Codec codec : codecClasses) {
                storage.addCodec(codec);
            }
        }

        final String packageName = getModelClass().getPackage().getName();
        storage.addPackageName(packageName);
    }

    public StorageBackedDAOBase(final String primaryCollectionName) {
        this(primaryCollectionName, null);
    }

    public StorageBackedDAOBase() {
        this(null, null);
    }

    @Override
    protected MongoDatabase getDatabase() {
        return storage.getDatabase();
    }

}

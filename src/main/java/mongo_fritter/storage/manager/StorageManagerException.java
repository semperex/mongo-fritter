package mongo_fritter.storage.manager;

import mongo_fritter.storage.StorageException;

public class StorageManagerException extends StorageException {

    public StorageManagerException() {
    }

    public StorageManagerException(String message) {
        super(message);
    }

    public StorageManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageManagerException(Throwable cause) {
        super(cause);
    }

    public StorageManagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

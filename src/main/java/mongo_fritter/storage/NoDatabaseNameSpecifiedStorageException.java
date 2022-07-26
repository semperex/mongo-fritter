package mongo_fritter.storage;

public class NoDatabaseNameSpecifiedStorageException extends StorageException {
    public NoDatabaseNameSpecifiedStorageException() {
    }

    public NoDatabaseNameSpecifiedStorageException(String message) {
        super(message);
    }

    public NoDatabaseNameSpecifiedStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoDatabaseNameSpecifiedStorageException(Throwable cause) {
        super(cause);
    }

    public NoDatabaseNameSpecifiedStorageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

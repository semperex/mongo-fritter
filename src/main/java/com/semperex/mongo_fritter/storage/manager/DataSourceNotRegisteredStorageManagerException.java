package com.semperex.mongo_fritter.storage.manager;

public class DataSourceNotRegisteredStorageManagerException extends StorageManagerException {

    public DataSourceNotRegisteredStorageManagerException() {
    }

    public DataSourceNotRegisteredStorageManagerException(String message) {
        super(message);
    }

    public DataSourceNotRegisteredStorageManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSourceNotRegisteredStorageManagerException(Throwable cause) {
        super(cause);
    }

    public DataSourceNotRegisteredStorageManagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

package mongo_fritter.storage;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public final class StorageBuilder {
    private String databaseName;

    private final List<String> hostNames = new ArrayList<>();

    private final IntList ports = new IntArrayList();

    private StorageBuilder() {
    }

    public static StorageBuilder aStorage() {
        return new StorageBuilder();
    }

    public StorageBuilder withDatabaseName(String databaseName) {
        if (StringUtils.isBlank(databaseName)) throw new IllegalArgumentException();
        this.databaseName = databaseName;
        return this;
    }

    public StorageBuilder withDatabaseServer(final String hostName, final int port) {
        if (StringUtils.isBlank(hostName)) throw new IllegalArgumentException();
        if (port < 0) throw new IllegalArgumentException();
        if (port == 0) throw new IllegalArgumentException();

        hostNames.add(hostName);
        ports.add(port);

        return this;
    }

    public StorageBuilder withLocalDatabaseServer

    public Storage build() {
        try {
            return new Storage(databaseName, hostNames.toArray(new String[]{}), ports.toIntArray());
        } catch (StorageException e) {
            throw new RuntimeException(e); // TODO
        }
    }

}

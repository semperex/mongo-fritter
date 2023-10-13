package com.semperex.mongo_fritter.util;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.semperex.mongo_fritter.model.Model;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class MongoDAOUtil {

    public static SortedSet<String> getCollectionNames(final MongoDatabase database) {
        final SortedSet<String> collectionNames = new TreeSet<>();
        for (final String collectionName : database.listCollectionNames())
            collectionNames.add(collectionName);
        return collectionNames;
    }

    public static <T> T insertOneWithUniqueUUIDBytes(
            final MongoCollection collection,
            final String idField,
            final String setIdMethodName,
            final T object,
            final int maxTries) throws Exception
    {

        Objects.requireNonNull(collection);
        if (StringUtils.isBlank(idField)) throw new IllegalArgumentException();
        if (StringUtils.isBlank(setIdMethodName)) throw new IllegalArgumentException();
        if (maxTries < 0) throw new IllegalArgumentException();
        if (maxTries == 0) throw new IllegalArgumentException();
        Objects.requireNonNull(object);

        byte[] uuid = null;

        for (int i = 0; i < maxTries; i++) {
            final byte[] _uuid = UUID.randomUUID().toString().getBytes();
            final FindIterable iterable = collection.find(Filters.eq(idField, _uuid));
            if (!iterable.iterator().hasNext()) {
                uuid = _uuid;
                break;
            }
        }

        if ( uuid == null ) {
            throw new IllegalStateException("max tries exceeded and unable to generate unique id -- this should be unlikely like the earth crashing into the sun tomorrow");
        }

        final Method getIdMethod;
        try {
            getIdMethod = object.getClass().getMethod(setIdMethodName, byte[].class);
        } catch (NoSuchMethodException e) {
            throw new Exception(e);
        }

        try {
            getIdMethod.invoke(object, uuid);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new Exception(e);
        }

        collection.insertOne( object );

        return object;

    }

    public static <T> T insertOneWithUniqueUUIDBytes(
            final MongoCollection collection,
            final String idField,
            final String setIdMethodName,
            final T object) throws Exception
    {
        return insertOneWithUniqueUUIDBytes( collection, idField, setIdMethodName, object, 10 );
    }

    public static <T extends Model<byte[]>> T insertOneWithUniqueUUIDBytes(
            final MongoCollection collection,
            final T object) throws Exception
    {
        return insertOneWithUniqueUUIDBytes( collection, "id", "setId", object );
    }

    public static <T extends Model<byte[]>> T insertOneWithUniqueUUIDBytes(
            final MongoCollection collection,
            final String idField,
            final T object) throws Exception
    {
        return insertOneWithUniqueUUIDBytes( collection, idField, "setId", object );
    }

    public static <D> Document getIndex(MongoCollection<D> collection, String indexName) {
        for (Document index : collection.listIndexes()) {
            Object o = index.get("key");
            if (o instanceof Document) {
                if (((Document) o).containsKey(indexName)) {
                    return (Document) index;
                }/*from w  w w  .j  ava2s  .c  o m*/
            }
        }
        return null;
    }

    public static Bson stringArrayToOrFilter(final String fieldName, final String[] values) {
        if (StringUtils.isBlank(fieldName)) throw new IllegalArgumentException();
        final Bson valuesFilter;
        if (values != null && values.length > 0) {
            final List<Bson> vFilters = new ArrayList<>();
            for (final String v : values) {
                vFilters.add(Filters.eq(fieldName, v));
            }
            valuesFilter = Filters.or(vFilters);
        } else {
            valuesFilter = null;
        }
        return valuesFilter;
    }

}

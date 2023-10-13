package com.semperex.mongo_fritter.dao;

import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import com.semperex.mongo_fritter.model.Model;
import com.semperex.mongo_fritter.util.MongoDAOUtil;
import com.semperex.mongo_fritter.util.MongoDBPOJOConnectionCreator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class DAOBase<T extends Model<IdT>, IdT> implements DAO<T,DAO,IdT> {

    private static final Logger log = LoggerFactory.getLogger(DAOBase.class);

    private final MongoDBPOJOConnectionCreator.MongoClientAndDatabase mongo;

    protected MongoDatabase getDatabase() {
        return mongo.getDatabase();
    }

    protected ClientSession newClientSession() {
        return mongo.getClient().startSession();
        //throw new UnsupportedOperationException("this method has not been implemented -- session not supported");
    }

    private final String primaryCollectionName;

    public DAOBase(final String primaryCollectionName) {
        if (primaryCollectionName != null && StringUtils.isBlank(primaryCollectionName)) throw new IllegalArgumentException();
        this.primaryCollectionName = primaryCollectionName;
        this.mongo = null;
    }

    public DAOBase() {
        this.primaryCollectionName = null;
        this.mongo = null;
    }

    public DAOBase(final MongoDBPOJOConnectionCreator connectionCreator, final String primaryCollectionName) {
        if (primaryCollectionName != null && StringUtils.isBlank(primaryCollectionName)) throw new IllegalArgumentException();
        this.primaryCollectionName = primaryCollectionName;
        this.mongo = connectionCreator.connect();
    }

    public DAOBase(final MongoDBPOJOConnectionCreator connectionCreator) {
        this(connectionCreator, null);
    }

    protected String getPrimaryCollectionName() {
        if (primaryCollectionName != null) {
            assert ! StringUtils.isBlank(primaryCollectionName);
            return primaryCollectionName;
        }
        return getModelClass().getSimpleName().toLowerCase();
    }

    protected MongoCollection<T> getPrimaryCollection() {
        return getDatabase().getCollection( getPrimaryCollectionName(), getModelClass() );
    }

    protected String getPrimaryIdFieldName() {
        return "_id";
    }

    protected void initializeCollection() {
        final CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
        // TODO: custom _id ?

        final MongoDatabase database = getDatabase();
        final String collectionName = getPrimaryCollectionName();

        final Set<String> existingCollectionNames = MongoDAOUtil.getCollectionNames( database );

        if ( ! existingCollectionNames.contains(collectionName) ) {
            database.createCollection(collectionName, createCollectionOptions);
            final MongoCollection<Document> collection = database.getCollection(collectionName);
            if ( collection == null ) throw new IllegalArgumentException();
        }
    }

    protected T newPrimaryModelInstance() throws Exception {
        return getModelClass().getDeclaredConstructor().newInstance();
    }

    private boolean createIndex(final Bson fieldBson, final boolean overwrite, final String indexName) throws DAOException {
        final MongoCollection collection = getPrimaryCollection();
        if (MongoDAOUtil.getIndex(collection, indexName) != null) {
            if (overwrite) {
                collection.dropIndex(indexName);
            } else {
                return false;
            }
        }
        collection.createIndex(fieldBson);
        return true;
    }

    protected boolean createIndex(final String fieldName, final boolean overwrite) throws DAOException {
        final String indexName = fieldName;
        return createIndex(Indexes.ascending(fieldName), overwrite, indexName);
    }

    /**
     * Creates an ascending index if it does not already exist.
     *
     * @param fieldName
     * @return
     * @throws DAOException
     */
    protected boolean createIndex(final String fieldName) throws DAOException {
        return createIndex(fieldName, false);
    }

    @Override
    public T create() throws DAOException {
        return create();  // TODO: fix this
    }

    @Override
    public T create(final T value) throws DAOException {
        Objects.requireNonNull( value );

        getPrimaryCollection().insertOne(value);
        return value;
    }

    @Override
    public void findAll(final Consumer<T> consumer) throws DAOException {
        getPrimaryCollection().find().limit(Integer.MAX_VALUE).forEach( consumer );
    }

    @Override
    public List<T> findAll() throws DAOException {
        final List<T> list = Collections.synchronizedList( new LinkedList<>() );
        findAll( list::add );
        return list;
    }

    @Override
    public T findById(final IdT id) throws DAOException {
        Objects.requireNonNull( id );

        final FindIterable<? extends T> iterable = getPrimaryCollection().find(Filters.eq(getPrimaryIdFieldName(), id)).limit(Integer.MAX_VALUE);
        final Iterator<? extends T> itr = iterable.iterator();
        if (!itr.hasNext()) return null;
        return itr.next();
    }


    private enum Op {
        FIND, COUNT
    }

    private static class CountingConsumerWrapper<T> implements Consumer<T> {

        private final Consumer base;
        private final LongAdder counter = new LongAdder();

        public CountingConsumerWrapper(final Consumer base) {
            this.base = base;
        }

        @Override
        public void accept(final Object o) {
            base.accept(o);
            counter.increment();
        }

        @Override
        public Consumer andThen(final Consumer after) {
            return base.andThen(after);
        }

        public long count() {
            final long x = counter.sum();
            assert x >= 0;
            return x;
        }

    }

    private Long findOrCountWithFilter(final Op[] ops,
                                       final Bson filter,
                                       final Consumer<T> consumer,
                                       final Integer limit,
                                       final Long skip,
                                       final Bson sort)
    {
        Objects.requireNonNull( ops );
        Objects.requireNonNull( filter );

        if (ops.length == 0) throw new IllegalArgumentException();

        assert limit == null || limit == -1 || limit > 0;
        if (limit != null) {
            if (limit == 0) throw new IllegalArgumentException();
            if (limit < -1) throw new IllegalArgumentException();
        }

        if (skip != null) {
            if (skip < 0) throw new IllegalArgumentException();
        }

        if (ArrayUtils.contains(ops, Op.FIND)) {
            Objects.requireNonNull(consumer);

            FindIterable<T> ts = getPrimaryCollection().find(filter);
            if (skip != null) {
                if (skip.intValue() != skip) throw new UnsupportedOperationException();
                ts = ts.skip(skip.intValue());
            }
            if (limit != null && limit != -1) {
                ts = ts.limit(limit);
            }
            if (sort != null) {
                ts = ts.sort(sort);
            }

            if (ArrayUtils.contains(ops, Op.COUNT)) {
                final CountingConsumerWrapper countingConsumerWrapper = new CountingConsumerWrapper(consumer);
                ts.forEach(countingConsumerWrapper);
                final long count = countingConsumerWrapper.count();
                assert count >= 0;
                return count;
            } else {
                ts.forEach(consumer);
                return null;
            }
        }

        if (ArrayUtils.contains(ops, Op.COUNT)) {
            final long count = getPrimaryCollection().count(filter);
            assert count >= 0;
            return count;
        }

        throw new IllegalStateException(new UnsupportedOperationException("no supported op found"));
    }

    protected Long countWithFilter(final Bson filter) {
        Objects.requireNonNull(filter);

        return findOrCountWithFilter(
                new Op[]{Op.COUNT},
                filter,
                null,
                null,
                null,
                null);
    }

    protected void findWithFilter(final Bson filter, final Consumer<T> consumer, final Integer limit, final Long skip, final Bson sort) {
        findOrCountWithFilter(
                new Op[]{Op.FIND},
                filter,
                consumer,
                limit,
                skip,
                sort);
    }

    protected void findWithFilter(final Bson filter,
                                  final Consumer<T> consumer,
                                  final Integer limit)
    {
        findWithFilter(filter, consumer, limit, null,null);
    }

    protected void findWithFilter(final Bson filter, final Consumer<T> consumer) {
        findWithFilter(filter, consumer, null);
    }

    protected Collection<T> findWithFilterToCollection(final Bson filter, final Integer limit, final Long skip, final Bson sort) {
        Objects.requireNonNull( filter );

        final List<T> results = Collections.synchronizedList( new ArrayList<>() );
        findWithFilter( filter, results::add, limit, skip, sort );
        return results;
    }

    protected Collection<T> findWithFilterToCollection(final Bson filter, final Integer limit, final Long skip) {
        return findWithFilterToCollection(filter, limit, skip, null);
    }

    public void findByField(final String fieldName, final Object fieldValue, final Consumer<T> consumer) {
        findWithFilter(Filters.eq(fieldName, fieldValue), consumer);
    }

    protected Collection<T> findWithFilterToCollection(final Bson filter) {
        return findWithFilterToCollection(filter, null, null);
    }

/*    public void findByStartTimeRange(final Range<Long> timeRangeMS, final Consumer<T> consumer ) {
        Objects.requireNonNull( timeRangeMS );
        Objects.requireNonNull( consumer );

        final Bson filter = FilterUtil.buildFilter("startTimeMS", timeRangeMS);
        findWithFilter( filter, consumer );
    }

    public Collection<T> findByStartTimeRange(final Range<Long> timeRangeMS ) {
        Objects.requireNonNull( timeRangeMS );

        final Bson filter = FilterUtil.buildFilter("startTimeMS", timeRangeMS);
        return findWithFilter( filter );
    }*/

    @Override
    public void deleteById(final IdT id) throws DAOException {
        Objects.requireNonNull( id );

        getPrimaryCollection().deleteOne(Filters.eq(getPrimaryIdFieldName(), id));
    }

    @Override
    public void update(final T value) throws DAOException {
        Objects.requireNonNull( value );

        getPrimaryCollection().replaceOne(
                Filters.eq(getPrimaryIdFieldName(), value.getId()), value );
    }

    @Override
    public void updateFields(final IdT id, final Map<String, Object> fields) throws DAOException {
        Objects.requireNonNull(id);
        Objects.requireNonNull(fields);

        if (log.isTraceEnabled()) {
            log.trace("id: " + id + "; fields: " +
                    StringUtils.join(
                            fields.entrySet().stream()
                                .map((entry) -> "k:" + entry.getKey() + "=v:" + entry.getValue())
                                .collect(Collectors.toUnmodifiableList()),
                            ","));
        }

        if (fields.size() == 0) throw new DAOException("nothing to update");

        final List<Bson> updates = new ArrayList<>(8);
        fields.forEach((k,v) -> {
            updates.add(Updates.set(k, v));
        });

        if (updates.size() == 0) {
            throw new IllegalStateException(new DAOException("nothing to update"));
        }

        final UpdateResult updateResult =
            getPrimaryCollection().updateOne(
                    Filters.eq(getPrimaryIdFieldName(), id),
                    updates.size() == 1 ? updates.get(0) : Updates.combine(updates)
            );

        log.trace("modified count: " + updateResult.getModifiedCount());
    }

    @Override
    public T createOrUpdate(final T value) throws DAOException {
        Objects.requireNonNull( value );

        getPrimaryCollection().replaceOne(
                Filters.eq(getPrimaryIdFieldName(), value.getId()), value, new ReplaceOptions().upsert(true));
        return value;
    }

    private static Method getPOJOGetMethodFromFieldName(final Class cls, final String fieldName, final boolean ignoreCase) {
        Objects.requireNonNull(cls);

        if (StringUtils.isBlank(fieldName)) throw new IllegalArgumentException();

        // input checking updateFieldName is valid characters only may be important re security to prevent arbitrary execution
        if (!StringUtils.isAlphanumeric(fieldName)) throw new IllegalArgumentException();

        // now, let's be paranoid just in case we screwed something up -- these *should* be extra, unnecessary checks
        if (fieldName.contains(".")) throw new IllegalArgumentException();
        if (StringUtils.containsWhitespace(fieldName)) throw new IllegalArgumentException();

        final String expectedMethodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1, fieldName.length());
        if (expectedMethodName.length() < 4) throw new IllegalStateException();
        assert StringUtils.isAlphanumeric(expectedMethodName);
        assert !StringUtils.containsWhitespace(fieldName);
        // TODO: This is slow (and likely repetitive). Cache or do it another away?
        Method result = null;
        for (final Method method: cls.getMethods()) {
            final String methodName = method.getName();
            if (ignoreCase) {
                if (expectedMethodName.equalsIgnoreCase(methodName)) {
                    if (result != null) throw new IllegalStateException("possible duplicate match for expected name " + expectedMethodName + " by method name " + methodName);
                    result = method;
                }
            } else {
                if (expectedMethodName.equalsIgnoreCase(methodName)) {
                    if (result != null) throw new IllegalStateException("possible duplicate match for expected name " + expectedMethodName + " by method name " + methodName);
                    result = method;
                }
            }
        }
        return result;
    }

    private Object getFieldValue(final Class cls, final T instance, final String fieldName) throws DAOException {
        final Method getMethod = getPOJOGetMethodFromFieldName(cls, fieldName, false);
        Object fieldValue;
        try {
            fieldValue = getMethod.invoke(instance);
        } catch (IllegalAccessException e) {
            throw new DAOException(e);
        } catch (InvocationTargetException e) {
            throw new DAOException(e);
        }
        if (cls.isPrimitive()) {
            if (cls.equals(boolean.class)) fieldValue = Boolean.valueOf((boolean)fieldValue);
            else if (cls.equals(byte.class)) fieldValue = Byte.valueOf((byte)fieldValue);
            else if (cls.equals(short.class)) fieldValue = Short.valueOf((short)fieldValue);
            else if (cls.equals(int.class)) fieldValue = Integer.valueOf((int)fieldValue);
            else if (cls.equals(long.class)) fieldValue = Long.valueOf((long)fieldValue);
            else if (cls.equals(float.class)) fieldValue = Float.valueOf((float)fieldValue);
            else if (cls.equals(double.class)) fieldValue = Double.valueOf((double)fieldValue);
        }
        return fieldValue;
    }

    public T createOrUpdateSpecificFieldsOnly(final T value, final Set<String> updateFieldNames, final Function<T,Boolean> filter) throws DAOException {
        Objects.requireNonNull( value );
        Objects.requireNonNull( updateFieldNames );
        assert updateFieldNames.size() >= 0;
        if (updateFieldNames.size() == 0) {
            log.warn("Running this method with no field names present is equivalent to createOrIgnore --> delegating to createOrIgnore");
            return createOrIgnore(value);
        }

        final List<Bson> updates = new ArrayList<>();

        for (final String updateFieldName : updateFieldNames) {
            if (StringUtils.isBlank(updateFieldName)) throw new DAOException("invalid field name -- was blank");
            if (!StringUtils.isAlphanumeric(updateFieldName)) throw new DAOException("invalid field name -- not alphanumeric");

            final Object updateFieldValue = getFieldValue(value.getClass(), value, updateFieldName);
            if (updateFieldValue == null) log.warn("update field value was null");
            log.trace("field name: {} and field value: {}", updateFieldName, updateFieldValue);
            try {
                updates.add(Updates.set(updateFieldName, updateFieldValue));
                log.trace("added updates to pre-query structure");
            } catch (Exception e) {
                log.error("error", e, e);
                throw new RuntimeException(e);
            }
        }

        final Bson uniqueFilter = Filters.eq(getPrimaryIdFieldName(), value.getId());

        final ClientSession session = newClientSession();
        session.startTransaction();

        // final long existingCount = getPrimaryCollection().countDocuments(uniqueFilter);

        final Collection<T> existing = findWithFilterToCollection(uniqueFilter, null, null);
        final long existingCount = existing.size();

        if (existingCount > 1) throw new DAOException("existing count greater than 1 indicating something has gone wrong as the filter is supposed to be unique");
        assert existingCount == 0 || existingCount == 1;

        if (existingCount == 0) {
            log.trace("inserting one");
            getPrimaryCollection().insertOne(session, value);
        } else if (existingCount == 1) {
            if (filter == null || filter.apply(existing.stream().findFirst().get())) {
                log.trace("updating one with updates: {}", updates);
                getPrimaryCollection().updateOne(
                        session,
                        uniqueFilter,
                        Updates.combine(updates)
                );
            }
        } else {
            throw new IllegalStateException();
        }

        session.commitTransaction();

        return value;
    }

    public T createOrUpdateSpecificFieldsOnly(final T value, final Set<String> updateFieldNames) throws DAOException {
        return createOrUpdateSpecificFieldsOnly(value, updateFieldNames, null);
    }

        @Override
    public T createOrIgnore(final T value) throws DAOException {
        Objects.requireNonNull( value );

        // TODO: this is not transactionally safe
        final long count = getPrimaryCollection().countDocuments( Filters.eq( getPrimaryIdFieldName(), value.getId() ) );
        if ( count < 0 ) throw new DAOException( new IllegalStateException() );
        if ( count > 1 ) throw new DAOException( new IllegalStateException() );
        if ( count == 1 ) return null;

        getPrimaryCollection().insertOne( value );
        return value;
    }

}

package com.semperex.mongo_fritter.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import com.semperex.mongo_fritter.model.Model;
import com.semperex.mongo_fritter.util.MongoDAOUtil;
import com.semperex.mongo_fritter.util.MongoDBPOJOConnectionCreator;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

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

    protected void findWithFilter(final Bson filter, final Consumer<T> consumer, final Integer limit, final Bson sort) {
        Objects.requireNonNull( filter );
        Objects.requireNonNull( consumer );

        assert limit == null || limit == -1 || limit > 0;
        if (limit != null) {
            if (limit == 0) throw new IllegalArgumentException();
            if (limit < -1) throw new IllegalArgumentException();
        }

        FindIterable<T> ts = getPrimaryCollection().find(filter);
        if (limit != null && limit != -1) {
            ts = ts.limit(limit);
        }
        if (sort != null) {
            ts = ts.sort(sort);
        }

        ts.forEach( consumer );
    }

    protected void findWithFilter(final Bson filter,
                                  final Consumer<T> consumer,
                                  final Integer limit)
    {
        findWithFilter(filter, consumer, limit, null);
    }

    protected void findWithFilter(final Bson filter, final Consumer<T> consumer) {
        findWithFilter(filter, consumer, null);
    }

    protected Collection<T> findWithFilterToCollection(final Bson filter, final Integer limit, final Bson sort) {
        Objects.requireNonNull( filter );

        final List<T> results = Collections.synchronizedList( new ArrayList<>() );
        findWithFilter( filter, results::add, limit, sort );
        return results;
    }

    protected Collection<T> findWithFilterToCollection(final Bson filter, final Integer limit) {
        return findWithFilterToCollection(filter, limit, null);
    }

    public void findByField(final String fieldName, final Object fieldValue, final Consumer<T> consumer) {
        findWithFilter(Filters.eq(fieldName, fieldValue), consumer);
    }

    protected Collection<T> findWithFilterToCollection(final Bson filter) {
        return findWithFilterToCollection(filter, null);
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
        final Object fieldValue;
        try {
            fieldValue = getMethod.invoke(instance);
        } catch (IllegalAccessException e) {
            throw new DAOException(e);
        } catch (InvocationTargetException e) {
            throw new DAOException(e);
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

        final List<Bson> updates = List.of();

        for (final String updateFieldName : updateFieldNames) {
            if (StringUtils.isBlank(updateFieldName)) throw new DAOException("invalid field name -- was blank");
            if (!StringUtils.isAlphanumeric(updateFieldName)) throw new DAOException("invalid field name -- not alphanumeric");

            final Object updateFieldValue = getFieldValue(value.getClass(), value, updateFieldName);
            if (updateFieldValue == null) log.warn("update field value was null");
            log.trace("field name: {} and field value: {}", updateFieldName, updateFieldValue);
            updates.add(Updates.set(updateFieldName, updateFieldValue));
        }

        final Bson uniqueFilter = Filters.eq(getPrimaryIdFieldName(), value.getId());

        final ClientSession session = newClientSession();
        session.startTransaction();

        // final long existingCount = getPrimaryCollection().countDocuments(uniqueFilter);

        final Collection<T> existing = findWithFilterToCollection(uniqueFilter, null);
        final long existingCount = existing.size();

        if (existingCount > 1) throw new DAOException("existing count greater than 1 indicating something has gone wrong as the filter is supposed to be unique");
        assert existingCount == 0 || existingCount == 1;

        if (existingCount == 0) {
            log.trace("inserting one");
            getPrimaryCollection().insertOne(session, value);
        } else if (existingCount == 1) {
            if (filter == null || filter.apply(existing.stream().findFirst().get())) {
                log.trace("updating one");
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

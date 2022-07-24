package mongo_fritter.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import mongo_fritter.model.Model;
import mongo_fritter.util.MongoDAOUtil;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public abstract class DAOBase<T extends Model<IdT>, IdT> implements DAO<T,DAO,IdT> {

    private static final Logger log = LoggerFactory.getLogger(DAOBase.class);

    protected abstract MongoDatabase getDatabase();

    private final String primaryCollectionName;

    public DAOBase(final String primaryCollectionName) {
        if (primaryCollectionName != null && StringUtils.isBlank(primaryCollectionName)) throw new IllegalArgumentException();
        this.primaryCollectionName = primaryCollectionName;
    }

    public DAOBase() {
        this(null);
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
        return create();
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

    protected void findWithFilter(final Bson filter, final Consumer<T> consumer) {
        Objects.requireNonNull( filter );
        Objects.requireNonNull( consumer );

        getPrimaryCollection().find(filter).limit(Integer.MAX_VALUE).forEach( consumer );
    }

    protected Collection<T> findWithFilter( final Bson filter) {
        Objects.requireNonNull( filter );

        final List<T> results = Collections.synchronizedList( new ArrayList<>() );
        findWithFilter( filter, results::add );
        return results;
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

//package mongo_fritter.dao;
//
//import com.google.common.collect.BoundType;
//import com.google.common.collect.Range;
//import com.mongodb.BasicDBObject;
//import com.mongodb.client.FindIterable;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.Filters;
//import com.mongodb.client.model.IndexOptions;
//import com.mongodb.client.model.Indexes;
//import org.apache.commons.lang3.StringUtils;
//import org.bson.Document;
//import org.bson.conversions.Bson;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import trade_wars.dao.DAOValidationException;
//import trade_wars.models.Model;
//import trade_wars.storage.Storage;
//import trade_wars.util.time.TimeRangeUtil;
//
//import java.util.Objects;
//import java.util.SortedSet;
//import java.util.function.Consumer;
//
//public abstract class MongoDBDAOBase<PrimaryModelClassT extends Model> implements DAO<PrimaryModelClassT>
//{
//
//    private final Logger log = LoggerFactory.getLogger( MongoDBDAOBase.class );
//
//    private final Class primaryModelClass;
//    private Storage storage;
//
//    protected static final String DEFAULT_TIME_INDEX_NAME = "createdAtMSUTC";
//
//    public MongoDBDAOBase(final Storage storage)
//    {
//        this.primaryModelClass = getModelClass();
//
//        initialize(storage);
//    }
//
//    protected void onCreateIndices()
//    {
//        // do nothing additional by default
//    }
//
//    /**
//     * For internal use only.
//     *
//     * @param storage
//     */
//    private void initialize(final Storage storage) {
//        Objects.requireNonNull(storage);
//
//        this.storage = storage;
//
//        final MongoDatabase database = getStorage().getDatabase();
//        final SortedSet<String> collectionNames = getStorage().getCollectionNames();
//
//        if (!collectionNames.contains(getPrimaryTableName())) {
//            database.createCollection(getPrimaryTableName());
//            final MongoCollection<Document> collection = database.getCollection(getPrimaryTableName());
//
//            // time index
//            final String timeIndex = collection.createIndex(
//                    Indexes.ascending(getPrimaryTableName()),
//                    new IndexOptions()
//                            .min(0d)
//                            .unique(false));
//            Objects.requireNonNull(timeIndex);
//
//            onCreateIndices();
//
//            // id index
////            final String idIndex = collection.createIndex(
////                    Indexes.ascending("id"),
////                    new IndexOptions()
////                        .unique(true));
////            Objects.requireNonNull(idIndex);
//        }
//    }
//
//    protected abstract String getPrimaryTableName();
//
//    protected String getPrimaryTableTimeIndexName()
//    {
//        return DEFAULT_TIME_INDEX_NAME;
//    }
//
//    protected Storage getStorage()
//    {
//        return storage;
//    }
//
//    protected MongoCollection<PrimaryModelClassT> getPrimaryCollection()
//    {
//        return storage.getDatabase().getCollection(getPrimaryTableName(), primaryModelClass);
//    }
//
//    protected Bson filterByTimeRange(final Range<Long> timeRangeMSUTC, final String timeIndexName)
//    {
//        Objects.requireNonNull(timeRangeMSUTC);
//        TimeRangeUtil.validateAnyAnyTimeRangeRTE( timeRangeMSUTC );
//        Objects.requireNonNull(timeIndexName);
//
//        // validate
//        if (timeRangeMSUTC.hasLowerBound() && timeRangeMSUTC.lowerEndpoint() < 0L)
//            throw new IllegalArgumentException("time cannot be less than 0; lower bound was: " + timeRangeMSUTC.lowerEndpoint());
//        if (timeRangeMSUTC.hasUpperBound() && timeRangeMSUTC.upperEndpoint() < 0L)
//            throw new IllegalArgumentException("time cannot be less than 0; upper bound was: " + timeRangeMSUTC.upperEndpoint());
//
//        final Bson a0 = ! timeRangeMSUTC.hasLowerBound() ? null :
//            switch (timeRangeMSUTC.lowerBoundType()) {
//                case CLOSED -> Filters.gte(timeIndexName, timeRangeMSUTC.lowerEndpoint());
//                case OPEN -> Filters.gt(timeIndexName, timeRangeMSUTC.lowerEndpoint());
//                default -> throw new UnsupportedOperationException();
//            };
//
//        final Bson a1 = ! timeRangeMSUTC.hasUpperBound() ? null :
//            switch (timeRangeMSUTC.upperBoundType()) {
//                case CLOSED -> Filters.lte(timeIndexName, timeRangeMSUTC.upperEndpoint());
//                case OPEN -> Filters.lt(timeIndexName, timeRangeMSUTC.upperEndpoint());
//                default -> throw new UnsupportedOperationException();
//            };
//
//        Bson retVal = null;
//
//        if (a0 != null && a1 != null)
//            retVal = Filters.and(a0, a1);
//
//        else if (a0 != null && a1 == null)
//            retVal = a0;
//
//        else if (a0 == null && a1 != null)
//            retVal = a1;
//
//        else if (a0 == null && a1 == null)
//            throw new UnsupportedOperationException("no bounds were defined");
//
//        if ( retVal == null )
//            throw new IllegalStateException();
//
//        log.trace("retVal: {}", retVal);
//
//        return retVal;
//    }
//
//    protected Bson filterByTimeRange(final Range<Long> timeRangeMSUTC)
//    {
//        TimeRangeUtil.validateAnyAnyTimeRangeRTE( timeRangeMSUTC );
//
//        final String timeIndexName = getPrimaryTableTimeIndexName();
//        return filterByTimeRange(timeRangeMSUTC, timeIndexName);
//    }
//
//    public void findByTimeRange(
//            final Range<Long> timeRange,
//            final Consumer<PrimaryModelClassT> consumer)
//    {
//        Objects.requireNonNull(timeRange);
//        Objects.requireNonNull(consumer);
//
//        getPrimaryCollection().find( filterByTimeRange(timeRange) ).forEach(consumer);
//    }
//
//    @Override
//    public void findByTimeAgo(
//            final long timeAgoMS,
//            final Consumer<PrimaryModelClassT> consumer)
//    {
//        if (timeAgoMS < 0) throw new IllegalArgumentException();
//        Objects.requireNonNull(consumer);
//
//        final long now = System.currentTimeMillis();
//        final long from = Math.max( 0L, Math.subtractExact( now, timeAgoMS ) );
//
//        findByTimeRange( Range.closed(from, now), consumer );
//    }
//
//    protected Bson filterByHistoryTimeRange(final Range<Long> timeRange, final RangeFieldNames rfn)
//    {
//        TimeRangeUtil.validateAnyAnyTimeRangeRTE( timeRange );
//        {
//            final long distance = TimeRangeUtil.computeDistance(timeRange);
//            if (distance < 0) throw new IllegalStateException();
//            if (distance == 0) throw new UnsupportedOperationException("don't know how to handle ranges too tiny of size 0");
//        }
//        Objects.requireNonNull( rfn );
//
//        // y = a && b
//
//        final Bson a =
//                timeRange.hasLowerBound() ?
//                        switch (timeRange.lowerBoundType())
//                                {
//                                    case CLOSED ->
//                                            Filters.or(
//                                                    Filters.and(
//                                                            Filters.gt( rfn.getUpperEndpointName(), timeRange.lowerEndpoint() )
//                                                    ),
//                                                    Filters.and(
//                                                            Filters.eq( rfn.getUpperEndpointName(), timeRange.lowerEndpoint() ),
//                                                            Filters.eq( rfn.getUpperBoundName(), BoundType.CLOSED )
//                                                    )
//                                            );
//                                    case OPEN ->
//                                            Filters.or(
//                                                    Filters.and(
//                                                            Filters.gt( rfn.getUpperEndpointName(), timeRange.lowerEndpoint() )
//                                                    )
//                                            );
//                                    default -> throw new UnsupportedOperationException();
//                                }
//                        :
//                        null;
//
//        final Bson b =
//                timeRange.hasUpperBound() ?
//                        switch (timeRange.upperBoundType())
//                                {
//                                    case CLOSED ->
//                                            Filters.or(
//                                                    Filters.and(
//                                                            Filters.lt( rfn.getLowerEndpointName(), timeRange.upperEndpoint() )
//                                                    ),
//                                                    Filters.and(
//                                                            Filters.eq( rfn.getLowerEndpointName(), timeRange.upperEndpoint() ),
//                                                            Filters.eq( rfn.getLowerBoundName(), BoundType.CLOSED )
//                                                    )
//                                            );
//                                    case OPEN ->
//                                            Filters.or(
//                                                    Filters.and(
//                                                            Filters.lt( rfn.getLowerEndpointName(), timeRange.upperEndpoint() )
//                                                    )
//                                            );
//                                    default -> throw new UnsupportedOperationException();
//                                }
//                        :
//                        null;
//
//
//        if (a != null && b != null) {
//            return Filters.and( a, b );
//        } else if (a != null && b == null) {
//            return a;
//        } else if (a == null && b != null) {
//            return b;
//        } else if (a == null && b == null) {
//            throw new UnsupportedOperationException("no bounds defined by time range");
//        }
//
//        throw new IllegalStateException();
//    }
//
//
//    @Override
//    public PrimaryModelClassT findById(final String id) {
//        if (StringUtils.isBlank(id)) throw new IllegalArgumentException();
//
//        final FindIterable<PrimaryModelClassT> itr = getPrimaryCollection().find(Filters.eq("_id", id));
//        final PrimaryModelClassT result = itr.first();
//
//        //final MongoCursor cursor = itr.cursor();
//        //final Object obj = cursor.next();
//        //if (!Objects.equals(obj, result)) throw new IllegalStateException();
//        //if (cursor.hasNext()) throw new IllegalStateException();
//
//        return result;
//    }
//
//    @Override
//    public void findLast(
//            final int limit,
//            final Consumer<PrimaryModelClassT> consumer)
//    {
//        if (limit < 0) throw new IllegalArgumentException();
//        Objects.requireNonNull(consumer);
//
//        getPrimaryCollection().find().sort(new BasicDBObject(DEFAULT_TIME_INDEX_NAME, -1)).limit(limit).forEach(consumer);
//    }
//
//    @Override
//    public void findLast(
//            Consumer<PrimaryModelClassT> consumer)
//    {
//        findLast(1, consumer);
//    }
//
//    public void findAll(
//            final Consumer<PrimaryModelClassT> consumer)
//    {
//        Objects.requireNonNull(consumer);
//
//        getPrimaryCollection().find().forEach(consumer);
//    }
//
//    @Override
//    public void insert(
//            final PrimaryModelClassT value) throws DAOException
//    {
//        Objects.requireNonNull(value);
//
//        validate(value);
//
//        getPrimaryCollection().insertOne(value);
//    }
//
//    @Override
//    public void update(
//            final PrimaryModelClassT value) throws DAOException
//    {
//        Objects.requireNonNull(value);
//
//        final Long updatedAt = System.currentTimeMillis();
//        assert updatedAt > 0;
//        value.setUpdatedAtMSUTC(updatedAt);
//
//        validate(value);
//
//        getPrimaryCollection().replaceOne(Filters.all("_id", value.getId()), value);
//    }
//
//    private void validate(final PrimaryModelClassT value) throws DAOValidationException
//    {
//        if (value == null) throw new DAOValidationException();
//
//        final Long createdAt = value.getCreatedAtMSUTC();
//        if (createdAt == null) throw new DAOValidationException();
//        if (createdAt < 0L) throw new DAOValidationException();
//        if (createdAt == 0L) throw new DAOValidationException();
//
//        final Long updatedAt = value.getUpdatedAtMSUTC();
//        if (updatedAt == null) throw new DAOValidationException();
//        if (updatedAt < 0L) throw new DAOValidationException();
//        if (updatedAt == 0L) throw new DAOValidationException();
//
//        if (createdAt > updatedAt) throw new DAOValidationException();
//
//        final String id = value.getId();
//        if (id == null) throw new DAOValidationException();
//        if (StringUtils.isBlank(value.getId())) throw new DAOValidationException();
//    }
//
//}

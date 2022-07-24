//package mongo_fritter.dao;
//
//import com.google.common.primitives.Longs;
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import trade_wars.models.ModelPlus;
//
//import java.util.*;
//import java.util.function.Consumer;
//
//public class DAOUtil {
//
//    private static final Logger log = LoggerFactory.getLogger(DAOUtil.class);
//
//    public static <ModelClassT extends ModelPlus> void findByTimeAgoAndActivesSortByTimeAscending(
//            final DAOPlus<ModelClassT> daoPlus,
//            final QueryOptions queryOptions,
//            final long timeAgoMS,
//            final Consumer<ModelClassT> consumer)
//    {
//        if (timeAgoMS < 0) throw new IllegalArgumentException();
//        Objects.requireNonNull(consumer);
//
//        final Map<String,ModelClassT> idToValMap = new HashMap<>();
//
//        log.warn("Query options not yet being passed to all queries"); // TODO
//        daoPlus.findByTimeAgo(timeAgoMS, m -> {
//            final String id = m.getId();
//            if (StringUtils.isBlank(id)) throw new IllegalStateException();
//            idToValMap.put(id,m);
//        }); // TODO: pass query options
//
//        daoPlus.findActiveSortedByCreatedAtAscending(m -> {
//            final String id = m.getId();
//            if (StringUtils.isBlank(id)) throw new IllegalStateException();
//            idToValMap.put(id,m);
//        }, queryOptions);
//
//        final SortedMap<String,ModelClassT> sortedMap = new TreeMap<>(new Comparator<String>() {
//            @Override
//            public int compare(final String o1, final String o2) {
//                if (StringUtils.isBlank(o1)) throw new IllegalArgumentException();
//                if (StringUtils.isBlank(o2)) throw new IllegalArgumentException();
//
//                final ModelClassT m1 = idToValMap.get(o1), m2 = idToValMap.get(o2);
//                Objects.requireNonNull(m1);
//                Objects.requireNonNull(m2);
//
//                final Long t1 = m1.getCreatedAtMSUTC(), t2 = m2.getCreatedAtMSUTC();
//                Objects.requireNonNull(t1);
//                Objects.requireNonNull(t2);
//                if (t1 < 0) throw new IllegalStateException();
//                if (t2 < 0) throw new IllegalStateException();
//
//                final int r = Longs.compare(t1, t2);
//                if ( r != 0 ) return r;
//
//                // tiebreak with id
//                return o1.compareTo(o2);
//            }
//        });
//
//        sortedMap.putAll( idToValMap );
//
//        sortedMap.values().forEach( consumer );
//    }
//
//}

package com.semperex.mongo_fritter.dao;

import com.google.common.collect.Range;
import com.google.common.collect.BoundType;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class FilterUtil {

    private static final Logger log = LoggerFactory.getLogger(FilterUtil.class);

    public static Bson buildFilter(final String fieldName, final Range<Long> timeRange) {
        if (StringUtils.isBlank(fieldName)) throw new IllegalArgumentException();
        Objects.requireNonNull(timeRange);
        // TODO: time range validation

        final Bson lowerFilter;
        final Bson upperFilter;

        if ( timeRange.hasLowerBound()) {
            if (timeRange.lowerBoundType().equals(BoundType.CLOSED)) {
                lowerFilter = Filters.gte(fieldName, timeRange.lowerEndpoint());
            } else if (timeRange.lowerBoundType().equals(BoundType.OPEN)) {
                lowerFilter = Filters.gt(fieldName, timeRange.lowerEndpoint());
            } else throw new IllegalStateException();
        } else {
            lowerFilter = null;
        }

        if ( timeRange.hasUpperBound()) {
            if (timeRange.upperBoundType().equals(BoundType.CLOSED)) {
                upperFilter = Filters.lte(fieldName, timeRange.upperEndpoint());
            } else if (timeRange.upperBoundType().equals(BoundType.OPEN)) {
                upperFilter = Filters.lt(fieldName, timeRange.upperEndpoint());
            } else throw new IllegalStateException();
        } else {
            upperFilter = null;
        }

        {
            if (lowerFilter == null && upperFilter == null) {
                log.warn("no lower or upper filters are specified");
                return Filters.all( fieldName );
            }

            if (lowerFilter == null && upperFilter != null) {
                return upperFilter;
            }

            if (lowerFilter != null && upperFilter == null) {
                return lowerFilter;
            }

            if (lowerFilter != null && upperFilter != null) {
                return Filters.and(lowerFilter, upperFilter);
            }

            throw new IllegalStateException();
        }
    }

}

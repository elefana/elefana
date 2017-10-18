/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search.agg;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.viridiansoftware.es2pgsql.exception.NoSuchMappingException;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationExec;

public class DateHistogramAggregationSpec extends Es5AggregationSpec {
	private static final Logger LOGGER = LoggerFactory.getLogger(DateHistogramAggregationSpec.class);

	private static final String KEY_FIELD = "field";
	private static final String KEY_INTERVAL = "interval";
	private static final String KEY_FORMAT = "format";

	private String fieldName;
	private String interval;
	private String dateFormat;

	private String lastFieldName;

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		final String fieldType = aggregationExec.getIndexFieldMappingService()
				.getFirstFieldMapping(aggregationExec.getIndices(), aggregationExec.getTypes(), fieldName);
		if (fieldType == null) {
			throw new NoSuchMappingException(fieldName);
		}

		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>();

		final String aggregationTableName = AGGREGATION_TABLE_PREFIX + aggregationExec.getRequestBodySearch().hashCode()
				+ "_" + fieldName + "_" + interval;

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT ");

		LOGGER.info(interval);
		switch (interval) {
		case "year":
			queryBuilder.append("date_trunc('year',");
			break;
		case "quarter":
			queryBuilder.append("date_trunc('quarter',");
			break;
		case "month":
			queryBuilder.append("date_trunc('month',");
			break;
		case "week":
			queryBuilder.append("date_trunc('week',");
			break;
		case "day":
			queryBuilder.append("date_trunc('day',");
			break;
		case "hour":
			queryBuilder.append("date_trunc('hour',");
			break;
		case "minute":
			queryBuilder.append("date_trunc('minute',");
			break;
		case "second":
			queryBuilder.append("date_trunc('second',");
			break;
		default:
			// TODO: Handle datemath intervals (e.g. 90m)
			queryBuilder.append("date_trunc('second',");
			break;
		}

		switch (fieldType) {
		case "date":
			queryBuilder.append("cast(_source->>'");
			queryBuilder.append(fieldName);
			queryBuilder.append("' as TIMESTAMP)");
			break;
		case "long":
			queryBuilder.append("to_timestamp(_source->>'");
			queryBuilder.append(fieldName);
			queryBuilder.append("' / 1000)");
			break;
		default:
			break;
		}
		queryBuilder.append(") AS es2pgsql_agg_bucket");
		queryBuilder.append(", * INTO ");
		queryBuilder.append(aggregationTableName);
		queryBuilder.append(" FROM ");
		queryBuilder.append(aggregationExec.getQueryTable());

		LOGGER.info(queryBuilder.toString());

		aggregationExec.getJdbcTemplate().execute(queryBuilder.toString());

		final String distinctBucketsQuery = "SELECT DISTINCT es2pgsql_agg_bucket FROM " + aggregationTableName;
		LOGGER.info(distinctBucketsQuery);
		SqlRowSet resultSet = aggregationExec.getJdbcTemplate().queryForRowSet(distinctBucketsQuery);

		final Set<Timestamp> uniqueBuckets = new HashSet<Timestamp>();
		while (resultSet.next()) {
			uniqueBuckets.add(resultSet.getTimestamp("es2pgsql_agg_bucket"));
		}

		for (Timestamp bucketTimestamp : uniqueBuckets) {
			final Map<String, Object> bucket = new HashMap<String, Object>();
			bucket.put("key", bucketTimestamp.getTime());

			final String bucketTableName = aggregationTableName + "_" + bucketTimestamp.getTime();
			final String bucketQuery = "SELECT * INTO " + bucketTableName + " FROM " + aggregationTableName
					+ " WHERE es2pgsql_agg_bucket = to_timestamp(" + bucketTimestamp.getTime() / 1000L + ")";
			LOGGER.info(bucketQuery);
			aggregationExec.getJdbcTemplate().execute(bucketQuery);

			final String bucketCountQuery = "SELECT COUNT(*) FROM " + bucketTableName;
			LOGGER.info(bucketCountQuery);
			Map<String, Object> aggResult = aggregationExec.getJdbcTemplate().queryForMap(bucketCountQuery);
			bucket.put("doc_count", aggResult.get("count"));

			for (Aggregation aggregation : aggregationExec.getAggregation().getSubAggregations()) {
				aggregation.executeSqlQuery(aggregationExec, bucket, bucketTableName);
			}
			buckets.add(bucket);
			aggregationExec.getTempTablesCreated().add(bucketTableName);
		}
		result.put("buckets", buckets);
		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);

		aggregationExec.getTempTablesCreated().add(aggregationTableName);
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		lastFieldName = name;
	}

	@Override
	public void writeString(String value) throws IOException {
		writeStringField(lastFieldName, value);
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		switch (name) {
		case KEY_FIELD:
			this.fieldName = value;
			break;
		case KEY_INTERVAL:
			this.interval = value;
			break;
		case KEY_FORMAT:
			this.dateFormat = value;
			break;
		}
	}
}

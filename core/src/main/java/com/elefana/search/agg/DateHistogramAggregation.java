/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.search.agg;

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

import com.elefana.exception.NoSuchMappingException;
import com.jsoniter.any.Any;

public class DateHistogramAggregation extends BucketAggregation {
	private static final Logger LOGGER = LoggerFactory.getLogger(DateHistogramAggregation.class);
	
	private static final String KEY_FIELD = "field";
	private static final String KEY_INTERVAL = "interval";
	
	private final String aggregationName;
	private final String fieldName;
	private final String interval;

	public DateHistogramAggregation(String aggregationName, Any context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).toString();
		this.interval = context.get(KEY_INTERVAL).toString();
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		final String fieldType = aggregationExec.getIndexFieldMappingService()
				.getFirstFieldMappingType(aggregationExec.getIndices(), aggregationExec.getTypes(), fieldName);
		if (fieldType == null) {
			throw new NoSuchMappingException(fieldName);
		}
		final String fieldFormat = aggregationExec.getIndexFieldMappingService()
				.getFirstFieldMappingFormat(aggregationExec.getIndices(), aggregationExec.getTypes(), fieldName);

		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>();

		final String aggregationTableName = AGGREGATION_TABLE_PREFIX + aggregationExec.getRequestBodySearch().hashCode()
				+ "_" + fieldName + "_" + interval;

		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("CREATE TEMP TABLE ");
		queryBuilder.append(aggregationTableName);
		queryBuilder.append(" AS (");
		queryBuilder.append("SELECT ");

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
			if(interval.indexOf("micros") > 0) {
				queryBuilder.append("date_trunc('microseconds',");
			} else if(interval.indexOf("ms") > 0) {
				queryBuilder.append("date_trunc('milliseconds',");
			} else if(interval.indexOf('s') > 0) {
				queryBuilder.append("date_trunc('second',");
			} else if(interval.indexOf('m') > 0) {
				queryBuilder.append("date_trunc('minute',");
			} else if(interval.indexOf('h') > 0) {
				queryBuilder.append("date_trunc('hour',");
			} else if(interval.indexOf('d') > 0) {
				queryBuilder.append("date_trunc('day',");
			} else {
				queryBuilder.append("date_trunc('second',");
			}
			break;
		}

		switch (fieldType) {
		case "date":
			LOGGER.info(fieldFormat);
			switch(fieldFormat) {
			case "epoch_millis":
				queryBuilder.append("to_timestamp((_source->>'");
				queryBuilder.append(fieldName);
				queryBuilder.append("')::numeric / 1000)");
				break;
			default:
				queryBuilder.append("cast(_source->>'");
				queryBuilder.append(fieldName);
				queryBuilder.append("' as TIMESTAMP)");
				break;
			}
			break;
		case "long":
			queryBuilder.append("to_timestamp((_source->>'");
			queryBuilder.append(fieldName);
			queryBuilder.append("')::numeric / 1000)");
			break;
		default:
			break;
		}
		queryBuilder.append(") AS elefana_agg_bucket");
		queryBuilder.append(", * FROM ");
		queryBuilder.append(aggregationExec.getQueryTable());
		appendIndicesWhereClause(aggregationExec, queryBuilder);
		queryBuilder.append(")");

		LOGGER.info(queryBuilder.toString());

		aggregationExec.getJdbcTemplate().execute(queryBuilder.toString());

		final String distinctBucketsQuery = "SELECT DISTINCT elefana_agg_bucket FROM " + aggregationTableName;
		LOGGER.info(distinctBucketsQuery);
		SqlRowSet resultSet = aggregationExec.getJdbcTemplate().queryForRowSet(distinctBucketsQuery);

		final Set<Timestamp> uniqueBuckets = new HashSet<Timestamp>();
		while (resultSet.next()) {
			uniqueBuckets.add(resultSet.getTimestamp("elefana_agg_bucket"));
		}

		for (Timestamp bucketTimestamp : uniqueBuckets) {
			final Map<String, Object> bucket = new HashMap<String, Object>();
			bucket.put("key", bucketTimestamp.getTime());

			final String bucketTableName = aggregationTableName + "_" + bucketTimestamp.getTime();
			final String bucketQuery = "SELECT * INTO " + bucketTableName + " FROM " + aggregationTableName
					+ " WHERE elefana_agg_bucket = to_timestamp(" + bucketTimestamp.getTime() / 1000L + ")";
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
	public String getAggregationName() {
		return aggregationName;
	}
}

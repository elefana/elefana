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

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.InvalidAggregationFieldType;
import com.elefana.api.exception.NoSuchMappingException;
import com.elefana.api.search.SearchResponse;
import com.elefana.search.PsqlQueryComponents;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class DateHistogramAggregation extends BucketAggregation {
	private static final Logger LOGGER = LoggerFactory.getLogger(DateHistogramAggregation.class);

	private static final String KEY_FIELD = "field";
	private static final String KEY_INTERVAL = "interval";

	private static final String[] EXPECTED_FIELD_TYPES = new String[] { "long", "date" };

	private static final long ONE_SECOND_IN_MILLIS = 1000L;
	private static final long ONE_MINUTE_IN_MILLIS = ONE_SECOND_IN_MILLIS * 60L;
	private static final long ONE_HOUR_IN_MILLIS = ONE_MINUTE_IN_MILLIS * 60L;
	private static final long ONE_DAY_IN_MILLIS = ONE_HOUR_IN_MILLIS * 24L;
	private static final long ONE_YEAR_IN_MILLIS = ONE_DAY_IN_MILLIS * 365L;

	private final String aggregationName;
	private final String fieldName;
	private final String interval;

	public DateHistogramAggregation(String aggregationName, JsonNode context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).textValue();
		this.interval = context.get(KEY_INTERVAL).textValue();
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) throws ElefanaException {
		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>();

		if (aggregationExec.getSearchResponse().getHits().getTotal() == 0) {
			result.put("buckets", buckets);
			aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
			return;
		}

		final boolean isTimestampColumn = aggregationExec.getIndexTemplate() != null
				&& aggregationExec.getIndexTemplate().isTimestampField(fieldName);
		final String fieldType = aggregationExec.getIndexFieldMappingService()
				.getFirstFieldMappingType(aggregationExec.getIndices(), aggregationExec.getTypes(), fieldName);
		if (fieldType == null || fieldType.isEmpty()) {
			throw new NoSuchMappingException(fieldName);
		}
		final String fieldFormat = aggregationExec.getIndexFieldMappingService()
				.getFirstFieldMappingFormat(aggregationExec.getIndices(), aggregationExec.getTypes(), fieldName);

		final PsqlQueryComponents queryComponents = aggregationExec.getQueryComponents();

		final StringBuilder distinctBucketQueryBuilder = new StringBuilder();
		if (isTimestampColumn) {
			generateDistinctBucketQueryUsingTimestampField(aggregationExec, distinctBucketQueryBuilder);
		} else {
			generateDistinctBucketQueryUsingActualField(aggregationExec, distinctBucketQueryBuilder, fieldType,
					fieldFormat);
		}

		LOGGER.info(distinctBucketQueryBuilder.toString());
		SqlRowSet bucketResultSet = aggregationExec.getJdbcTemplate()
				.queryForRowSet(distinctBucketQueryBuilder.toString());

		final List<Long> distinctBuckets = getDistinctBuckets(aggregationExec, isTimestampColumn, bucketResultSet);
		final long bucketInterval = getBucketInterval();

		for (long bucketTimestamp : distinctBuckets) {
			final Map<String, Object> bucket = new ConcurrentHashMap<String, Object>();
			bucket.put("key", bucketTimestamp);
			buckets.add(bucket);

			aggregationExec.getQueryFutures()
					.offer(aggregationExec.getExecutorService().submit(new Callable<SearchResponse>() {

						@Override
						public SearchResponse call() throws Exception {
							final StringBuilder bucketCountQueryBuilder = new StringBuilder();
							bucketCountQueryBuilder.append("SELECT COUNT(");
							bucketCountQueryBuilder.append("_id");
							bucketCountQueryBuilder.append(") FROM ");
							bucketCountQueryBuilder.append(queryComponents.getFromComponent());

							if (aggregationExec.getNodeSettingsService().isUsingCitus()) {
								bucketCountQueryBuilder.append(" AS bucketCount ");
							}

							final StringBuilder appendedWhereClause = new StringBuilder();

							if (!aggregationExec.getNodeSettingsService().isUsingCitus()) {
								if (queryComponents.getWhereComponent() != null
										&& !queryComponents.getWhereComponent().isEmpty()) {
									appendedWhereClause.append(queryComponents.getWhereComponent());
									appendedWhereClause.append(" AND (");
								} else {
									appendedWhereClause.append("(");
								}
							} else {
								appendedWhereClause.append("(");
							}

							if (isTimestampColumn) {
								appendedWhereClause.append(getBucketColumn());
							} else {
								appendedWhereClause.append(getActualColumn(fieldType, fieldFormat));
							}
							appendedWhereClause.append(" >= ");
							appendedWhereClause.append(bucketTimestamp);

							appendedWhereClause.append(" AND ");

							if (isTimestampColumn) {
								appendedWhereClause.append(getBucketColumn());
							} else {
								appendedWhereClause.append(getActualColumn(fieldType, fieldFormat));
							}
							appendedWhereClause.append(" < ");
							appendedWhereClause.append((bucketTimestamp + bucketInterval));

							appendedWhereClause.append(')');

							bucketCountQueryBuilder.append(" WHERE ");
							bucketCountQueryBuilder.append(appendedWhereClause.toString());

							final Map<String, Object> aggResult = aggregationExec.getJdbcTemplate()
									.queryForMap(bucketCountQueryBuilder.toString());
							bucket.put("doc_count", aggResult.get("count"));

							for (Aggregation aggregation : aggregationExec.getAggregation().getSubAggregations()) {
								PsqlQueryComponents subAggregationQueryComponents = new PsqlQueryComponents(
										new String(queryComponents.getFromComponent()), appendedWhereClause.toString(),
										new String(queryComponents.getGroupByComponent()), "",
										new String(queryComponents.getLimitComponent()),
										queryComponents.getTemporaryTables());
								aggregation.executeSqlQuery(aggregationExec, subAggregationQueryComponents,
										aggregationExec.getSearchResponse(), bucket);
							}
							return aggregationExec.getSearchResponse();
						}
					}));
		}
		result.put("buckets", buckets);
		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
	}

	private List<Long> getDistinctBuckets(AggregationExec aggregationExec, boolean isTimestampColumn,
			SqlRowSet bucketResultSet) {
		final Set<Long> results = new HashSet<Long>();
		final String resultColumn = isTimestampColumn ? getBucketColumn() : "elefana_agg_bucket";
		while (bucketResultSet.next()) {
			results.add(bucketResultSet.getLong(resultColumn));
		}

		if (isTimestampColumn) {
			final long bucketInterval = getBucketInterval();
			final Set<Long> bucketedResults = new HashSet<Long>();
			for (long bucket : results) {
				bucketedResults.add(bucket - (bucket % bucketInterval));
			}
			return new ArrayList<Long>(bucketedResults);
		}
		return new ArrayList<Long>(results);
	}

	private String getBucketColumn() {
		final String bucketColumn;
		switch (interval) {
		case "year":
		case "quarter":
		case "month":
		case "week":
		case "day":
			bucketColumn = "_bucket1d";
			break;
		case "hour":
			bucketColumn = "_bucket1h";
			break;
		case "minute":
			bucketColumn = "_bucket1m";
			break;
		case "second":
			bucketColumn = "_bucket1s";
			break;
		default:
			if (interval.indexOf("micros") > 0) {
				bucketColumn = "_bucket1s";
			} else if (interval.indexOf("ms") > 0) {
				bucketColumn = "_bucket1s";
			} else if (interval.indexOf('s') > 0) {
				bucketColumn = "_bucket1s";
			} else if (interval.indexOf('m') > 0) {
				bucketColumn = "_bucket1m";
			} else if (interval.indexOf('h') > 0) {
				bucketColumn = "_bucket1h";
			} else if (interval.indexOf('d') > 0) {
				bucketColumn = "_bucket1d";
			} else {
				bucketColumn = "_bucket1s";
			}
			break;
		}
		return bucketColumn;
	}

	private String getActualColumn(final String fieldType, final String fieldFormat)
			throws InvalidAggregationFieldType {
		final StringBuilder result = new StringBuilder();
		switch (fieldType) {
		case "date":
			switch (fieldFormat) {
			case "epoch_millis":
				result.append("(_source->>'");
				result.append(fieldName);
				result.append("')::bigint");
				break;
			default:
				result.append("EXTRACT(EPOCH FROM TIMESTAMP ");
				result.append("cast(_source->>'");
				result.append(fieldName);
				result.append("' as TIMESTAMP)");
				result.append(") * 1000");
				break;
			}
			break;
		case "long":
			result.append("(_source->>'");
			result.append(fieldName);
			result.append("')::bigint");
			break;
		default:
			throw new InvalidAggregationFieldType(EXPECTED_FIELD_TYPES, fieldType);
		}
		return result.toString();
	}

	private long getBucketInterval() {
		final long bucketInterval;
		switch (interval) {
		case "year":
			bucketInterval = ONE_YEAR_IN_MILLIS;
			break;
		case "quarter":
		case "month":
		case "week":
		case "day":
			bucketInterval = ONE_DAY_IN_MILLIS;
			break;
		case "hour":
			bucketInterval = ONE_HOUR_IN_MILLIS;
			break;
		case "minute":
			bucketInterval = ONE_MINUTE_IN_MILLIS;
			break;
		case "second":
			bucketInterval = ONE_SECOND_IN_MILLIS;
			break;
		default:
			if (interval.indexOf("micros") > 0) {
				bucketInterval = ONE_SECOND_IN_MILLIS;
			} else if (interval.indexOf("ms") > 0) {
				bucketInterval = ONE_SECOND_IN_MILLIS;
			} else if (interval.indexOf('s') > 0) {
				bucketInterval = ONE_SECOND_IN_MILLIS * (Long.parseLong(interval.substring(0, interval.indexOf('s'))));
			} else if (interval.indexOf('m') > 0) {
				bucketInterval = ONE_MINUTE_IN_MILLIS * (Long.parseLong(interval.substring(0, interval.indexOf('m'))));
			} else if (interval.indexOf('h') > 0) {
				bucketInterval = ONE_HOUR_IN_MILLIS * (Long.parseLong(interval.substring(0, interval.indexOf('h'))));
			} else if (interval.indexOf('d') > 0) {
				bucketInterval = ONE_DAY_IN_MILLIS * (Long.parseLong(interval.substring(0, interval.indexOf('d'))));
			} else {
				bucketInterval = ONE_SECOND_IN_MILLIS;
			}
			break;
		}
		return bucketInterval;
	}

	private void generateDistinctBucketQueryUsingTimestampField(AggregationExec aggregationExec,
			StringBuilder distinctBucketQueryBuilder) {
		final PsqlQueryComponents queryComponents = aggregationExec.getQueryComponents();
		distinctBucketQueryBuilder.append("SELECT DISTINCT(");

		distinctBucketQueryBuilder.append(getBucketColumn());
		distinctBucketQueryBuilder.append(')');
		distinctBucketQueryBuilder.append(" FROM ");
		distinctBucketQueryBuilder.append(queryComponents.getFromComponent());

		if (!aggregationExec.getNodeSettingsService().isUsingCitus()) {
			queryComponents.appendWhere(distinctBucketQueryBuilder);
		} else {
			distinctBucketQueryBuilder.append(" AS ");
			distinctBucketQueryBuilder.append("hit_results");
		}
	}

	private void generateDistinctBucketQueryUsingActualField(AggregationExec aggregationExec,
			StringBuilder distinctBucketQueryBuilder, final String fieldType, final String fieldFormat)
			throws InvalidAggregationFieldType {
		final PsqlQueryComponents queryComponents = aggregationExec.getQueryComponents();
		distinctBucketQueryBuilder.append("SELECT DISTINCT(elefana_agg_bucket) FROM (");

		final long bucketInterval = getBucketInterval();

		distinctBucketQueryBuilder
				.append("SELECT (bucketValue - (bucketValue % " + bucketInterval + ")) AS elefana_agg_bucket FROM (");
		distinctBucketQueryBuilder.append("SELECT ");
		distinctBucketQueryBuilder.append(getActualColumn(fieldType, fieldFormat));
		distinctBucketQueryBuilder.append(" AS bucketValue FROM ");
		distinctBucketQueryBuilder.append(queryComponents.getFromComponent());
		if (!aggregationExec.getNodeSettingsService().isUsingCitus()) {
			aggregationExec.getQueryComponents().appendWhere(distinctBucketQueryBuilder);
		} else {
			distinctBucketQueryBuilder.append(" AS ");
			distinctBucketQueryBuilder.append("hit_results");
		}
		distinctBucketQueryBuilder.append(") AS bucketResults");
		distinctBucketQueryBuilder.append(") AS aggResults");
	}

	@Override
	public String getAggregationName() {
		return aggregationName;
	}
}

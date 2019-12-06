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
import com.elefana.search.PsqlQueryComponents;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RangeAggregation extends BucketAggregation {
	private static final Logger LOGGER = LoggerFactory.getLogger(RangeAggregation.class);
	
	private static final String KEY_FIELD = "field";
	private static final String KEY_RANGES = "ranges";
	private static final String KEY_FROM = "from";
	private static final String KEY_TO = "to";
	
	private final String aggregationName;
	
	private final String fieldName;
	private final Set<Range> ranges = new HashSet<Range>();
	private boolean keyed = false;
	
	public RangeAggregation(String aggregationName, JsonNode context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).textValue();

		if(!context.has(KEY_RANGES) || !context.get(KEY_RANGES).isArray()) {
			return;
		}
		final JsonNode rangesContext = context.get(KEY_RANGES);

		for(int i = 0; i < rangesContext.size(); i++) {
			final JsonNode rangeContext = rangesContext.get(i);
			final Range range = new Range();


			if(rangeContext.has(KEY_FROM)) {
				final JsonNode fromContext = rangeContext.get(KEY_FROM);
				if(fromContext.toString().indexOf('.') > 0) {
					range.doubleFrom = fromContext.asDouble();
				} else {
					range.longFrom = fromContext.asLong();
				}
			}
			if(rangeContext.has(KEY_TO)) {
				final JsonNode toContext = rangeContext.get(KEY_TO);
				if(toContext.toString().indexOf('.') > 0) {
					range.doubleTo = toContext.asDouble();
				} else {
					range.longTo = toContext.asLong();
				}
			}
			ranges.add(range);
		}
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) throws ElefanaException {
		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>();
		
		if(aggregationExec.getSearchResponse().getHits().getTotal() == 0) {
			result.put("buckets", buckets);
			aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
			return;
		}
		
		for(Range range : ranges) {
			final String rangeTableName = AGGREGATION_TABLE_PREFIX + aggregationExec.getRequestBodySearch().hashCode() + "_" + fieldName + "_" + range.toString();
			final Map<String, Object> bucket = new HashMap<String, Object>();
			
			final StringBuilder queryBuilder = new StringBuilder();
			if(aggregationExec.getAggregation().getSubAggregations().isEmpty()) {
				queryBuilder.append("SELECT COUNT(*) FROM ");
			} else {
				queryBuilder.append("CREATE TEMP TABLE ");
				queryBuilder.append(rangeTableName);
				queryBuilder.append(" AS (");
				queryBuilder.append("SELECT * FROM ");
			}
			queryBuilder.append(aggregationExec.getQueryComponents().getFromComponent());
			if (!aggregationExec.getNodeSettingsService().isUsingCitus()) {
				if(aggregationExec.getQueryComponents().appendWhere(queryBuilder)) {
					queryBuilder.append(" AND ");
				} else {
					queryBuilder.append(" WHERE ");
				}
			} else if(aggregationExec.getQueryComponents().getWhereComponent().isEmpty()) {
				queryBuilder.append(" WHERE ");
			} else {
				queryBuilder.append(" AND ");
			}
			
			if(range.doubleFrom != null) {
				bucket.put("from", range.doubleFrom);
				
				queryBuilder.append("_source->>'");
				queryBuilder.append(fieldName);
				queryBuilder.append("' >= '");
				queryBuilder.append(range.doubleFrom);
				queryBuilder.append("'");
				
				if(range.doubleTo != null || range.longTo != null) {
					queryBuilder.append(" AND ");
				}
			} else if(range.longFrom != null) {
				bucket.put("from", range.longFrom);
				
				queryBuilder.append("_source->>'");
				queryBuilder.append(fieldName);
				queryBuilder.append("' >= '");
				queryBuilder.append(range.longFrom);
				queryBuilder.append("'");
				
				if(range.doubleTo != null || range.longTo != null) {
					queryBuilder.append(" AND ");
				}
			}
			if(range.doubleTo != null) {
				bucket.put("to", range.doubleTo);
				
				queryBuilder.append("_source->>'");
				queryBuilder.append(fieldName);
				queryBuilder.append("' < '");
				queryBuilder.append(range.doubleTo);
				queryBuilder.append("'");
			} else if(range.longTo != null) {
				bucket.put("to", range.longTo);
				
				queryBuilder.append("_source->>'");
				queryBuilder.append(fieldName);
				queryBuilder.append("' < '");
				queryBuilder.append(range.longTo);
				queryBuilder.append("'");
			}
			
			if(!aggregationExec.getAggregation().getSubAggregations().isEmpty()) {
				queryBuilder.append(")");
			}
			
			LOGGER.info(queryBuilder.toString());
			
			if(!aggregationExec.getAggregation().getSubAggregations().isEmpty()) {
				aggregationExec.getJdbcTemplate().execute(queryBuilder.toString());
				
				Map<String, Object> aggResult = aggregationExec.getJdbcTemplate().queryForMap("SELECT COUNT(*) FROM " + rangeTableName);
				bucket.put("doc_count", aggResult.get("count"));
				
				for(Aggregation aggregation : aggregationExec.getAggregation().getSubAggregations()) {
					PsqlQueryComponents queryComponents = new PsqlQueryComponents(rangeTableName, "", "", "");
					aggregation.executeSqlQuery(aggregationExec, queryComponents, aggregationExec.getSearchResponse(), bucket);
				}
			} else {
				Map<String, Object> aggResult = aggregationExec.getJdbcTemplate().queryForMap(queryBuilder.toString());
				bucket.put("doc_count", aggResult.get("count"));
			}
			
			buckets.add(bucket);
		}
		
		result.put("buckets", buckets);
		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
	}

	@Override
	public String getAggregationName() {
		return aggregationName;
	}

	private class Range {
		public Double doubleTo;
		public Double doubleFrom;
		public Long longTo;
		public Long longFrom;
		
		@Override
		public String toString() {
			StringBuilder result = new StringBuilder();
			if(doubleFrom != null) {
				result.append(doubleFrom.longValue());
			} else if(longFrom != null) {
				result.append(longFrom.longValue());
			}
			if(doubleTo != null) {
				if(doubleFrom != null || longFrom != null) {
					result.append("_");
				}
				result.append(doubleTo.longValue());
			} else if(longTo != null) {
				if(doubleFrom != null || longFrom != null) {
					result.append("_");
				}
				result.append(longTo.longValue());
			}
			return result.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((doubleFrom == null) ? 0 : doubleFrom.hashCode());
			result = prime * result + ((doubleTo == null) ? 0 : doubleTo.hashCode());
			result = prime * result + ((longFrom == null) ? 0 : longFrom.hashCode());
			result = prime * result + ((longTo == null) ? 0 : longTo.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Range other = (Range) obj;
			if (doubleFrom == null) {
				if (other.doubleFrom != null)
					return false;
			} else if (!doubleFrom.equals(other.doubleFrom))
				return false;
			if (doubleTo == null) {
				if (other.doubleTo != null)
					return false;
			} else if (!doubleTo.equals(other.doubleTo))
				return false;
			if (longFrom == null) {
				if (other.longFrom != null)
					return false;
			} else if (!longFrom.equals(other.longFrom))
				return false;
			if (longTo == null) {
				if (other.longTo != null)
					return false;
			} else if (!longTo.equals(other.longTo))
				return false;
			return true;
		}
	}
}

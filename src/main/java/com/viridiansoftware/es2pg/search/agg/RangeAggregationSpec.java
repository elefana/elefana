/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.viridiansoftware.es2pg.search.SearchService;

public class RangeAggregationSpec extends AggregationSpec {
	private static final Logger LOGGER = LoggerFactory.getLogger(RangeAggregationSpec.class);
	
	private static final String KEY_FIELD = "field";
	private static final String KEY_RANGES = "ranges";
	private static final String KEY_FROM = "from";
	private static final String KEY_TO = "to";
	
	private String fieldName;
	private final List<Range> ranges = new ArrayList<Range>();
	private boolean keyed = false;
	
	private boolean parsingRanges = false;
	
	private String lastFieldName;
	private Range currentRange;

	@Override
	public void executeSqlQuery(final AggregationExec aggregationExec) {
		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>();
		
		for(Range range : ranges) {
			final String rangeTableName = AGGREGATION_TABLE_PREFIX + aggregationExec.getRequestBodySearch().hashCode() + "_" + fieldName + "_" + range.toString();
			final Map<String, Object> bucket = new HashMap<String, Object>();
			
			StringBuilder queryBuilder = new StringBuilder();
			if(aggregationExec.getAggregation().getSubAggregations().isEmpty()) {
				queryBuilder.append("SELECT COUNT(*) FROM ");
			} else {
				queryBuilder.append("SELECT * INTO ");
				queryBuilder.append(rangeTableName);
				queryBuilder.append(" FROM ");
			}
			queryBuilder.append(aggregationExec.getQueryTable());
			queryBuilder.append(" WHERE ");
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
			LOGGER.info(queryBuilder.toString());
			
			if(!aggregationExec.getAggregation().getSubAggregations().isEmpty()) {
				aggregationExec.getJdbcTemplate().execute(queryBuilder.toString());
				
				Map<String, Object> aggResult = aggregationExec.getJdbcTemplate().queryForMap("SELECT COUNT(*) FROM " + rangeTableName);
				bucket.put("doc_count", aggResult.get("count"));
				
				for(Aggregation aggregation : aggregationExec.getAggregation().getSubAggregations()) {
					aggregation.executeSqlQuery(aggregationExec, bucket, rangeTableName);
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
	public void writeFieldName(String name) throws IOException {
		if(currentRange != null) {
			currentRange.lastFieldName = name;
		} else {
			lastFieldName = name;
		}
	}
	
	@Override
	public void writeStringField(String name, String value) throws IOException {
		if(currentRange != null) {
			return;
		}
		
		switch(name) {
		case KEY_FIELD:
			this.fieldName = value;
			break;
		}
	}
	
	@Override
	public void writeString(String value) throws IOException {
		writeStringField(lastFieldName, value);
	}
	
	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if(currentRange == null) {
			return;
		}
		switch(name) {
		case KEY_FROM:
			currentRange.doubleFrom = value;
			break;
		case KEY_TO:
			currentRange.doubleTo = value;
			break;
		}
	}
	
	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if(currentRange == null) {
			return;
		}
		switch(name) {
		case KEY_FROM:
			currentRange.doubleFrom = (double) value;
			break;
		case KEY_TO:
			currentRange.doubleTo = (double) value;
			break;
		}
	}
	
	@Override
	public void writeNumberField(String name, int value) throws IOException {
		if(currentRange == null) {
			return;
		}
		switch(name) {
		case KEY_FROM:
			currentRange.longFrom = (long) value;
			break;
		case KEY_TO:
			currentRange.longTo = (long) value;
			break;
		}
	}
	
	@Override
	public void writeNumberField(String name, long value) throws IOException {
		if(currentRange == null) {
			return;
		}
		switch(name) {
		case KEY_FROM:
			currentRange.longFrom = value;
			break;
		case KEY_TO:
			currentRange.longTo = value;
			break;
		}
	}
	
	@Override
	public void writeNumber(double value) throws IOException {
		writeNumberField(currentRange != null ? currentRange.lastFieldName : lastFieldName, value);
	}
	
	@Override
	public void writeNumber(float value) throws IOException {
		writeNumberField(currentRange != null ? currentRange.lastFieldName : lastFieldName, value);
	}
	
	@Override
	public void writeNumber(int value) throws IOException {
		writeNumberField(currentRange != null ? currentRange.lastFieldName : lastFieldName, value);
	}
	
	@Override
	public void writeNumber(long value) throws IOException {
		writeNumberField(currentRange != null ? currentRange.lastFieldName : lastFieldName, value);
	}
	
	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();
		switch(lastFieldName) {
		case KEY_RANGES:
			parsingRanges = true;
			break;
		}
	}
	
	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		if(parsingRanges) {
			parsingRanges = false;
		}
	}
	
	@Override
	public void writeStartObject() throws IOException {
		super.writeStartObject();
		if(parsingRanges) {
			currentRange = new Range();
		}
	}
	
	@Override
	public void writeEndObject() throws IOException {
		super.writeEndObject();
		if(currentRange != null) {
			ranges.add(currentRange);
			currentRange = null;
		}
	}
	
	private class Range {
		public Double doubleTo;
		public Double doubleFrom;
		public Long longTo;
		public Long longFrom;
		public String lastFieldName;
		
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

/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

public class AvgAggregationSpec extends AggregationSpec {
	private static final String KEY_FIELD = "field";

	private String fieldName;

	private String lastFieldName;

	@Override
	public void executeSqlQuery(final AggregationExec aggregationExec) {
		List<Map<String, Object>> resultSet = aggregationExec.getJdbcTemplate()
				.queryForList("SELECT avg((_source->>'" + fieldName + "')::numeric) AS "
						+ aggregationExec.getAggregation().getAggregationName() + " FROM "
						+ aggregationExec.getQueryTable());

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("value", resultSet.get(0).get(aggregationExec.getAggregation().getAggregationName()));

		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		this.lastFieldName = name;
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		switch (name) {
		case KEY_FIELD:
			this.fieldName = value;
			break;
		}
	}

	@Override
	public void writeString(String value) throws IOException {
		switch (lastFieldName) {
		case KEY_FIELD:
			this.fieldName = value;
			break;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
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
		AvgAggregationSpec other = (AvgAggregationSpec) obj;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		return true;
	}
}

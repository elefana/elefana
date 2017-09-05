/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.io.IOException;
import java.util.List;

public class AvgAggregationSpec extends AggregationSpec {
	private static final String KEY_FIELD = "field";
	
	private String fieldName;
	
	private String lastFieldName;

	@Override
	public String toSqlQuery(List<String> tempTablesCreated, String queryTable, Aggregation aggregation) {
		return "avg(data->>'" + fieldName + "') AS " + aggregation.getAggregationName();
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		this.lastFieldName = name;
	}
	
	@Override
	public void writeStringField(String name, String value) throws IOException {
		switch(name) {
		case KEY_FIELD:
			this.fieldName = value;
			break;
		}
	}
	
	@Override
	public void writeString(String value) throws IOException {
		switch(lastFieldName) {
		case KEY_FIELD:
			this.fieldName = value;
			break;
		}
	}
}

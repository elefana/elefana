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
package com.elefana.search.query;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elefana.esqs.EsFieldQuery;
import com.elefana.esqs.EsQueryOperator;
import com.elefana.esqs.EsQueryString;
import com.elefana.esqs.EsQueryStringWalker;
import com.elefana.node.NodeInfoService;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class QueryStringQuery extends Query implements EsQueryStringWalker {
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryStringQuery.class);
	
	private static final String KEY_DEFAULT_FIELD = "default_field";
	private static final String KEY_DEFAULT_OPERATOR = "default_operator";
	private static final String KEY_FIELDS = "fields";
	private static final String KEY_QUERY = "query";

	private final StringBuilder queryBuilder = new StringBuilder();
	private final List<String> fields;

	private String defaultField;
	private String defaultOperator = "OR";
	private String sqlQuery;

	private String mostRecentField = null;
	
	public QueryStringQuery(Any queryContext) {
		super();

		Any defaultFieldContext = queryContext.get(KEY_DEFAULT_FIELD);
		if (defaultFieldContext.valueType().equals(ValueType.STRING)) {
			defaultField = defaultFieldContext.toString();
		} else {
			defaultField = null;
		}

		Any defaultOperatorContext = queryContext.get(KEY_DEFAULT_OPERATOR);
		if (defaultOperatorContext.valueType().equals(ValueType.STRING)
				&& (defaultOperatorContext.toString().equalsIgnoreCase("OR")
						|| defaultOperatorContext.toString().equalsIgnoreCase("AND"))) {
			defaultOperator = defaultOperatorContext.toString().toUpperCase();
		}

		Any fieldsContext = queryContext.get(KEY_FIELDS);
		if (fieldsContext.valueType().equals(ValueType.ARRAY)) {
			fields = new ArrayList<String>(fieldsContext.size());
			for (int i = 0; i < fieldsContext.size(); i++) {
				fields.add(fieldsContext.get(i).toString());
			}
		} else {
			fields = null;
		}

		EsQueryString queryString = EsQueryString.parse(queryContext.get(KEY_QUERY).toString());
		
		if(fields != null && !fields.isEmpty()) {
			for(int i = 0; i < fields.size(); i++) {
				defaultField = fields.get(i);
				mostRecentField = null;
				
				queryString.walk(this);
			}
		} else {
			queryString.walk(this);
		}
		
		sqlQuery = queryBuilder.toString();
	}

	@Override
	public boolean isMatchAllQuery() {
		if(sqlQuery.isEmpty()) {
			return true;
		}
		if(sqlQuery.equalsIgnoreCase("()")) {
			return true;
		}
		return false;
	}

	@Override
	public String toSqlWhereClause() {
		LOGGER.info(sqlQuery);
		return sqlQuery;
	}

	@Override
	public void beginGrouping() {
		queryBuilder.append('(');
	}

	@Override
	public void endGrouping() {
		queryBuilder.append(')');
	}

	@Override
	public void beginField(EsQueryOperator operator, EsFieldQuery field) {
		if(queryBuilder.length() == 0) {
			return;
		}
		switch(operator) {
		case AND:
		case OR:
			queryBuilder.append(" " + operator.name() + " ");
			break;
		case DEFAULT:
		default:
			queryBuilder.append(" " + defaultOperator + " ");
			break;
		}
	}

	@Override
	public void append(EsFieldQuery field, EsQueryOperator operator) {
		switch(operator) {
		case AND:
		case OR:
			queryBuilder.append(" " + operator.name() + " ");
			break;
		case DEFAULT:
		default:
			queryBuilder.append(" " + defaultOperator + " ");
			break;
		}
	}

	@Override
	public void append(EsFieldQuery field, boolean phraseQuery, String term) {
		String fieldName = field.isDefaultField() ? defaultField : field.getFieldName();
		String queryValue = term.replace("?", "_").replace("*", "%");
		queryBuilder.append("_source->>'" + fieldName + "' ILIKE '%" + queryValue + "%'");
	}

	@Override
	public void endField(EsFieldQuery field) {
	}
}

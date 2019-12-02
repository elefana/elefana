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
package com.elefana.search;

import com.codahale.metrics.Histogram;
import com.elefana.api.search.SearchHit;
import com.elefana.api.search.SearchResponse;
import com.elefana.util.EscapeUtils;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class SearchHitsQueryExecutor {
	protected final JdbcTemplate jdbcTemplate;
	protected final Histogram searchHitsTime, searchHits;
	
	public SearchHitsQueryExecutor(JdbcTemplate jdbcTemplate, Histogram searchHitsTime, Histogram searchHits) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.searchHitsTime = searchHitsTime;
		this.searchHits = searchHits;
	}
	
	public abstract ResultSet queryHitsCount(Statement statement, PsqlQueryComponents queryComponents,
	                                         long startTime, int from, int size) throws SQLException;

	public abstract ResultSet queryHits(Statement statement, PsqlQueryComponents queryComponents,
	                                    long startTime, int from, int size) throws SQLException;
	
	public Callable<SearchResponse> executeCountQuery(final SearchResponse searchResponse, final Statement statement,
	                                                  PsqlQueryComponents queryComponents, long startTime, int from, int size) {
		return new Callable<SearchResponse>() {
			@Override
			public SearchResponse call() throws Exception {
				ResultSet resultSet = null;
				try {
					resultSet = queryHitsCount(statement, queryComponents, startTime, from, size);
				} catch (Exception e) {
					e.printStackTrace();
					if (!e.getMessage().contains("No results")) {
						throw e;
					}
				}
				if (resultSet == null) {
					searchResponse.getHits().setTotal(0);
				} else {
					searchResponse.getHits().setTotal(getTotalHits(resultSet));
					resultSet.close();
				}
				searchHits.update(searchResponse.getHits().getTotal());
				return searchResponse;
			}
		};
	}

	public Callable<SearchResponse> executeHitsQuery(final SearchResponse searchResponse, final Statement statement,
	                                                 PsqlQueryComponents queryComponents, long startTime, int from, int size) {
		return new Callable<SearchResponse>() {
			@Override
			public SearchResponse call() throws Exception {
				if(size > 0) {
					ResultSet resultSet = null;
					try {
						resultSet = queryHits(statement, queryComponents, startTime, from, size);
					} catch (Exception e) {
						e.printStackTrace();
						if (!e.getMessage().contains("No results")) {
							throw e;
						}
						resultSet = null;
					}
	
					if (resultSet != null) {
						populateHits(searchResponse.getHits().getHits(), resultSet);
						resultSet.close();
					}
					searchResponse.getHits().setMaxScore(1.0);
	
					searchHitsTime.update(System.currentTimeMillis() - startTime);
				}
				return searchResponse;
			}
		};
	}
	
	private int getTotalHits(ResultSet countsRowSet) throws SQLException {
		int totalHits = 0;
		boolean hasCountColumn = true;
		while (countsRowSet.next()) {
			if (hasCountColumn) {
				try {
					totalHits += countsRowSet.getInt("count");
				} catch (Exception e) {
					hasCountColumn = false;
				}
			}
			if (!hasCountColumn) {
				totalHits++;
			}
		}
		return totalHits;
	}
	
	private void populateHits(List<SearchHit> results, ResultSet hitsRowSet) throws SQLException {
		while (hitsRowSet.next()) {
			SearchHit searchHit = new SearchHit();
			searchHit._index = hitsRowSet.getString("_index");
			searchHit._type = hitsRowSet.getString("_type");
			searchHit._id = hitsRowSet.getString("_id");
			searchHit._score = 1.0;
			searchHit._source = JsonIterator.deserialize(EscapeUtils.psqlUnescapeString(hitsRowSet.getString("_source")),
					new TypeLiteral<Map<String, Object>>() {
					});
			results.add(searchHit);
		}
	}
}

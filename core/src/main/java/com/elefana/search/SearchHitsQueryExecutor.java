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

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.codahale.metrics.Histogram;
import com.elefana.api.search.SearchHit;
import com.elefana.api.search.SearchResponse;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;

public abstract class SearchHitsQueryExecutor {
	protected final JdbcTemplate jdbcTemplate;
	protected final Histogram searchTime, searchHits;
	
	public SearchHitsQueryExecutor(JdbcTemplate jdbcTemplate, Histogram searchTime, Histogram searchHits) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.searchTime = searchTime;
		this.searchHits = searchHits;
	}
	
	protected abstract SqlRowSet queryHitsCount(PsqlQueryComponents queryComponents, long startTime, int from, int size);

	protected abstract SqlRowSet queryHits(PsqlQueryComponents queryComponents, long startTime, int from, int size);

	public SearchResponse executeQuery(PsqlQueryComponents queryComponents, long startTime, int from, int size) {
		final SearchResponse result = new SearchResponse();
		result.getShards().put("total", 1);
		result.getShards().put("successful", 1);
		result.getShards().put("failed", 0);
		
		SqlRowSet countRowSet = null;
		SqlRowSet hitsRowSet = null;
		try {
			countRowSet = queryHitsCount(queryComponents, startTime, from, size);
		} catch (Exception e) {
			e.printStackTrace();
			if (!e.getMessage().contains("No results")) {
				throw e;
			}
			countRowSet = null;
		}
		try {
			hitsRowSet = queryHits(queryComponents, startTime, from, size);
		} catch (Exception e) {
			e.printStackTrace();
			if (!e.getMessage().contains("No results")) {
				throw e;
			}
			hitsRowSet = null;
		}

		if (countRowSet == null) {
			result.getHits().setTotal(0);
		} else {
			result.getHits().setTotal(getTotalHits(countRowSet));
			populateHits(result.getHits().getHits(), hitsRowSet);
		}
		
		result.getHits().setMaxScore(1.0);

		final long took = System.currentTimeMillis() - startTime;
		result.setTook(took);
		result.setTimedOut(false);

		searchHits.update(result.getHits().getTotal());
		searchTime.update(took);
		return result;
	}
	
	private int getTotalHits(SqlRowSet countsRowSet) {
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
	
	private void populateHits(List<SearchHit> results, SqlRowSet hitsRowSet) {
		while (hitsRowSet.next()) {
			SearchHit searchHit = new SearchHit();
			searchHit._index = hitsRowSet.getString("_index");
			searchHit._type = hitsRowSet.getString("_type");
			searchHit._id = hitsRowSet.getString("_id");
			searchHit._score = 1.0;
			searchHit._source = JsonIterator.deserialize(IndexUtils.psqlUnescapeString(hitsRowSet.getString("_source")),
					new TypeLiteral<Map<String, Object>>() {
					});
			results.add(searchHit);
		}
	}
}

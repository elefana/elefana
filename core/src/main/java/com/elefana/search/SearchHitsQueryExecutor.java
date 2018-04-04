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

	protected abstract SqlRowSet queryHits(PsqlQueryComponents queryComponents, long startTime, int from, int size);

	public SearchResponse executeQuery(PsqlQueryComponents queryComponents, long startTime, int from, int size) {
		final SearchResponse result = new SearchResponse();
		result.getShards().put("total", 1);
		result.getShards().put("successful", 1);
		result.getShards().put("failed", 0);
		
		SqlRowSet rowSet = null;
		try {
			rowSet = queryHits(queryComponents, startTime, from, size);
		} catch (Exception e) {
			e.printStackTrace();
			if (!e.getMessage().contains("No results")) {
				throw e;
			}
			rowSet = null;
		}

		if (rowSet == null) {
			result.getHits().setTotal(0);
		} else if (size > 0) {
			int count = 0;
			int hitOffset = 0;
			boolean lastRowValid = true;
			while (hitOffset < from && (lastRowValid = rowSet.next())) {
				hitOffset++;
				count++;
			}
			if(lastRowValid) {
				while ((count - hitOffset) < size && (lastRowValid = rowSet.next())) {
					SearchHit searchHit = new SearchHit();
					searchHit._index = rowSet.getString("_index");
					searchHit._type = rowSet.getString("_type");
					searchHit._id = rowSet.getString("_id");
					searchHit._score = 1.0;
					searchHit._source = JsonIterator.deserialize(IndexUtils.psqlUnescapeString(rowSet.getString("_source")),
							new TypeLiteral<Map<String, Object>>() {
							});
					result.getHits().getHits().add(searchHit);
					count++;
				}
			}
			if(lastRowValid) {
				while (rowSet.next()) {
					count++;
				}
			}
			result.getHits().setTotal(count);
		} else {
			int totalHits = 0;
			boolean hasCountColumn = true;
			while (rowSet.next()) {
				if (hasCountColumn) {
					try {
						totalHits += rowSet.getInt("count");
					} catch (Exception e) {
						hasCountColumn = false;
					}
				}
				if (!hasCountColumn) {
					totalHits++;
				}
			}
			result.getHits().setTotal(totalHits);
		}
		result.getHits().setMaxScore(1.0);

		final long took = System.currentTimeMillis() - startTime;
		result.setTook(took);
		result.setTimedOut(false);

		searchHits.update(result.getHits().getTotal());
		searchTime.update(took);
		return result;
	}
}

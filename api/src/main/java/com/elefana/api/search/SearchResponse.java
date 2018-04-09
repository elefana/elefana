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
package com.elefana.api.search;

import java.util.HashMap;
import java.util.Map;

import com.elefana.api.ApiResponse;
import com.jsoniter.annotation.JsonProperty;
import com.jsoniter.output.JsonStream;

import io.netty.handler.codec.http.HttpResponseStatus;

public class SearchResponse extends ApiResponse {
	private final SearchHits hits = new SearchHits();
	@JsonProperty("_shards")
	private final Map<String, Object> shards = new HashMap<String, Object>();
	private Map<String, Object> aggregations;
	private volatile long took;
	@JsonProperty("timed_out")
	private volatile boolean timedOut;

	public SearchResponse() {
		super(HttpResponseStatus.OK.code());
	}

	public long getTook() {
		return took;
	}

	public void setTook(long took) {
		this.took = took;
	}

	public boolean isTimedOut() {
		return timedOut;
	}

	public void setTimedOut(boolean timedOut) {
		this.timedOut = timedOut;
	}

	public SearchHits getHits() {
		return hits;
	}

	public Map<String, Object> getShards() {
		return shards;
	}

	public Map<String, Object> getAggregations() {
		return aggregations;
	}

	public void setAggregations(Map<String, Object> aggregations) {
		this.aggregations = aggregations;
	}

	@Override
	public String toJsonString() {
		return JsonStream.serialize(this);
	}
}

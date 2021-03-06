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
package com.elefana.util;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.indices.IndexTemplateService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.api.exception.ElefanaException;


public class CoreIndexUtilsTest {
	private final CoreIndexUtils indexUtils = new CoreIndexUtils();

	private JdbcTemplate jdbcTemplate;
	private IndexTemplateService indexTemplateService;

	private GetIndexTemplateForIndexRequest indexTemplateForIndexRequest;
	private GetIndexTemplateForIndexResponse indexTemplateForIndexResponse;

	@Before
	public void setUp() {
		jdbcTemplate = mock(JdbcTemplate.class);
		indexTemplateService = mock(IndexTemplateService.class);
		indexTemplateForIndexRequest = mock(GetIndexTemplateForIndexRequest.class);
		indexTemplateForIndexResponse = mock(GetIndexTemplateForIndexResponse.class);

		indexUtils.setJdbcTemplate(jdbcTemplate);
		indexUtils.setIndexTemplateService(indexTemplateService);
	}

	@After
	public void teardown() {
		validateMockitoUsage();
	}
	
	@Test
	public void testGenerateDocumentId() {
		final String index = "message-logs-2018-03-15t23:00:00";
		final String type = "message";
		final String source1 = "{\"message\":\"This is message 1\"}";
		final String source2 = "{\"message\":\"This is message 1\"}";
		
		for(int i = 0; i < 50000; i++) {
			final String result1 = indexUtils.generateDocumentId(index, type, source1);
			final String result2 = indexUtils.generateDocumentId(index, type, source2);
			Assert.assertNotEquals(result1, result2);
		}
		for(int i = 0; i < 50000; i++) {
			final String result1 = indexUtils.generateDocumentId(index, type, source1);
			final String result2 = indexUtils.generateDocumentId(index, type, source1);
			Assert.assertNotEquals(result1, result2);
		}
	}
	
	@Test
	public void testListIndicesForIndexPattern() throws ElefanaException {
		final List<String> indices = new ArrayList<String>();
		indices.add("message-logs-2018-03-15t23:00:00");
		indices.add("message-logs-2018-03-16t00:00:00");
		indices.add("message-logs-2018-03-16t01:00:00");
		
		final List<Map<String, Object>> sqlRows = new ArrayList<Map<String, Object>>();
		for(String index : indices) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put("_index", index);
			sqlRows.add(row);
		}
		
		final String allMessageLogsPattern = "message-logs-*";
		final String todayMessageLogsPattern = "message-logs-2018-03-16*";
		
		when(jdbcTemplate.queryForList(anyString())).thenReturn(sqlRows);

		final List<String> allMessageLogs = indexUtils.listIndicesForIndexPatternFromDatabase(allMessageLogsPattern);
		Assert.assertEquals(3, allMessageLogs.size());
		Assert.assertEquals(indices, allMessageLogs);
		
		final List<String> todaysMessageLogs = indexUtils.listIndicesForIndexPatternFromDatabase(todayMessageLogsPattern);
		Assert.assertEquals(2, todaysMessageLogs.size());
		Assert.assertEquals(false, todaysMessageLogs.contains("message-logs-2018-03-15t23:00:00"));
	}

	@Test
	public void testGetTimestamp() throws ElefanaException {
		final String index = "message-logs";
		final long expectedTimestamp = 5634653566534L;
		final String json = "{\"timestamp\":" + expectedTimestamp +"}";
		final char [] jsonCharArray = json.toCharArray();

		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.getStorage().setTimestampPath("timestamp");

		when(indexTemplateService.prepareGetIndexTemplateForIndex(null, index)).thenReturn(indexTemplateForIndexRequest);
		when(indexTemplateForIndexRequest.get()).thenReturn(indexTemplateForIndexResponse);
		when(indexTemplateForIndexResponse.getIndexTemplate()).thenReturn(indexTemplate);

		final long result = indexUtils.getTimestamp(index, jsonCharArray, jsonCharArray.length);
		Assert.assertEquals(expectedTimestamp, result);
	}
}

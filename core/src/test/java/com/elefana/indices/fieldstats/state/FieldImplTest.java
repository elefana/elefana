/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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
package com.elefana.indices.fieldstats.state;

import com.elefana.indices.fieldstats.state.field.FieldImpl;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldImplTest {
	private static final String INDEX_1 = "test_index_1";
	private static final String INDEX_2 = "test_index_2";
	private static final String INDEX_NON_EXISTANT = "test_index_-1";

	private final FieldImpl field = new FieldImpl(Long.class);

	@Test
	public void testGetIndexFieldStatsWithSingleIndex() {
		final long expectedDocCount = 101L;

		final FieldStats<Long> fieldStats = field.getIndexFieldStats(INDEX_1);
		fieldStats.addDocumentCount(expectedDocCount);

		final List<String> indices = new ArrayList<String>();
		indices.add(INDEX_1);
		final FieldStats<Long> result = field.getIndexFieldStats(indices);
		Assert.assertEquals(expectedDocCount, result.getDocumentCount());
	}

	@Test
	public void testGetIndexFieldStatsWithMultipleIndices() {
		final long expectedDocCountPerIndex = 107L;

		final List<String> indices = new ArrayList<String>();
		indices.add(INDEX_1);
		indices.add(INDEX_2);

		for(String index : indices) {
			final FieldStats<Long> fieldStats = field.getIndexFieldStats(index);
			fieldStats.addDocumentCount(expectedDocCountPerIndex);
		}

		final FieldStats<Long> result = field.getIndexFieldStats(indices);
		Assert.assertEquals(expectedDocCountPerIndex * indices.size(), result.getDocumentCount());
	}

	@Test
	public void testGetIndexFieldStatsWithMultipleIndicesAndNonExistant() {
		final long expectedDocCountPerIndex = 107L;

		final List<String> indices = new ArrayList<String>();
		indices.add(INDEX_1);
		indices.add(INDEX_2);

		for(String index : indices) {
			final FieldStats<Long> fieldStats = field.getIndexFieldStats(index);
			fieldStats.addDocumentCount(expectedDocCountPerIndex);
		}

		indices.clear();
		indices.add(INDEX_NON_EXISTANT);
		indices.add(INDEX_1);
		indices.add(INDEX_2);

		final FieldStats<Long> result = field.getIndexFieldStats(indices);
		Assert.assertEquals(expectedDocCountPerIndex * 2, result.getDocumentCount());
	}
}

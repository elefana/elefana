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
package com.elefana.document;

import com.codahale.metrics.Timer;
import com.elefana.api.exception.ElefanaException;
import com.elefana.document.ingest.TimeIngestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class BulkTimeIndexTask extends BulkIndexTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkTimeIndexTask.class);

	private final TimeIngestTable timeIngestTable;
	private final int shardOffset;

	public BulkTimeIndexTask(JdbcTemplate jdbcTemplate, List<BulkIndexOperation> indexOperations,
	                         String index, TimeIngestTable timeIngestTable, boolean flatten, int from, int size, int shardOffset,
	                         Timer psqlTimer, Timer batchBuildTimer, Timer flattenTimer, Timer escapeTimer) {
		super(jdbcTemplate, indexOperations, index, timeIngestTable, flatten, from, size,
				psqlTimer, batchBuildTimer, flattenTimer, escapeTimer);
		this.timeIngestTable = timeIngestTable;
		this.shardOffset = shardOffset;
	}

	public int getShardOffset() {
		return shardOffset;
	}

	@Override
	protected int getStagingTableId() throws ElefanaException {
		return timeIngestTable.lockTable(shardOffset);
	}
}

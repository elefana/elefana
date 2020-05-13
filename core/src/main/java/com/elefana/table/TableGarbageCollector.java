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
package com.elefana.table;

import com.elefana.node.NodeStatsService;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@DependsOn("nodeStatsService")
public class TableGarbageCollector implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableGarbageCollector.class);
	private static final long GC_TIME_MILLIS = 1000L;
	
	private final Queue<String> tableDeletionQueue = new ConcurrentLinkedQueue<String>();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("elefana-table-garbage-collector"));
	
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private NodeStatsService nodeStatsService;
	
	@PostConstruct
	public void postConstruct() {
		if(!nodeStatsService.isMasterNode()) {
			//Only master node can execute search
			return;
		}
		scheduledExecutorService.scheduleAtFixedRate(this, 1L, GC_TIME_MILLIS, TimeUnit.MILLISECONDS);
	}
	
	@PreDestroy
	public void preDestroy() {
		scheduledExecutorService.shutdown();

		try {
			scheduledExecutorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
	}
	
	public void queueTemporaryTablesForDeletion(List<String> tablesNames) {
		for(int i = 0; i < tablesNames.size(); i++) {
			final String tableName = tablesNames.get(i);
			if(tableName == null) {
				continue;
			}
			tableDeletionQueue.offer(tableName);
		}
	}
	
	@Override
	public void run() {
		while(!tableDeletionQueue.isEmpty()) {
			String nextTable = tableDeletionQueue.poll();
			indexUtils.deleteTemporaryTable(nextTable);
			LOGGER.info("Deleted temporary table '" + nextTable + "'");
		}
	}

}

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

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import com.elefana.node.NodeSettingsService;

@Service
public class TableGarbageCollector implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableGarbageCollector.class);
	
	private final Queue<String> tableDeletionQueue = new ConcurrentLinkedQueue<String>();
	private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
	
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private TaskScheduler taskScheduler;
	@Autowired
	private TableUtils tableUtils;
	
	@PostConstruct
	public void postConstruct() {
		taskScheduler.scheduleWithFixedDelay(this, nodeSettingsService.getGarbageCollectionInterval());
	}
	
	@PreDestroy
	public void preDestroy() {
		shutdownInitiated.set(true);
	}
	
	public void scheduleTablesForDeletion(Collection<String> tableNames) {
		for(String tableName : tableNames) {
			scheduleTableForDeletion(tableName);
		}
	}
	
	public void scheduleTableForDeletion(String tableName) {
		tableDeletionQueue.offer(tableName);
	}

	@Override
	public void run() {
		try {
			if(shutdownInitiated.get()) {
				while(!tableDeletionQueue.isEmpty()) {
					deleteNextTable();
				}
				return;
			}
			deleteNextTable();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void deleteNextTable() {
		String nextTable = tableDeletionQueue.poll();
		if(nextTable == null) {
			return;
		}
		tableUtils.deleteTable(nextTable);
		LOGGER.info("[GC] Removed " + nextTable);
	}
}

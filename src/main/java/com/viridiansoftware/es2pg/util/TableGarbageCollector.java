/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.util;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

@Service
public class TableGarbageCollector implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableGarbageCollector.class);
	
	private final Queue<String> tableDeletionQueue = new ConcurrentLinkedQueue<String>();
	private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
	
	@Autowired
	private Environment environment;
	@Autowired
	private TaskScheduler taskScheduler;
	@Autowired
	private TableUtils tableUtils;
	
	private long deletionFrequency;
	
	@PostConstruct
	public void postConstruct() {
		deletionFrequency = environment.getProperty("es2pg.gc.frequency", Long.class, 1000L);
		taskScheduler.scheduleWithFixedDelay(this, deletionFrequency);
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
		taskScheduler.scheduleWithFixedDelay(this, deletionFrequency);
	}

	private void deleteNextTable() {
		String nextTable = tableDeletionQueue.poll();
		if(nextTable == null) {
			return;
		}
		tableUtils.deleteTable(nextTable);
	}
}

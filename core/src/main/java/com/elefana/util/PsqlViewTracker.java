/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.exception.ElefanaException;
import com.elefana.node.NodeStatsService;
import com.elefana.search.psql.PsqlSearchService;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
@DependsOn("nodeStatsService")
public class PsqlViewTracker implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlViewTracker.class);
	private static final String VIEW_NAME_PREFIX = "efv_" + System.currentTimeMillis();

	private final Queue<String> cleanupQueue = new ConcurrentLinkedQueue<>();
	private final List<String> views = new ArrayList<String>();
	private final Lock lock = new ReentrantLock();

	@Autowired
	private NodeStatsService nodeStatsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private ExecutorService executorService = null;
	private int viewCounter = 0;

	@PostConstruct
	public void postConstruct() {
		if(!nodeStatsService.isMasterNode()) {
			return;
		}
		executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("elefana-view-tracker", ThreadPriorities.VIEW_TRACKER));
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeStatsService.isMasterNode()) {
			return;
		}
		try {
			lock.lock();
			cleanupQueue.addAll(views);
			executorService.submit(this);
			lock.unlock();

			executorService.shutdown();
			executorService.awaitTermination(5, TimeUnit.MINUTES);
		} catch (Exception e) {}
	}

	public String getNextViewName() {
		lock.lock();
		try {
			final String result = VIEW_NAME_PREFIX + viewCounter++;
			views.add(result);
			return result;
		} finally {
			lock.unlock();
		}
	}

	public void queueViewForCleanup(String viewName) {
		cleanupQueue.offer(viewName);
		executorService.submit(this);
	}

	@Override
	public void run() {
		if(cleanupQueue.isEmpty()) {
			return;
		}
		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			while(!cleanupQueue.isEmpty()) {
				final String viewName = cleanupQueue.peek();
				final String query = "DROP MATERIALIZED VIEW IF EXISTS " + viewName;
				LOGGER.info(query);

				final PreparedStatement preparedStatement = connection.prepareStatement(query);
				preparedStatement.execute();
				preparedStatement.close();

				cleanupQueue.poll();
			}
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {}
			}
		}
	}
}

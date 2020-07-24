/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.ElefanaChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class DiskBackedQueue<T extends BytesMarshallable> implements StoreFileListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(DiskBackedQueue.class);
	private static final String QUEUES_DIRECTORY = "queues";

	private final String queueId;
	private final Class<T> clazz;
	private final ElefanaChronicleQueue chronicleQueue;
	private final ExcerptTailer tailer;
	private final DiskBackedMap<Integer, QueueCycleFile> files;

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz) {
		this(queueId, dataDirectory, clazz,  RollCycles.DAILY);
	}

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz, RollCycles rollCycles) {
		this(queueId, dataDirectory, clazz,  rollCycles, SystemTimeProvider.INSTANCE);
	}

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz,
	                       RollCycles rollCycles, TimeProvider timeProvider) {
		this.queueId = queueId;
		this.clazz = clazz;
		files = new DiskBackedMap<>(queueId + "-files", Integer.class, QueueCycleFile.class,
				dataDirectory, 512, Integer.MAX_VALUE, new QueueCycleFile(dataDirectory));

		if(!dataDirectory.exists()) {
			dataDirectory.mkdirs();
		}
		final File queuesDir = new File(dataDirectory, QUEUES_DIRECTORY);
		if(!queuesDir.exists()) {
			queuesDir.mkdirs();
		}
		final File queueDir = new File(queuesDir, queueId);
		if(!queueDir.exists()) {
			queueDir.mkdirs();
		}

		chronicleQueue = ElefanaChronicleQueue.singleBuilder(queueDir).
				rollCycle(rollCycles).
				timeProvider(timeProvider).
				storeFileListener(this).build();
		tailer = chronicleQueue.createTailer(queueId + "-tailer");
	}

	public int prune() {
		int totalPruned = 0;
		for(int cycle : files.keySet()) {
			synchronized(tailer) {
				if(cycle > tailer.cycle()) {
					continue;
				}
			}
			final QueueCycleFile file = files.get(cycle);
			if(!file.isReleased()) {
				continue;
			}
			try {
				if(!chronicleQueue.unlockFiles(cycle, file.getFile())) {
					continue;
				}
				Files.delete(file.getFile().toPath());
				chronicleQueue.refreshDirectoryListing();
				files.remove(cycle);

				LOGGER.info("Pruned " + file.getFile() + " (Exists: " + file.getFile().exists() + ")");
				totalPruned++;
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		return totalPruned;
	}

	@Override
	public void onAcquired(int cycle, File file) {
		files.compute(cycle, (cycleKey, queueCycleFile) -> {
			if(queueCycleFile == null) {
				return new QueueCycleFile(file);
			} else {
				queueCycleFile.setReleased(false);
				return queueCycleFile;
			}
		});
	}

	@Override
	public void onReleased(int cycle, File file) {
		try {
			files.computeIfPresent(cycle, (integer, queueCycleFile) -> {
				queueCycleFile.setReleased(true);
				return queueCycleFile;
			});
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void dispose() {
		try {
			chronicleQueue.close();
			files.dispose();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public boolean isEmpty() {
		synchronized(tailer) {
			return !tailer.peekDocument();
		}
	}

	public void clear() {
		chronicleQueue.clear();
	}

	public boolean peek(T result) {
		synchronized(tailer) {
			long oldIndex = tailer.index();
			tailer.direction(TailerDirection.FORWARD);
			try (DocumentContext context = tailer.readingDocument()) {
				if (context.isPresent()) {
					if (oldIndex == 0) {
						oldIndex = context.index();
					}
					tailer.readBytes(result);
				}
			}
			tailer.moveToIndex(oldIndex);
			return true;
		}

	}

	public boolean poll(T result) {
		synchronized(tailer) {
			try (DocumentContext context = tailer.readingDocument()) {
				if(!context.isPresent()) {
					return false;
				}
				return context.wire().readBytes(result);
			}
		}
	}

	public boolean offer(T t) {
		try {
			final ExcerptAppender appender = chronicleQueue.acquireAppender();
			try (DocumentContext context = appender.writingDocument()) {
				context.wire().writeBytes(t);
			}
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	public int getFileCount() {
		return files.size();
	}
}

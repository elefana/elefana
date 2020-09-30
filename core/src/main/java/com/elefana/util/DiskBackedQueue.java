/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.ElefanaChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public class DiskBackedQueue<T extends BytesMarshallable> implements StoreFileListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(DiskBackedQueue.class);
	private static final String QUEUES_DIRECTORY = "queues";

	private final String queueId;
	private final Class<T> clazz;
	private final ElefanaChronicleQueue chronicleQueue;
	private final ExcerptTailer tailer;
	private final DiskBackedMap<Integer, QueueCycleFile> files;

	private final AtomicBoolean disposed = new AtomicBoolean(false);

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz) {
		this(queueId, dataDirectory, clazz, false);
	}

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz, boolean cleanImmediately) {
		this(queueId, dataDirectory, clazz,  RollCycles.DAILY, cleanImmediately);
	}

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz, RollCycles rollCycles) {
		this(queueId, dataDirectory, clazz,  rollCycles, false);
	}


	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz, RollCycles rollCycles, boolean cleanImmediately) {
		this(queueId, dataDirectory, clazz,  rollCycles, SystemTimeProvider.INSTANCE, cleanImmediately);
	}

	public DiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz,
	                       RollCycles rollCycles, TimeProvider timeProvider, boolean cleanImmediately) {
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

		if(cleanImmediately && queueDir.listFiles() != null) {
			for(File file : queueDir.listFiles()) {
				file.delete();
			}
		}

		chronicleQueue = ElefanaChronicleQueue.singleBuilder(queueDir).
				rollCycle(rollCycles).
				timeProvider(timeProvider).
				storeFileListener(this).build();
		tailer = chronicleQueue.createTailer(queueId + "-tailer");

		if(cleanImmediately) {
			files.clear();
		}
	}

	public int prune() {
		if(disposed.get()) {
			return 0;
		}
		int totalPruned = 0;
		for(int cycle : files.keySet()) {
			if(disposed.get()) {
				return totalPruned;
			}
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
		if(disposed.get()) {
			return;
		}
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
		if(disposed.get()) {
			return;
		}
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
		if(disposed.getAndSet(true)) {
			return;
		}
		try {
			chronicleQueue.close();
			files.dispose();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void clear() {
		if(disposed.get()) {
			return;
		}
		chronicleQueue.clear();
	}

	public boolean peek(T result) {
		if(disposed.get()) {
			return false;
		}
		synchronized(tailer) {
			long oldIndex = tailer.index();
			int oldCycle = tailer.cycle();
			long oldSequence = chronicleQueue.rollCycle().toSequenceNumber(oldIndex);
			if (oldIndex == 0) {
				tailer.toStart();
				tailer.direction(TailerDirection.FORWARD);
			} else {
				tailer.direction(TailerDirection.NONE);
			}

			boolean success = true;
			try (DocumentContext context = tailer.readingDocument()) {
				if (context.isPresent()) {
					context.wire().readBytes(result);
				} else {
					success = false;
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
			if(oldIndex == 0) {
				tailer.toStart();
			}
			tailer.direction(TailerDirection.FORWARD);

			long newIndex = tailer.index();
			int newCycle = tailer.cycle();
			long newSequence = chronicleQueue.rollCycle().toSequenceNumber(newIndex);
			System.out.println(oldCycle + " " + oldSequence + " -> " + newCycle + " " + newSequence + " " + success);
			return success;
		}
	}

	public long getTailerIndex() {
		synchronized (tailer) {
			return tailer.index();
		}
	}

	public boolean poll(T result) {
		if(disposed.get()) {
			return false;
		}
		synchronized(tailer) {
			boolean success = false;
			int cycle = tailer.cycle();
			long previousSequenceNumber = chronicleQueue.rollCycle().toSequenceNumber(tailer.index());
			if(previousSequenceNumber == 0) {
				//tailer.moveToIndex(tailer.index() + 1);
			}
			System.out.println(tailer.cycle() + " " + chronicleQueue.rollCycle().toSequenceNumber(tailer.index()));

			try (DocumentContext context = tailer.readingDocument()) {
				if(!context.isPresent()) {
					System.out.println("HERE1");
					return false;
				}
				success = context.wire().readBytes(result);
			}

			if(!success) {
				System.out.println("HERE2");
				return false;
			}
			long sequenceNumber = chronicleQueue.rollCycle().toSequenceNumber(tailer.index());
			if(sequenceNumber == 1 && tailer.cycle() != cycle) {
				tailer.moveToIndex(tailer.index() - 1);
				System.out.println("HERE3 " + chronicleQueue.rollCycle().toSequenceNumber(tailer.index()));
			}
			return true;
		}
	}

	public boolean offer(T t) {
		if(disposed.get()) {
			return false;
		}
		try {
			final ExcerptAppender appender = chronicleQueue.acquireAppender();
			try (DocumentContext context = appender.writingDocument()) {
				context.wire().writeBytes(t);
			}
			System.out.println("LAST APPEND " + appender.cycle() + " " + chronicleQueue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean offerAll(Collection<T> t) {
		if(disposed.get()) {
			return false;
		}
		try {
			final ExcerptAppender appender = chronicleQueue.acquireAppender();
			for(T element : t) {
				try (DocumentContext context = appender.writingDocument()) {
					context.wire().writeBytes(element);
				}
			}
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	public int getFileCount() {
		if(disposed.get()) {
			return 0;
		}
		return files.size();
	}
}

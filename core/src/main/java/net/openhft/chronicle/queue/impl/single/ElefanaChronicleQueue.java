/**
 * Copyright 2020 Viridian Software Ltd.
 */
package net.openhft.chronicle.queue.impl.single;

import com.elefana.util.DiskBackedQueue;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.WireStoreSupplier;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;

public class ElefanaChronicleQueue extends SingleChronicleQueue {
	private static final Logger LOGGER = LoggerFactory.getLogger(ElefanaChronicleQueue.class);

	protected ElefanaChronicleQueue(@NotNull SingleChronicleQueueBuilder builder) {
		super(builder);
	}

	public StoreSupplier getStoreSupplier() {
		return (StoreSupplier) storeSupplier();
	}

	public MappedFile getMappedFile(File file) throws FileNotFoundException {
		return mappedFile(file);
	}

	public boolean unlockFiles(int cycle, File file) {
		return true;
//		final StoreSupplier storeSupplier = getStoreSupplier();
//		final SingleChronicleQueueStore store = storeSupplier.acquire(cycle, false);
//		if(store == null) {
//			return true;
//		}
//		try {
//			final MappedFile mappedFile = getMappedFile(file);
//			if(mappedFile != null) {
//				if(mappedFile.raf().getChannel().isOpen()) {
//					mappedFile.raf().close();
//				}
//				System.out.println(mappedFile.refCount());
//				mappedFile.close();
//				System.out.println(mappedFile.refCount());
//			}
//			store.close();
//		} catch (Exception e) {
//			LOGGER.error(e.getMessage(), e);
//			return false;
//		}
//		return true;
	}

	public static ElefanaChronicleQueueBuilder singleBuilder(@NotNull File path) {
		return ElefanaChronicleQueue.binary(path);
	}

	public static ElefanaChronicleQueueBuilder binary(@NotNull File basePathFile) {
		return ElefanaChronicleQueue.builder(basePathFile, WireType.BINARY_LIGHT);
	}

	public static ElefanaChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
		ElefanaChronicleQueueBuilder result = (ElefanaChronicleQueueBuilder) new ElefanaChronicleQueueBuilder().wireType(wireType);
		if (file.isFile()) {
			if (!file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
				throw new IllegalArgumentException("Invalid file type: " + file.getName());
			}
			result.path(file.getParentFile());
		} else
			result.path(file);

		return result;
	}
}

/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueCycleFile implements BytesMarshallable {
	private static final Logger LOGGER = LoggerFactory.getLogger(QueueCycleFile.class);
	private static final int MASHALLABLE_VERSION = 1;

	private final AtomicBoolean released = new AtomicBoolean(false);
	private File file;

	public QueueCycleFile() {}

	public QueueCycleFile(File file) {
		this.file = file;
	}

	@Override
	public void writeMarshallable(BytesOut bytes) {
		bytes.writeStopBit(MASHALLABLE_VERSION);
		bytes.writeUtf8(file.getAbsolutePath());
		bytes.writeBoolean(released.get());
	}

	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		long stopBit = bytes.readStopBit();
		if(stopBit != MASHALLABLE_VERSION) {
			throw new IllegalStateException("Expected stop bit to be " + MASHALLABLE_VERSION + " but was " + stopBit);
		}
		file = new File(bytes.readUtf8());
		released.set(bytes.readBoolean());
	}

	public File getFile() {
		return file;
	}

	public boolean isReleased() {
		return released.get();
	}

	public void setReleased(boolean released) {
		this.released.set(released);
	}
}

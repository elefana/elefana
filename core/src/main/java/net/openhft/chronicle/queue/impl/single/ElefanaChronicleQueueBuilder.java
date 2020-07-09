/**
 * Copyright 2020 Viridian Software Ltd.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.jetbrains.annotations.NotNull;

public class ElefanaChronicleQueueBuilder extends SingleChronicleQueueBuilder {

	@NotNull
	@Override
	public ElefanaChronicleQueue build() {
		preBuild();
		return new ElefanaChronicleQueue(this);
	}

	@NotNull
	@Override
	public ElefanaChronicleQueueBuilder rollCycle(@NotNull RollCycle rollCycle) {
		super.rollCycle(rollCycle);
		return this;
	}

	@Override
	public ElefanaChronicleQueueBuilder timeProvider(TimeProvider timeProvider) {
		super.timeProvider(timeProvider);
		return this;
	}

	@Override
	public ElefanaChronicleQueueBuilder storeFileListener(StoreFileListener storeFileListener) {
		super.storeFileListener(storeFileListener);
		return this;
	}
}

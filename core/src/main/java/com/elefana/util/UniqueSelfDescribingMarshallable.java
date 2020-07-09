/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public abstract class UniqueSelfDescribingMarshallable extends SelfDescribingMarshallable implements BytesMarshallable {

	public abstract String getKey();
}

/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

import com.elefana.api.indices.IndexGenerationMode;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class TableIndexDelay extends SelfDescribingMarshallable {
	private String tableName;
	private long indexTimestamp;
	private IndexGenerationMode mode;
	private boolean ginEnabled, brinEnabled;

	public TableIndexDelay() {}

	public TableIndexDelay(String tableName, long indexTimestamp, IndexGenerationMode mode, boolean ginEnabled, boolean brinEnabled) {
		this.tableName = tableName;
		this.indexTimestamp = indexTimestamp;
		this.mode = mode;
		this.ginEnabled = ginEnabled;
		this.brinEnabled = brinEnabled;
	}

	@Override
	public void writeMarshallable(BytesOut bytes) {
		bytes.writeUtf8(tableName);
		bytes.writeLong(indexTimestamp);
		bytes.writeInt(mode.ordinal());
		bytes.writeBoolean(ginEnabled);
		bytes.writeBoolean(brinEnabled);
	}

	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		tableName = bytes.readUtf8();
		indexTimestamp = bytes.readLong();
		mode = IndexGenerationMode.values()[bytes.readInt()];
		ginEnabled = bytes.readBoolean();
		brinEnabled = bytes.readBoolean();
	}

	public String getTableName() {
		return tableName;
	}

	public long getIndexTimestamp() {
		return indexTimestamp;
	}

	public IndexGenerationMode getMode() {
		return mode;
	}

	public boolean isGinEnabled() {
		return ginEnabled;
	}

	public boolean isBrinEnabled() {
		return brinEnabled;
	}
}

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

public class TableFieldIndexDelay extends TableIndexDelay {
	private String fieldName;

	public TableFieldIndexDelay() {
		super();
	}

	public TableFieldIndexDelay(String tableName, String fieldName, long indexTimestamp, IndexGenerationMode mode) {
		super(tableName, indexTimestamp, mode, true, true);
		this.fieldName = fieldName;
	}

	public String getFieldName() {
		return fieldName;
	}

	@Override
	public void writeMarshallable(BytesOut bytes) {
		super.writeMarshallable(bytes);
		bytes.writeUtf8(fieldName);
	}

	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		super.readMarshallable(bytes);
		fieldName = bytes.readUtf8();
	}
}

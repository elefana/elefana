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
package com.elefana.document.ingest;

import com.elefana.api.exception.ElefanaException;

public interface IngestTable {
	public int lockWrittenTable() throws ElefanaException;

	public int lockWrittenTable(long timeout) throws ElefanaException;

	public boolean isDataMarked(int index);

	public void markData(int index);

	public void unmarkData(int index);

	public void unlockTable(int index);

	public String getIngestionTableName(int index);

	public String getIndex();

	public int getCapacity();
}

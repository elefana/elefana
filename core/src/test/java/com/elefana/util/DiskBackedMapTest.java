/*******************************************************************************
 * Copyright 2020 Viridian Software Limited
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
package com.elefana.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

public class DiskBackedMapTest {

	@Test
	public void testClearImmediately() throws IOException {
		final String mapId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(mapId).toFile();
		final DiskBackedMap<Integer, Integer> map1 = new DiskBackedMap<>(mapId, Integer.class, Integer.class,
				dataDirectory, 1000, 0, 0, false);
		map1.put(77, 78);
		Assert.assertEquals(1, map1.size());
		map1.dispose();

		final DiskBackedMap<Integer, Integer> map2 = new DiskBackedMap<>(mapId, Integer.class, Integer.class,
				dataDirectory, 1000, 0, 0, true);
		Assert.assertEquals(0, map2.size());
	}
}

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
package com.elefana.table;

import com.elefana.api.indices.IndexGenerationMode;
import com.elefana.api.indices.IndexGenerationSettings;
import com.elefana.api.indices.IndexStorageSettings;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class TableIndexCreatorTest {
	private final TableIndexCreator tableIndexCreator = new TableIndexCreator();

	private JdbcTemplate jdbcTemplate;
	private DataSource dataSource;
	private Connection connection;
	private PreparedStatement preparedStatement;
	private NodeSettingsService nodeSettingsService;

	private File dataDirectory;

	@Before
	public void setUp() throws Exception {
		final String uuid = UUID.randomUUID().toString();
		dataDirectory = Files.createTempDirectory(uuid).toFile();

		jdbcTemplate = mock(JdbcTemplate.class);
		dataSource = mock(DataSource.class);
		connection = mock(Connection.class);
		preparedStatement = mock(PreparedStatement.class);
		nodeSettingsService = mock(NodeSettingsService.class);

		when(nodeSettingsService.isMasterNode()).thenReturn(true);
		when(nodeSettingsService.getMappingInterval()).thenReturn(1000L);
		when(nodeSettingsService.getDataDirectory()).thenReturn(dataDirectory);

		when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

		tableIndexCreator.setJdbcTemplate(jdbcTemplate);
		tableIndexCreator.setNodeSettingsService(nodeSettingsService);
		tableIndexCreator.initialise();
	}

	@After
	public void teardown() {
		validateMockitoUsage();
		tableIndexCreator.preDestroy();

		try {
			for(File file : dataDirectory.listFiles()) {
				if(file.isFile()) {
					file.deleteOnExit();
				}
			}
		} catch (Exception e) {}
	}

	@Test
	public void testGetPsqlIndexName() {
		final String indexName = "messages_m_logs_m_2020_f_09_f_21_f_01";
		final String fieldName = "sccp_udts_calling-party-address_global-title_digits";

		final String psqlIndex = TableIndexCreator.getPsqlIndexName(IndexUtils.HASH_INDEX_PREFIX, indexName, fieldName);
		Assert.assertTrue(psqlIndex.length() <= 63);
	}

	@Test
	public void testTableFieldIndexDelay() throws Exception {
		final String tableName = "tableName";
		final String fieldName = "fieldName";

		final IndexStorageSettings storageSettings = new IndexStorageSettings();
		storageSettings.setHashEnabled(true);
		storageSettings.setIndexGenerationSettings(new IndexGenerationSettings());
		storageSettings.getIndexGenerationSettings().setMode(IndexGenerationMode.PRESET);
		storageSettings.getIndexGenerationSettings().setIndexDelaySeconds(1L);
		storageSettings.getIndexGenerationSettings().setPresetIndexFields(new ArrayList<String>());
		storageSettings.getIndexGenerationSettings().getPresetIndexFields().add(fieldName);

		tableIndexCreator.createPsqlFieldIndex(null, tableName, fieldName, storageSettings);

		try {
			Thread.sleep(2500);
		} catch (Exception e) {}

		verify(connection, times(1)).prepareStatement(anyString());
		verify(preparedStatement, times(1)).execute();
		verify(preparedStatement, times(1)).close();
		verifyNoMoreInteractions(preparedStatement);
	}
}

/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.codahale.metrics.MetricRegistry;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexGenerationSettings;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.zaxxer.hikari.HikariDataSource;

import net.openhft.hashing.LongHashFunction;

/**
 *
 */
@Service
@DependsOn("nodeSettingsService")
public class CoreIndexUtils implements IndexUtils {
	private static final String [] DEFAULT_TABLESPACES = new String [] { "" };
	private static final Logger LOGGER = LoggerFactory.getLogger(CoreIndexUtils.class);
	
	private final Map<String, String []> jsonPathCache = new ConcurrentHashMap<String, String []>();
	private final Set<String> knownTables = new ConcurrentSkipListSet<String>();
	private final Lock tableCreationLock = new ReentrantLock();
	private final LongHashFunction xxHash = LongHashFunction.xx();
	
	@Autowired
	private Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private IndexTemplateService indexTemplateService;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private MetricRegistry metricRegistry;
	@Autowired
	private DbInitializer dbInitializer;
	
	private final AtomicInteger tablespaceIndex = new AtomicInteger();
	private String [] tablespaces;

	@PostConstruct
	public void postConstruct() throws SQLException {
		tablespaces = environment.getProperty("elefana.service.document.tablespaces", "").split(",");
		if(isEmptyTablespaceList(tablespaces)) {
			tablespaces = DEFAULT_TABLESPACES;
		}
		
		dbInitializer.initialiseDatabase();

		if (jdbcTemplate.getDataSource() instanceof HikariDataSource) {
			((HikariDataSource) jdbcTemplate.getDataSource()).setMetricRegistry(metricRegistry);
		}

		addAllKnownTables(listTables());
	}
	
	protected boolean addAllKnownTables(List<String> tableNames) {
		return knownTables.addAll(tableNames);
	}
	
	protected boolean addKnownTable(String tableName) {
		return knownTables.add(tableName);
	}
	
	protected boolean removeKnownTable(String tableName) {
		return knownTables.remove(tableName);
	}
	
	protected boolean isKnownTable(String tableName) {
		return knownTables.contains(tableName);
	}
	
	@Override
	public String generateDocumentId(String index, String type, String source) {
		return Long.toHexString(xxHash.hashChars(System.nanoTime() + index + type + source));
	}

	public List<String> listIndices() throws ElefanaException {
		final String query = "SELECT _index FROM " + PARTITION_TRACKING_TABLE;
		final List<String> results = new ArrayList<String>(1);

		try {
			for (Map<String, Object> row : jdbcTemplate.queryForList(query)) {
				final String index = (String) row.get("_index");
				results.add(index);
			}
			return results;
		} catch (Exception e) {
			throw new ShardFailedException(e);
		}
	}

	public List<String> listIndicesForIndexPattern(List<String> indexPatterns) throws ElefanaException {
		Set<String> results = new HashSet<String>();
		for (String indexPattern : indexPatterns) {
			results.addAll(listIndicesForIndexPattern(indexPattern));
		}
		return new ArrayList<String>(results);
	}

	public List<String> listIndicesForIndexPattern(String indexPattern) throws ElefanaException {
		final List<String> results = listIndices();
		final String[] patterns = indexPattern.split(",");

		for (int i = 0; i < patterns.length; i++) {
			patterns[i] = patterns[i].toLowerCase();
			patterns[i] = patterns[i].replace(".", "\\.");
			patterns[i] = patterns[i].replace("-", "\\-");
			patterns[i] = patterns[i].replace("*", "(.*)");
			patterns[i] = "^" + patterns[i] + "$";
		}

		for (int i = results.size() - 1; i >= 0; i--) {
			boolean matchesPattern = false;

			for (int j = 0; j < patterns.length; j++) {
				if (results.get(i).toLowerCase().matches(patterns[j])) {
					matchesPattern = true;
					break;
				}
			}
			if (matchesPattern) {
				continue;
			}
			results.remove(i);
		}
		return results;
	}

	public String getQueryTarget(String indexName) {
		if(!nodeSettingsService.isUsingCitus()) {
			return DATA_TABLE;
		}
		return getPartitionTableForIndex(indexName);
	}
	
	public long getTimestamp(String index, String document) throws ElefanaException {
		final GetIndexTemplateForIndexRequest indexTemplateForIndexRequest = indexTemplateService.prepareGetIndexTemplateForIndex(index);
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateForIndexRequest.get();
		final IndexTemplate indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		if(indexTemplate == null) {
			return System.currentTimeMillis();
		}
		if(!indexTemplate.isTimeSeries()) {
			return System.currentTimeMillis();
		}
		String timestampPath = indexTemplate.getStorage().getTimestampPath();
		if(timestampPath == null) {
			return System.currentTimeMillis();
		}
		
		String [] path = jsonPathCache.get(timestampPath);
		if(path == null) {
			path = timestampPath.split("\\.");
			jsonPathCache.put(timestampPath, path);
		}
		Any json = JsonIterator.deserialize(document);
		for(int i = 0; i < path.length; i++) {
			if(json.valueType().equals(ValueType.INVALID)) {
				return System.currentTimeMillis();
			}
			json = json.get(path[i]);
		}
		if(!json.valueType().equals(ValueType.NUMBER)) {
			return System.currentTimeMillis();
		}
		return json.toLong();
	}
	
	@Override
	public void ensureJsonFieldIndexExist(String indexName, List<String> fieldNames) throws ElefanaException {
		final IndexTemplate indexTemplate;
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateService.prepareGetIndexTemplateForIndex(indexName).get();
		if(indexTemplateForIndexResponse.getIndexTemplate() == null) {
			indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		} else {
			indexTemplate = null;
		}
		final String tableName = convertIndexNameToTableName(indexName);
		
		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			PreparedStatement preparedStatement;
			
			for(String fieldName : fieldNames) {
				if(indexTemplate != null) {
					final IndexGenerationSettings indexGenerationSettings = indexTemplate.getStorage().getIndexGenerationSettings();
					switch(indexGenerationSettings.getMode()) {
					case ALL:
						break;
					case PRESET:
						boolean matchedPresetField = false;
						for(String presetFieldName : indexGenerationSettings.getPresetFields()) {
							if(presetFieldName.equalsIgnoreCase(fieldName)) {
								matchedPresetField = true;
								break;
							}
						}
						if(!matchedPresetField) {
							continue;
						}
						break;
					case DYNAMIC:
					default:
						
						break;
					}
				}
				
				final String jsonIndexName = JSON_INDEX_PREFIX + tableName + "_" + fieldName;
				
				final StringBuilder createJsonFilterQuery = new StringBuilder();
				createJsonFilterQuery.append("CREATE INDEX IF NOT EXISTS ");
				createJsonFilterQuery.append(jsonIndexName);
				createJsonFilterQuery.append(" ON ");
				if (nodeSettingsService.isUsingCitus()) {
					createJsonFilterQuery.append(tableName);
				} else {
					createJsonFilterQuery.append(getPartitionTableForIndex(indexName));
				}
				createJsonFilterQuery.append("(elefana_json_field(_source,'");
				createJsonFilterQuery.append(fieldName);
				createJsonFilterQuery.append("'))");
				
				preparedStatement = connection.prepareStatement(createJsonFilterQuery.toString());
				preparedStatement.execute();
				preparedStatement.close();
			}
			
		} catch (Exception e) {
			if(connection != null) {
				try {
					connection.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
		if(connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ensureIndexExists(String indexName) throws ElefanaException {
		final String tableName = convertIndexNameToTableName(indexName);
		if (isKnownTable(tableName)) {
			return;
		}
		tableCreationLock.lock();
		if (isKnownTable(tableName)) {
			tableCreationLock.unlock();
			return;
		}
		
		final GetIndexTemplateForIndexRequest indexTemplateForIndexRequest = indexTemplateService.prepareGetIndexTemplateForIndex(indexName);
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateForIndexRequest.get();
		final IndexTemplate indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		boolean timeSeries = false;

		if (indexTemplate != null && indexTemplate.isTimeSeries()) {
			timeSeries = true;
		}

		final String timestampIndexName = TIMESTAMP_INDEX_PREFIX + tableName;
		final String ginIndexName = GIN_INDEX_PREFIX + tableName;
		final String constraintName = PRIMARY_KEY_PREFIX + tableName;

		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			PreparedStatement preparedStatement;

			final StringBuilder createTableQuery = new StringBuilder();
			createTableQuery.append("CREATE TABLE IF NOT EXISTS ");
			createTableQuery.append(tableName);
			
			if (nodeSettingsService.isUsingCitus()) {
				createTableQuery.append(" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, _source jsonb)");
			} else {
				createTableQuery.append(" PARTITION OF ");
				createTableQuery.append(DATA_TABLE);
				createTableQuery.append(" FOR VALUES in ('");
				createTableQuery.append(indexName);
				createTableQuery.append("')");
			}
			final String tablespace = tablespaces[tablespaceIndex.incrementAndGet() % tablespaces.length];
			if(tablespace != null && !tablespace.isEmpty()) {
				createTableQuery.append(" TABLESPACE ");
				createTableQuery.append(tablespace);
			}
			
			LOGGER.info(createTableQuery.toString());
			preparedStatement = connection.prepareStatement(createTableQuery.toString());
			preparedStatement.execute();
			preparedStatement.close();
			
			if (nodeSettingsService.isUsingCitus() && timeSeries) {
				final String createPrimaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName
						+ " PRIMARY KEY (_timestamp, _id);";
				LOGGER.info(createPrimaryKeyQuery);
				preparedStatement = connection.prepareStatement(createPrimaryKeyQuery);
				preparedStatement.execute();
				preparedStatement.close();
			} else {
				final String createPrimaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName
						+ " PRIMARY KEY (_id);";
				LOGGER.info(createPrimaryKeyQuery);
				preparedStatement = connection.prepareStatement(createPrimaryKeyQuery);
				preparedStatement.execute();
				preparedStatement.close();
			}

			final String createGinIndexQuery = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName
					+ " USING GIN (_source jsonb_ops)";
			LOGGER.info(createGinIndexQuery);
			preparedStatement = connection.prepareStatement(createGinIndexQuery);
			preparedStatement.execute();
			preparedStatement.close();
			
			final String createTimestampIndexQuery = "CREATE INDEX IF NOT EXISTS " + timestampIndexName + " ON " + tableName
					+ " (_timestamp)";
			LOGGER.info(createTimestampIndexQuery);
			preparedStatement = connection.prepareStatement(createTimestampIndexQuery);
			preparedStatement.execute();
			preparedStatement.close();

			final String createPartitionTrackingEntry = "INSERT INTO " + PARTITION_TRACKING_TABLE
					+ " (_index, _partitionTable) VALUES (?, ?) ON CONFLICT DO NOTHING";
			preparedStatement = connection.prepareStatement(createPartitionTrackingEntry);
			preparedStatement.setString(1, indexName);
			preparedStatement.setString(2, tableName);
			preparedStatement.execute();
			preparedStatement.close();

			if (nodeSettingsService.isUsingCitus()) {
				if (timeSeries) {
					preparedStatement = connection.prepareStatement(
							"SELECT create_distributed_table('" + tableName + "', '_timestamp', 'append');");
					preparedStatement.execute();
					preparedStatement.close();
				} else {
					preparedStatement = connection
							.prepareStatement("SELECT create_distributed_table('" + tableName + "', '_id');");
					preparedStatement.execute();
					preparedStatement.close();
				}
			}

			connection.close();
			addKnownTable(tableName);
			tableCreationLock.unlock();
		} catch (Exception e) {
			if(connection != null) {
				try {
					connection.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
			tableCreationLock.unlock();
			throw new ShardFailedException(e);
		}
	}

	public void deleteIndex(String indexName) {
		deleteTable(convertIndexNameToTableName(indexName));
	}

	private void deleteTable(String tableName) {
		jdbcTemplate.update("DROP TABLE IF EXISTS " + tableName + " CASCADE;");
		removeKnownTable(tableName);
	}
	
	@Override
	public void deleteTemporaryTable(String tableName) {
		jdbcTemplate.update("DROP TABLE IF EXISTS " + tableName + " CASCADE;");
	}

	private List<String> listTables() throws SQLException {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE;
		final List<String> results = new ArrayList<String>(1);

		for (Map<String, Object> row : jdbcTemplate.queryForList(query)) {
			results.add((String) row.get("_partitionTable"));
		}
		return results;
	}

	public String getIndexForPartitionTable(String partitionTable) {
		final String query = "SELECT _index FROM " + PARTITION_TRACKING_TABLE + " WHERE _partitionTable = ?";
		for (Map<String, Object> row : jdbcTemplate.queryForList(query, partitionTable)) {
			return (String) row.get("_index");
		}
		return null;
	}

	public String getPartitionTableForIndex(String index) {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE + " WHERE _index = ?";
		for (Map<String, Object> row : jdbcTemplate.queryForList(query, index)) {
			return (String) row.get("_partitionTable");
		}
		return null;
	}

	protected static String convertIndexNameToTableName(String indexName) {
		indexName = indexName.replace(".", "_f_");
		indexName = indexName.replace("-", "_m_");
		indexName = indexName.replace(":", "_c_");

		if (!Character.isLetter(indexName.charAt(0))) {
			indexName = "_" + indexName;
		}
		return indexName;
	}

	protected static String convertTableNameToIndexName(String tableName) {
		tableName = tableName.replace("_f_", ".");
		tableName = tableName.replace("_m_", "-");
		tableName = tableName.replace("_c_", ":");
		if (tableName.charAt(0) == '_') {
			return tableName.substring(1);
		}
		return tableName;
	}

	public static String destringifyJson(String json) {
		if (json.startsWith("\"")) {
			json = json.substring(1, json.length() - 1);
			json = json.replace("\\", "");
		}
		return json;
	}

	public static boolean isTypesEmpty(String[] types) {
		if (types == null) {
			return true;
		}
		if (types.length == 0) {
			return true;
		}
		for (int i = 0; i < types.length; i++) {
			if (types[i] == null) {
				continue;
			}
			if (types[i].isEmpty()) {
				continue;
			}
			return false;
		}
		return true;
	}
	
	private boolean isEmptyTablespaceList(String [] tablespaces) {
		if(tablespaces == null) {
			return true;
		}
		for(int i = 0; i < tablespaces.length; i++) {
			if(tablespaces[i] == null) {
				continue;
			}
			if(tablespaces[i].isEmpty()) {
				continue;
			}
			return false;
		}
		return true;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
}

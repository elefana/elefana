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

import com.codahale.metrics.MetricRegistry;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.*;
import com.elefana.api.json.JsonUtils;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.table.TableIndexCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.zaxxer.hikari.HikariDataSource;
import net.openhft.hashing.LongHashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
@Service
@DependsOn("nodeSettingsService")
public class CoreIndexUtils implements IndexUtils {
	private static final String[] DEFAULT_TABLESPACES = new String[] { "" };
	private static final IndexStorageSettings DEFAULT_INDEX_STORAGE_SETTINGS = new IndexStorageSettings();
	private static final Logger LOGGER = LoggerFactory.getLogger(CoreIndexUtils.class);

	private static final LoadingCache<String, String> INDEX_NAME_TO_TABLE_NAME_CACHE = CacheBuilder.newBuilder().
			maximumSize(100).expireAfterAccess(5L, TimeUnit.MINUTES).
			build(new CacheLoader<String, String>() {
				@Override
				public String load(String index) throws Exception {
					return internalConvertIndexNameToTableName(index);
				}
			});

	private final Map<String, String[]> jsonPathCache = new ConcurrentHashMap<String, String[]>();
	private final Set<String> knownTables = new ConcurrentSkipListSet<String>();
	private final Lock tableCreationLock = new ReentrantLock();
	private final LongHashFunction xxHash = LongHashFunction.xx();

	private final Map<String, ThreadLocal<NoAllocTimestampExtractor>> timestampExtractorCache = new ConcurrentHashMap<String, ThreadLocal<NoAllocTimestampExtractor> >();

	@Autowired
	private Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private IndexTemplateService indexTemplateService;
	@Autowired
	private TableIndexCreator tableIndexCreator;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private MetricRegistry metricRegistry;
	@Autowired
	private DbInitializer dbInitializer;

	private final TimeBasedGenerator uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface());
	private final AtomicInteger tablespaceIndex = new AtomicInteger();
	private String[] tablespaces;

	private LoadingCache<String, List<String>> indicesByPatternCache;
	private LoadingCache<String, String> indicesToPartitionTableCache, partitionTableToIndicesCache;

	@PostConstruct
	public void postConstruct() throws SQLException {
		tablespaces = environment.getProperty("elefana.service.document.tablespaces", "").split(",");
		if (isEmptyTablespaceList(tablespaces)) {
			tablespaces = DEFAULT_TABLESPACES;
		}

		indicesByPatternCache = CacheBuilder.newBuilder().
				maximumSize(environment.getProperty("elefana.service.document.cache.indexPattern.size", Integer.class, 25)).
				expireAfterAccess(environment.getProperty("elefana.service.document.cache.indexPattern.expiryMinutes", Long.class, 10L), TimeUnit.MINUTES).
				build(new CacheLoader<String, List<String>>() {
					@Override
					public List<String> load(String indexPattern) throws Exception {
						return listIndicesForIndexPatternFromDatabase(indexPattern);
					}
				});
		indicesToPartitionTableCache = CacheBuilder.newBuilder().
				maximumSize(environment.getProperty("elefana.service.document.cache.indexToPartitions.size", Integer.class, 250)).
				expireAfterAccess(environment.getProperty("elefana.service.document.cache.indexToPartitions.expiryMinutes", Long.class, 60L), TimeUnit.MINUTES).
				build(new CacheLoader<String, String>() {
					@Override
					public String load(String index) throws Exception {
						return getPartitionTableForIndex(index);
					}
				});
		partitionTableToIndicesCache = CacheBuilder.newBuilder().
				maximumSize(environment.getProperty("elefana.service.document.cache.partitionToIndices.size", Integer.class, 250)).
				expireAfterAccess(environment.getProperty("elefana.service.document.cache.partitionToIndices.expiryMinutes", Long.class, 60L), TimeUnit.MINUTES).
				build(new CacheLoader<String, String>() {
					@Override
					public String load(String partitionTable) throws Exception {
						return getIndexForPartitionTable(partitionTable);
					}
				});

		dbInitializer.initialiseDatabase();

		if (jdbcTemplate.getDataSource() instanceof HikariDataSource) {
			((HikariDataSource) jdbcTemplate.getDataSource()).setMetricRegistry(metricRegistry);
		}

		addAllKnownTables(listTables());
		tableIndexCreator.initialise();
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
	public String generateDocumentId(String index, String type, CharSequence source) {
		return uuidGenerator.generate().toString();
	}

	@Override
	public String generateDocumentId(String index, String type, char[] document, int documentLength) {
		return uuidGenerator.generate().toString();
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
		try {
			return indicesByPatternCache.get(indexPattern);
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return listIndicesForIndexPatternFromDatabase(indexPattern);
	}

	public List<String> listIndicesForIndexPatternFromDatabase(String indexPattern) throws ElefanaException {
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

	@Override
	public String getQueryTarget(Connection connection, String indexName) throws SQLException {
		if (!nodeSettingsService.isUsingCitus()) {
			return DATA_TABLE;
		}
		return getPartitionTableForIndex(connection, indexName);
	}

	public String getQueryTarget(String indexName) {
		if (!nodeSettingsService.isUsingCitus()) {
			return DATA_TABLE;
		}
		return getPartitionTableForIndex(indexName);
	}

	private NoAllocTimestampExtractor getTimestampExtractor(final String key) {
		return timestampExtractorCache.computeIfAbsent(key, s -> {
			return new ThreadLocal<NoAllocTimestampExtractor>() {
				@Override
				protected NoAllocTimestampExtractor initialValue() {
					return new NoAllocTimestampExtractor(key);
				}
			};
		}).get();
	}

	@Override
	public long getTimestamp(String index, String document) throws ElefanaException {
		final GetIndexTemplateForIndexRequest indexTemplateForIndexRequest = indexTemplateService
				.prepareGetIndexTemplateForIndex(null, index);
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateForIndexRequest.get();
		final IndexTemplate indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		if (indexTemplate == null) {
			return System.currentTimeMillis();
		}
		String timestampPath = indexTemplate.getStorage().getTimestampPath();
		if (timestampPath == null) {
			return System.currentTimeMillis();
		}

		final NoAllocTimestampExtractor timestampExtractor = getTimestampExtractor(timestampPath);
		final PooledStringBuilder str = PooledStringBuilder.allocate(document);
		final long result = timestampExtractor.extract(str);
		str.release();
		return result;
	}

	public long getTimestamp(String index, PooledStringBuilder document) throws ElefanaException {
		final GetIndexTemplateForIndexRequest indexTemplateForIndexRequest = indexTemplateService
				.prepareGetIndexTemplateForIndex(null, index);
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateForIndexRequest.get();
		final IndexTemplate indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		if (indexTemplate == null) {
			return System.currentTimeMillis();
		}
		String timestampPath = indexTemplate.getStorage().getTimestampPath();
		if (timestampPath == null) {
			return System.currentTimeMillis();
		}

		final NoAllocTimestampExtractor timestampExtractor = getTimestampExtractor(timestampPath);
		final long result = timestampExtractor.extract(document);
		return result;
	}

	@Override
	public long getTimestamp(String index, char[] document, int documentLength) throws ElefanaException {
		final PooledStringBuilder str = PooledStringBuilder.allocate();
		str.append(document, 0, documentLength);
		final long result = getTimestamp(index, str);
		str.release();
		return result;
	}

	@Override
	public void ensureJsonFieldIndexExist(String indexName, List<String> fieldNames) throws ElefanaException {
		final IndexTemplate indexTemplate;
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateService
				.prepareGetIndexTemplateForIndex(null, indexName).get();
		if (indexTemplateForIndexResponse.getIndexTemplate() != null) {
			indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		} else {
			return;
		}

		final IndexStorageSettings indexStorageSettings = indexTemplate.getStorage();
		if(!indexStorageSettings.isGinEnabled() && !indexStorageSettings.isHashEnabled() && !indexStorageSettings.isBrinEnabled()) {
			return;
		}

		final String tableName = convertIndexNameToTableName(indexName);

		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			PreparedStatement preparedStatement;

			boolean indexCreated = false;
			for (String fieldName : fieldNames) {
				switch (indexStorageSettings.getIndexGenerationSettings().getMode()) {
				case ALL:
					indexCreated = true;
					break;
				case PRESET:
					boolean matchedPresetField = false;
					for (String presetFieldName : indexStorageSettings.getIndexGenerationSettings().getPresetIndexFields()) {
						if (presetFieldName.equalsIgnoreCase(fieldName)) {
							matchedPresetField = true;
							break;
						}
					}
					if (!matchedPresetField) {
						continue;
					}
					tableIndexCreator.createPsqlFieldIndex(connection, tableName, fieldName, indexStorageSettings);
					break;
				case DYNAMIC:
				default:
					//TODO: Implement metric-driven index creation
					break;
				}
			}
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ensureIndexExists(String indexName) throws ElefanaException {
		indexName = indexName.intern();

		final String tableName = convertIndexNameToTableName(indexName);
		if (isKnownTable(tableName)) {
			return;
		}
		tableCreationLock.lock();
		if (isKnownTable(tableName)) {
			tableCreationLock.unlock();
			return;
		}

		final GetIndexTemplateForIndexRequest indexTemplateForIndexRequest = indexTemplateService
				.prepareGetIndexTemplateForIndex(null, indexName);
		final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateForIndexRequest.get();
		final IndexTemplate indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
		boolean timeSeries = false;

		if (indexTemplate != null && indexTemplate.isTimeSeries()) {
			timeSeries = true;
		}

		final String constraintName = PRIMARY_KEY_PREFIX + tableName;

		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			PreparedStatement preparedStatement;

			final StringBuilder createTableQuery = new StringBuilder();
			createTableQuery.append("CREATE TABLE IF NOT EXISTS ");
			createTableQuery.append(tableName);

			if (nodeSettingsService.isUsingCitus()) {
				createTableQuery.append(
						" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, "
								+ "_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb)");
			} else {
				createTableQuery.append(" PARTITION OF ");
				createTableQuery.append(DATA_TABLE);
				createTableQuery.append(" FOR VALUES in ('");
				createTableQuery.append(indexName);
				createTableQuery.append("')");
			}
			final String tablespace = tablespaces[Math.abs(tablespaceIndex.incrementAndGet() % tablespaces.length)];
			if (tablespace != null && !tablespace.isEmpty()) {
				createTableQuery.append(" TABLESPACE ");
				createTableQuery.append(tablespace);
			}

			LOGGER.info(createTableQuery.toString());
			preparedStatement = connection.prepareStatement(createTableQuery.toString());
			preparedStatement.execute();
			preparedStatement.close();

			if(indexTemplate != null && indexTemplate.getStorage() != null) {
				tableIndexCreator.createPsqlTableIndices(connection, tableName, indexTemplate.getStorage());

				if(indexTemplate.getStorage().getIndexGenerationSettings() != null &&
						indexTemplate.getStorage().getIndexGenerationSettings().getMode().equals(IndexGenerationMode.PRESET)) {
					ensureJsonFieldIndexExist(indexName, indexTemplate.getStorage().getIndexGenerationSettings().getPresetIndexFields());
				}
			} else {
				tableIndexCreator.createPsqlTableIndices(connection, tableName, DEFAULT_INDEX_STORAGE_SETTINGS);
			}

			final boolean createPrimaryKey = !nodeSettingsService.isUsingCitus() || (nodeSettingsService.isUsingCitus() && !timeSeries);
			final boolean createIdIndex;
			if(indexTemplate != null && indexTemplate.getStorage() != null) {
				createIdIndex = indexTemplate.getStorage().isIdEnabled();
			} else {
				createIdIndex = true;
			}
			if(createPrimaryKey) {
				if(createIdIndex) {
					final String createPrimaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName
							+ " PRIMARY KEY (_id);";
					LOGGER.info(createPrimaryKeyQuery);
					preparedStatement = connection.prepareStatement(createPrimaryKeyQuery);
					preparedStatement.execute();
					preparedStatement.close();
				}
			} else {
				if(createIdIndex) {
					final String hashIndexName = IndexUtils.HASH_INDEX_PREFIX + tableName + "_id";
					final String createIdIndexQuery = "CREATE INDEX IF NOT EXISTS " + hashIndexName + " ON " + tableName + " USING HASH (_id);";
					LOGGER.info(createIdIndexQuery);
					preparedStatement = connection.prepareStatement(createIdIndexQuery);
					preparedStatement.execute();
					preparedStatement.close();
				}
			}

			final String createPartitionTrackingEntry = "INSERT INTO " + PARTITION_TRACKING_TABLE
					+ " (_index, _partitionTable) VALUES (?, ?) ON CONFLICT DO NOTHING";
			preparedStatement = connection.prepareStatement(createPartitionTrackingEntry);
			preparedStatement.setString(1, indexName);
			preparedStatement.setString(2, tableName);
			preparedStatement.execute();
			preparedStatement.close();

			if (nodeSettingsService.isUsingCitus()) {
				if (timeSeries) {
					try {
						preparedStatement = connection.prepareStatement(
								"SELECT create_distributed_table('" + tableName + "', '_timestamp', 'append');");
						preparedStatement.execute();
						preparedStatement.close();
					} catch (Exception e) {
						if(!e.getMessage().contains("already distributed")) {
							throw e;
						}
					}

					int totalShards;
					if(indexTemplate.getStorage() != null && indexTemplate.getStorage().getIndexTimeBucket() != null) {
						totalShards = indexTemplate.getStorage().getIndexTimeBucket().getIngestTableCapacity();
					} else {
						totalShards = 60;
					}

					preparedStatement = connection.prepareStatement(
							"SELECT create_required_shards('" + tableName + "', " + totalShards + ");");
					preparedStatement.execute();
					preparedStatement.close();
				} else {
					try {
						preparedStatement = connection
								.prepareStatement("SELECT create_distributed_table('" + tableName + "', '_id');");
						preparedStatement.execute();
						preparedStatement.close();
					} catch (Exception e) {
						if(!e.getMessage().contains("already distributed")) {
							throw e;
						}
					}
				}
			}

			connection.close();
			addKnownTable(tableName);
			tableCreationLock.unlock();
		} catch (Exception e) {
			if (connection != null) {
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

	protected List<String> listTables() throws SQLException {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE;
		final List<String> results = new ArrayList<String>(1);

		for (Map<String, Object> row : jdbcTemplate.queryForList(query)) {
			results.add((String) row.get("_partitionTable"));
		}
		return results;
	}

	public String getIndexForPartitionTable(final Connection connection, final String partitionTable) throws SQLException {
		try {
			return partitionTableToIndicesCache.get(partitionTable, new Callable<String>() {
				@Override
				public String call() throws Exception {
					return getIndexForPartitionTableFromDatabase(connection, partitionTable);
				}
			});
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return getIndexForPartitionTableFromDatabase(connection, partitionTable);
	}

	private String getIndexForPartitionTableFromDatabase(Connection connection, String partitionTable) throws SQLException {
		final String query = "SELECT _index FROM " + PARTITION_TRACKING_TABLE + " WHERE _partitionTable = ?";

		final PreparedStatement preparedStatement = connection.prepareStatement(query);
		preparedStatement.setString(1, partitionTable);

		final ResultSet resultSet = preparedStatement.executeQuery();
		String result = null;
		if(resultSet.next()) {
			result = resultSet.getString("_index");
		}
		preparedStatement.close();
		return result;
	}

	public String getIndexForPartitionTable(final String partitionTable) {
		try {
			return partitionTableToIndicesCache.get(partitionTable, new Callable<String>() {
				@Override
				public String call() throws Exception {
					return getIndexForPartitionTableFromDatabase(partitionTable);
				}
			});
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return getIndexForPartitionTableFromDatabase(partitionTable);
	}

	private String getIndexForPartitionTableFromDatabase(String partitionTable) {
		final String query = "SELECT _index FROM " + PARTITION_TRACKING_TABLE + " WHERE _partitionTable = ?";
		for (Map<String, Object> row : jdbcTemplate.queryForList(query, partitionTable)) {
			return (String) row.get("_index");
		}
		return null;
	}

	public String getPartitionTableForIndex(final Connection connection, final String index) throws SQLException {
		try {
			return indicesToPartitionTableCache.get(index, new Callable<String>() {
				@Override
				public String call() throws Exception {
					return getPartitionTableForIndexFromDatabase(connection, index);
				}
			});
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return getPartitionTableForIndexFromDatabase(connection, index);
	}

	private String getPartitionTableForIndexFromDatabase(Connection connection, String index) throws SQLException {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE + " WHERE _index = ?";

		final PreparedStatement preparedStatement = connection.prepareStatement(query);
		preparedStatement.setString(1, index);

		final ResultSet resultSet = preparedStatement.executeQuery();
		String result = null;
		if(resultSet.next()) {
			result = resultSet.getString("_partitionTable");
		}
		preparedStatement.close();
		return result;
	}

	public String getPartitionTableForIndex(final String index) {
		try {
			return indicesToPartitionTableCache.get(index, new Callable<String>() {
				@Override
				public String call() throws Exception {
					return getPartitionTableForIndexFromDatabase(index);
				}
			});
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return getPartitionTableForIndexFromDatabase(index);
	}

	private String getPartitionTableForIndexFromDatabase(String index) {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE + " WHERE _index = ?";
		for (Map<String, Object> row : jdbcTemplate.queryForList(query, index)) {
			return (String) row.get("_partitionTable");
		}
		return null;
	}

	public static String convertIndexNameToTableName(String indexName) {
		try {
			return INDEX_NAME_TO_TABLE_NAME_CACHE.get(indexName);
		} catch (ExecutionException e) {
			return internalConvertIndexNameToTableName(indexName);
		}
	}

	public static String internalConvertIndexNameToTableName(String indexName) {
		indexName = indexName.replace(".", "_f_");
		indexName = indexName.replace("-", "_m_");
		indexName = indexName.replace(":", "_c_");

		if (!Character.isLetter(indexName.charAt(0))) {
			indexName = "_" + indexName;
		}

		indexName.intern();
		return indexName;
	}

	public static String convertTableNameToIndexName(String tableName) {
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

	private boolean isEmptyTablespaceList(String[] tablespaces) {
		if (tablespaces == null) {
			return true;
		}
		for (int i = 0; i < tablespaces.length; i++) {
			if (tablespaces[i] == null) {
				continue;
			}
			if (tablespaces[i].isEmpty()) {
				continue;
			}
			return false;
		}
		return true;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setIndexTemplateService(IndexTemplateService indexTemplateService) {
		this.indexTemplateService = indexTemplateService;
	}
}

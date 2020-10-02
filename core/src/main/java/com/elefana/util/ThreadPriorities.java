/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

public class ThreadPriorities {
	public static final int BULK_INDEX_SERVICE = 9;
	public static final int BULK_INGEST_SERVICE = 8;
	public static final int CITUS_SHARD_MAINTAINER = 5;
	public static final int CLUSTER_SERVICE = 10;
	public static final int DOCUMENT_SERVICE = 4;
	public static final int FIELD_MAPPING_SERVICE = 4;
	public static final int FIELD_STATS_SERVICE = 7;
	public static final int INDEX_TEMPLATE_SERVICE = 6;
	public static final int LOAD_UNLOAD_MANAGER = 5;
	public static final int NODES_SERVICE = 10;
	public static final int NODE_STATS_SERVICE = 4;
	public static final int PSQL_INDEX_CREATOR = 5;
	public static final int SEARCH_SERVICE = 6;
	public static final int TABLE_GARBAGE_COLLECTOR = 2;
}

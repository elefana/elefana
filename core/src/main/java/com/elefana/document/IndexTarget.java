/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.document;

public class IndexTarget {
	private final String index;
	private final String targetTable;
	private final String stagingTable;
	
	public IndexTarget(String index, String targetTable, String stagingTable) {
		super();
		this.index = index;
		this.targetTable = targetTable;
		this.stagingTable = stagingTable;
	}

	public String getIndex() {
		return index;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public String getStagingTable() {
		return stagingTable;
	}
}

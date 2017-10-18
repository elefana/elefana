/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.document;

import java.util.ArrayList;
import java.util.List;

public class MultiGetResponse {
	private final List<GetResponse> docs = new ArrayList<GetResponse>();

	public List<GetResponse> getDocs() {
		return docs;
	}
}

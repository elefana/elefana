/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search;

public interface RequestBodySearchFactory {

	public RequestBodySearch createRequestBodySearch(String requestBody) throws Exception;
}

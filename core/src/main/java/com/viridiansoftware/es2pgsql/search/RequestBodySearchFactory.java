/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search;

import org.springframework.http.HttpEntity;

public interface RequestBodySearchFactory {

	public RequestBodySearch createRequestBodySearch(HttpEntity<String> httpRequest) throws Exception;
}

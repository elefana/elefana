/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search;

import org.springframework.stereotype.Service;

import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.RequestBodySearchFactory;

@Service
public class Es5RequestBodySearchFactory implements RequestBodySearchFactory {

	@Override
	public RequestBodySearch createRequestBodySearch(String requestBody) throws Exception {
		return new Es5RequestBodySearch(requestBody);
	}

}

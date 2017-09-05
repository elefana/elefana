/**
 * Copyright 2017 Viridian Software Ltd.
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
 */
package com.viridiansoftware.es2pg.document;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.viridiansoftware.es2pg.search.SearchService;

@RestController
public class DocumentApiController {
	@Autowired
	private DocumentService documentService;
	@Autowired
	private SearchService searchService;
	
	@RequestMapping(path = "/{indexPattern}", method = RequestMethod.POST)
	public Object index(@PathVariable String indexPattern, HttpEntity<String> request) throws Exception {
		if(indexPattern.toLowerCase().equals("_search")) {
			return searchService.search(request.getBody());
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{types}", method = RequestMethod.POST)
	public Object index(@PathVariable String indexPattern, @PathVariable String types, HttpEntity<String> request) throws Exception {
		if(types.toLowerCase().equals("_search")) {
			return searchService.search(indexPattern, request.getBody());
		}
		return index(indexPattern, types, UUID.randomUUID().toString(), request);
	}

	@RequestMapping(path = "/{indexPattern}/{types}/{id}", method = RequestMethod.POST)
	public Object index(@PathVariable String indexPattern, @PathVariable String types, @PathVariable String id,
			HttpEntity<String> request) throws Exception {
		if(id.toLowerCase().equals("_search")) {
			return searchService.search(indexPattern, types, request.getBody());
		}
		String document = request.getBody();
		return documentService.index(indexPattern, types, id, document);
	}
}

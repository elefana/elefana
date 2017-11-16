/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.indices;

import java.util.HashMap;
import java.util.Map;

public class IndexTemplate {
	private final Map<String, Object> settings = new HashMap<String, Object>();
	
	private String template;
	private String tableTemplate;
	private Map<String, Object> mappings;

	public IndexTemplate() {
		super();
		settings.put("number_of_shards", 1);
	}
	
	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	public String getTableTemplate() {
		return tableTemplate;
	}

	public void setTableTemplate(String tableTemplate) {
		this.tableTemplate = tableTemplate;
	}

	public Map<String, Object> getSettings() {
		return settings;
	}

	public Map<String, Object> getMappings() {
		return mappings;
	}

	public void setMappings(Map<String, Object> mappings) {
		this.mappings = mappings;
	}

	@Override
	public String toString() {
		return "IndexTemplate [settings=" + settings + ", template=" + template + ", tableTemplate=" + tableTemplate
				+ ", mappings=" + mappings + "]";
	}
}

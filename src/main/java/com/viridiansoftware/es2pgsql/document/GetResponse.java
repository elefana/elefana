/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.document;

import java.util.Map;

public class GetResponse {
	private String _index;
	private String _type;
	private String _id;
	private int version = 1;
	private Map _source;

	public String get_index() {
		return _index;
	}

	public void set_index(String _index) {
		this._index = _index;
	}

	public String get_type() {
		return _type;
	}

	public void set_type(String _type) {
		this._type = _type;
	}

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public Map get_source() {
		return _source;
	}

	public void set_source(Map _source) {
		this._source = _source;
	}
}

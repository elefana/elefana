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
package com.elefana.api.document;

public class BulkItemResponse {
	public static final String STATUS_CREATED = "created";
	public static final String STATUS_UPDATED = "updated";
	public static final String STATUS_FAILED = "failed";
	
	private final int itemId;
	private final BulkOpType opType;
	
	private String index;
	private String type;
	private String id;
	private String result;
	private int version = -1;
	
	public BulkItemResponse(int itemId, BulkOpType opType) {
		super();
		this.itemId = itemId;
		this.opType = opType;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getItemId() {
		return itemId;
	}

	public BulkOpType getOpType() {
		return opType;
	}
	
	public boolean isFailed() {
		return !result.equalsIgnoreCase(STATUS_CREATED) && !result.equalsIgnoreCase(STATUS_UPDATED);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((index == null) ? 0 : index.hashCode());
		result = prime * result + itemId;
		result = prime * result + ((opType == null) ? 0 : opType.hashCode());
		result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + version;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BulkItemResponse other = (BulkItemResponse) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (index == null) {
			if (other.index != null)
				return false;
		} else if (!index.equals(other.index))
			return false;
		if (itemId != other.itemId)
			return false;
		if (opType != other.opType)
			return false;
		if (result == null) {
			if (other.result != null)
				return false;
		} else if (!result.equals(other.result))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (version != other.version)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BulkItemResponse [itemId=" + itemId + ", opType=" + opType + ", index=" + index + ", type=" + type
				+ ", id=" + id + ", result=" + result + ", version=" + version + "]";
	}
}

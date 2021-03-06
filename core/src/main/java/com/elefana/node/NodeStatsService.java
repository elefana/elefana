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
package com.elefana.node;

import com.elefana.api.node.NodeStats;

public interface NodeStatsService {

	public NodeStats getNodeStats(String... infoFields);

	public NodeStats getNodeStats();

	public String getNodeId();
	
	public String getNodeName();

	public OsStats getOsStats();

	public JvmStats getJvmStats();

	public ProcessStats getProcessStats();

	public boolean isMasterNode();

	public boolean isDataNode();

	public boolean isIngestNode();
}

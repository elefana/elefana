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

import com.elefana.api.node.NodesStatsRequest;

public interface NodesService {

    public NodesStatsRequest prepareAllNodesStats();

    public NodesStatsRequest prepareNodesStats(String [] filteredNodes);
	
	public NodesStatsRequest prepareNodesStats(String [] filteredNodes, String[] infoFields);

	public NodesStatsRequest prepareAllNodesStats(String[] infoFields);

	public NodesStatsRequest prepareLocalNodeStats();
	
	public NodesStatsRequest prepareLocalNodeStats(String[] infoFields);
}

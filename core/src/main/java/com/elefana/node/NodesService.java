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

import com.elefana.api.node.NodesInfoRequest;

public interface NodesService {

    public NodesInfoRequest prepareAllNodesInfo();

    public NodesInfoRequest prepareNodesInfo(String [] filteredNodes);
	
	public NodesInfoRequest prepareNodesInfo(String [] filteredNodes, String[] infoFields);

	public NodesInfoRequest prepareAllNodesInfo(String[] infoFields);

	public NodesInfoRequest prepareLocalNodeInfo();
	
	public NodesInfoRequest prepareLocalNodeInfo(String[] infoFields);
}

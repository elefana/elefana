/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

package com.elefana.indices.fieldstats.job;

import com.elefana.indices.fieldstats.LoadUnloadManager;
import com.elefana.indices.fieldstats.state.State;

public class CoreFieldStatsRemoveIndexJob extends FieldStatsJob {

    public CoreFieldStatsRemoveIndexJob(State state, LoadUnloadManager loadUnloadManager, String indexName) {
        super(state, loadUnloadManager, indexName);
    }

    @Override
    public void run() {
       loadUnloadManager.deleteIndex(indexName);
    }
}

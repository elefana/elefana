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

package com.elefana.indices.fieldstats.state.field.types;

import com.elefana.indices.fieldstats.state.field.FieldStatsImpl;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class BooleanFieldStats extends ComparableFieldStats<Boolean> {

    @Override
    protected long getDocFrequencyAddend(Boolean value) {
        return 1;
    }

    @Override
    protected long getTotalTermFrequencyAddend(Boolean value) {
        return -1;
    }

    @Override
    protected FieldStatsImpl<Boolean> instance() {
        return new BooleanFieldStats();
    }
}

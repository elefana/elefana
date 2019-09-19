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
import com.jsoniter.annotation.JsonIgnore;
import com.jsoniter.annotation.JsonProperty;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.LongAccumulator;

@ThreadSafe
public class LongFieldStats extends FieldStatsImpl<Long> {
    @JsonIgnore
    private LongAccumulator minValue = new LongAccumulator(Long::min, Long.MAX_VALUE);
    @JsonIgnore
    private LongAccumulator maxValue = new LongAccumulator(Long::max, Long.MIN_VALUE);

    @Override
    protected long getDocFrequencyAddend(Long value) {
        return 1;
    }

    @Override
    protected long getTotalTermFrequencyAddend(Long value) {
        return -1;
    }

    @Override
    protected void updateMin(Long value) {
        minValue.accumulate(value);
    }

    @Override
    protected void updateMax(Long value) {
        maxValue.accumulate(value);
    }

    @Override
    @JsonProperty("i")
    public Long getMinimumValue() {
        return minValue.get();
    }

    @Override
    @JsonProperty("a")
    public Long getMaximumValue() {
        return maxValue.get();
    }

    @Override
    protected FieldStatsImpl<Long> instance() {
        return new LongFieldStats();
    }
}

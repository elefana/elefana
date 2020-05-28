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
import java.util.concurrent.atomic.DoubleAccumulator;

@ThreadSafe
public class DoubleFieldStats extends FieldStatsImpl<Double> {

    private DoubleAccumulator minValue = new DoubleAccumulator(Double::min, Double.MAX_VALUE);
    private DoubleAccumulator maxValue = new DoubleAccumulator(Double::max, Double.MIN_VALUE);

    @Override
    protected long getDocFrequencyAddend(Double value) {
        return 0;
    }

    @Override
    protected long getTotalTermFrequencyAddend(Double value) {
        return 0;
    }

    @Override
    public void updateMin(Double value) {
        minValue.accumulate(value);
    }

    @Override
    public void updateMax(Double value) {
        maxValue.accumulate(value);
    }

    @Override
    public Class<Double> getFieldClass() {
        return Double.class;
    }

    @Override
    public Double getMinimumValue() {
        return minValue.get();
    }

    @Override
    public Double getMaximumValue() {
        return maxValue.get();
    }

    @Override
    protected FieldStatsImpl<Double> instance() {
        return new DoubleFieldStats();
    }
}

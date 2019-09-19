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

import java.util.concurrent.atomic.AtomicReference;

public abstract class ComparableFieldStats<T extends Comparable<T>> extends FieldStatsImpl<T> {

    @JsonIgnore
    private AtomicReference<T> minValue = new AtomicReference<>();
    @JsonIgnore
    private AtomicReference<T> maxValue = new AtomicReference<>();

    @Override
    protected void updateMin(T value) {
        minValue.accumulateAndGet(value, this::nullSafeMin);
    }

    @Override
    protected void updateMax(T value) {
        maxValue.accumulateAndGet(value, this::nullSafeMax);
    }

    private T nullSafeMin(T a, T b) {
        if(a == null && b == null)
            return null;
        if(b == null)
            return a;
        if(a == null)
            return b;

        return min(a, b);
    }

    private T nullSafeMax(T a, T b) {
        if(a == null && b == null)
            return null;
        if(b == null)
            return a;
        if(a == null)
            return b;

        return min(a, b);
    }

    private T min(T a, T b) {
        int compare = a.compareTo(b);
        if(compare < 0) {
            return a;
        } else if (compare > 0) {
            return b;
        } else {
            return a;
        }
    }

    private T max(T a, T b) {
        int compare = a.compareTo(b);
        if(compare > 0) {
            return a;
        } else if (compare < 0) {
            return b;
        } else {
            return a;
        }
    }

    @Override
    @JsonProperty("i")
    public T getMinimumValue() {
        return minValue.get();
    }

    @Override
    @JsonProperty("a")
    public T getMaximumValue() {
        return maxValue.get();
    }
}

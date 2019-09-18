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

package com.elefana.indices.fieldstats.state.field;

import com.elefana.indices.fieldstats.state.index.Index;
import com.jsoniter.annotation.JsonIgnore;
import com.jsoniter.annotation.JsonProperty;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public abstract class FieldStatsImpl<T> implements FieldStats<T> {
    @JsonIgnore
    private LongAdder docCount = new LongAdder();
    @JsonIgnore
    private LongAccumulator sumDocFrequency = new LongAccumulator(this::mergePossiblyUnsupportedValues, 0);
    @JsonIgnore
    private LongAccumulator sumTotalTermFrequency = new LongAccumulator(this::mergePossiblyUnsupportedValues, 0);

    @Override
    public void updateFieldIsInDocument() {
        docCount.increment();
    }

    @Override
    public void updateSingeOccurrence(T value) {
        long docFrequency = getDocFrequencyAddend(value);
        sumDocFrequency.accumulate(docFrequency);

        long totalTermFrequency = getTotalTermFrequencyAddend(value);
        sumTotalTermFrequency.accumulate(totalTermFrequency);

        updateMin(value);
        updateMax(value);
    }

    protected abstract long getDocFrequencyAddend(T value);
    protected abstract long getTotalTermFrequencyAddend(T value);
    protected abstract void updateMin(T value);
    protected abstract void updateMax(T value);

    @Override
    @JsonProperty("minValue")
    public abstract T getMinimumValue();
    @Override
    @JsonProperty("maxValue")
    public abstract T getMaximumValue();

    @Override
    @JsonProperty("docCount")
    public long getDocumentCount() {
        return docCount.sum();
    }

    @Override
    @JsonProperty("sumDoc")
    public long getSumDocumentFrequency() {
        return sumDocFrequency.get();
    }

    @Override
    @JsonProperty("sumTotal")
    public long getSumTotalTermFrequency() {
        return sumTotalTermFrequency.get();
    }

    @Override
    public double getDensity(Index index) {
        double percent = (double)getDocumentCount() / index.getMaxDocuments();
        return percent * 100d;
    }

    protected abstract FieldStatsImpl<T> instance();

    @Override
    public FieldStats<T> merge(FieldStats<T> other) {
        FieldStatsImpl<T> ret = instance();

        ret.docCount.add(this.getDocumentCount() + other.getDocumentCount());

        ret.sumDocFrequency.accumulate(this.getSumDocumentFrequency());
        ret.sumDocFrequency.accumulate(other.getSumDocumentFrequency());

        ret.sumTotalTermFrequency.accumulate(this.getSumTotalTermFrequency());
        ret.sumTotalTermFrequency.accumulate(other.getSumTotalTermFrequency());

        ret.updateMax(this.getMaximumValue());
        ret.updateMax(other.getMaximumValue());

        ret.updateMin(this.getMinimumValue());
        ret.updateMin(other.getMinimumValue());

        return ret;
    }

    @Override
    public FieldStats<T> mergeUnsafe(FieldStats other) {
        FieldStatsImpl<T> ret = instance();

        ret.docCount.add(this.getDocumentCount() + other.getDocumentCount());

        ret.sumDocFrequency.accumulate(this.getSumDocumentFrequency());
        ret.sumDocFrequency.accumulate(other.getSumDocumentFrequency());

        ret.sumTotalTermFrequency.accumulate(this.getSumTotalTermFrequency());
        ret.sumTotalTermFrequency.accumulate(other.getSumTotalTermFrequency());

        ret.updateMax(this.getMaximumValue());
        ret.updateMax((T)other.getMaximumValue());

        ret.updateMin(this.getMinimumValue());
        ret.updateMin((T)other.getMinimumValue());

        return ret;
    }

    private long mergePossiblyUnsupportedValues(long a, long b) {
        if (a == -1 || b == -1)
            return -1;
        return a + b;
    }
}

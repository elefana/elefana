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

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public abstract class FieldStatsImpl<T> implements FieldStats<T> {
    private LongAdder docCount = new LongAdder();
    private LongAccumulator sumDocFrequency = new LongAccumulator(this::mergePossiblyUnsupportedValues, 0);
    private LongAccumulator sumTotalTermFrequency = new LongAccumulator(this::mergePossiblyUnsupportedValues, 0);

    @Override
    public void updateSubmitFieldIsInDocument() {
        docCount.increment();
    }

    @Override
    public void updateSubmitSingeOccurrence(T value) {
        long docFrequency = getDocFrequencyAddend(value);
        sumDocFrequency.accumulate(docFrequency);

        long totalTermFrequency = getTotalTermFrequencyAddend(value);
        sumTotalTermFrequency.accumulate(totalTermFrequency);

        updateMin(value);
        updateMax(value);
    }

    @Override
    public void updateDeleteFieldIsInDocument() {
        docCount.decrement();
    }

    @Override
    public void updateDeleteSingleOccurrence(T value) {
        long docFrequency = getDocFrequencyAddend(value);
        sumDocFrequency.accumulate(-docFrequency);

        long totalTermFrequency = getTotalTermFrequencyAddend(value);
        sumTotalTermFrequency.accumulate(-totalTermFrequency);
    }

    protected abstract long getDocFrequencyAddend(T value);
    protected abstract long getTotalTermFrequencyAddend(T value);
    protected abstract void updateMin(T value);
    protected abstract void updateMax(T value);

    @Override
    public abstract T getMinimumValue();
    @Override
    public abstract T getMaximumValue();

    @Override
    public long getDocumentCount() {
        return docCount.sum();
    }

    @Override
    public long getSumDocumentFrequency() {
        return sumDocFrequency.get();
    }

    @Override
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
    public void mergeAndModifySelf(FieldStats<T> other) {
        this.docCount.add(other.getDocumentCount());
        this.sumDocFrequency.accumulate(other.getSumDocumentFrequency());
        this.sumTotalTermFrequency.accumulate(other.getSumTotalTermFrequency());
        this.updateMax(other.getMaximumValue());
        this.updateMin(other.getMinimumValue());
    }

    private long mergePossiblyUnsupportedValues(long a, long b) {
        if (a == -1)
            return -1;
        return a + b;
    }
}

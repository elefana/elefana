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

public class FieldComponent<T> {
    public T minValue;
    public T maxValue;
    public long docCount;
    public long sumDocFreq;
    public long sumTotalTermFreq;

    public FieldComponent(T minValue, T maxValue, long docCount, long sumDocFreq, long sumTotalTermFreq, Class<T> type) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.docCount = docCount;
        this.sumDocFreq = sumDocFreq;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.type = type;
    }

    public Class<T> type;

    public FieldStats<T> construct() {
        return Fields.getFieldStats(type).merge(new FieldStats<T>() {
            @Override
            public T getMinimumValue() {
                return minValue;
            }

            @Override
            public T getMaximumValue() {
                return maxValue;
            }

            @Override
            public long getDocumentCount() {
                return docCount;
            }

            @Override
            public long getSumDocumentFrequency() {
                return sumDocFreq;
            }

            @Override
            public long getSumTotalTermFrequency() {
                return sumTotalTermFreq;
            }

            @Override
            public void updateSingeOccurrence(T value) {

            }

            @Override
            public FieldStats<T> merge(FieldStats<T> other) {
                return null;
            }

            @Override
            public double getDensity(Index index) {
                return 0;
            }

            @Override
            public void updateFieldIsInDocument() {

            }

            @Override
            public void mergeAndModifySelf(FieldStats<T> other) {

            }

            @Override
            public FieldComponent<T> getFieldComponent(Class<?> type) {
                return null;
            }
        });
    }

    public Field<T> constructField() {
        return new FieldImpl<T>(type);
    }

    @Override
    public String toString() {
        return "FieldComponent{" +
                "minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", docCount=" + docCount +
                ", sumDocFreq=" + sumDocFreq +
                ", sumTotalTermFreq=" + sumTotalTermFreq +
                ", type=" + type +
                '}';
    }
}

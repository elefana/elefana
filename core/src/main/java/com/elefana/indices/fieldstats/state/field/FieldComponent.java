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

import com.elefana.indices.fieldstats.job.ElefanaUnsupportedFieldType;
import com.elefana.indices.fieldstats.state.field.types.*;
import com.elefana.indices.fieldstats.state.index.Index;

import java.time.Instant;
import java.util.Date;

public class FieldComponent {
    public String minValue;
    public String maxValue;
    public long docCount;
    public long sumDocFreq;
    public long sumTotalTermFreq;

    private abstract class StaticFieldStats<T> implements FieldStats<T> {

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
        public void updateMin(T value) {

        }

        @Override
        public void updateMax(T value) {

        }

        @Override
        public void addDocumentCount(long amount) {

        }

        @Override
        public void addSumDocumentFrequency(long amount) {

        }

        @Override
        public void addSumTotalTermFrequency(long amount) {

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
    }

    public FieldComponent(String minValue, String maxValue, long docCount, long sumDocFreq, long sumTotalTermFreq, Class type) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.docCount = docCount;
        this.sumDocFreq = sumDocFreq;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.type = type;
    }

    public Class type;

    FieldStats construct() {
        return getFieldStats(type).merge(getStaticFieldStats());
    }

    private FieldStats getStaticFieldStats() {
        if (type.equals(Long.class)) {
            return new StaticFieldStats<Long>() {
                @Override
                public Long getMinimumValue() {
                    return Long.parseLong(minValue);
                }

                @Override
                public Long getMaximumValue() {
                    return Long.parseLong(maxValue);
                }
            };
        }
        if (type.equals(Double.class)) {
            return new StaticFieldStats<Double>() {
                @Override
                public Double getMinimumValue() {
                    return Double.parseDouble(minValue);
                }

                @Override
                public Double getMaximumValue() {
                    return Double.parseDouble(maxValue);
                }
            };
        }
        if (type.equals(Date.class)) {
            return new StaticFieldStats<Date>() {
                @Override
                public Date getMinimumValue() {
                    return Date.from(Instant.parse(minValue));
                }

                @Override
                public Date getMaximumValue() {
                    return Date.from(Instant.parse(maxValue));
                }
            };
        }
        if (type.equals(String.class)) {
            return new StaticFieldStats<String>() {
                @Override
                public String getMinimumValue() {
                    return minValue;
                }

                @Override
                public String getMaximumValue() {
                    return maxValue;
                }
            };
        }
        if (type.equals(Boolean.class)) {
            return new StaticFieldStats<Boolean>() {
                @Override
                public Boolean getMinimumValue() {
                    return Boolean.parseBoolean(minValue);
                }

                @Override
                public Boolean getMaximumValue() {
                    return Boolean.parseBoolean(maxValue);
                }
            };
        }
        throw new ElefanaUnsupportedFieldType(type);
    }

    public static FieldStats getFieldStats(Class tClass) throws ElefanaUnsupportedFieldType {
        if (tClass.equals(Long.class)) { return new LongFieldStats(); }
        if (tClass.equals(Double.class)) { return new DoubleFieldStats(); }
        if (tClass.equals(Date.class)) { return new DateFieldStats(); }
        if (tClass.equals(String.class)) { return new StringFieldStats(); }
        if (tClass.equals(Boolean.class)) { return new BooleanFieldStats(); }
        throw new ElefanaUnsupportedFieldType(tClass);
    }

    public Field constructField() {
        return new FieldImpl(type);
    }

    private static <T> String getStringRepresentation(T value, Class type) {
        if (type.equals(Long.class)) { return Long.toString((Long)value); }
        if (type.equals(Double.class)) { return Double.toString((Double)value); }
        if (type.equals(Date.class)) { return ((Date)value).toInstant().toString(); }
        if (type.equals(String.class)) { return (String)value; }
        if (type.equals(Boolean.class)) { return Boolean.toString((Boolean)value); }
        throw new ElefanaUnsupportedFieldType(type);
    }

    public static FieldComponent getFieldComponent(FieldStats fieldStats, Class type) {
        String maxValue = getStringRepresentation(fieldStats.getMaximumValue(), type);
        String minValue = getStringRepresentation(fieldStats.getMinimumValue(), type);

        return new FieldComponent(
                minValue,
                maxValue,
                fieldStats.getDocumentCount(),
                fieldStats.getSumDocumentFrequency(),
                fieldStats.getSumTotalTermFrequency(),
                type
        );
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

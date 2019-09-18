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
import com.elefana.indices.fieldstats.state.index.IndexImpl;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.spi.Decoder;
import com.jsoniter.spi.JsoniterSpi;
import com.jsoniter.spi.TypeLiteral;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class FieldsImpl implements Fields {

    private static FieldsImpl fieldsImpl = new FieldsImpl();

    public static Fields getInstance() {
        return fieldsImpl;
    }

    private FieldsImpl() {}

    @Override
    public <F> FieldStats<F> getFieldStats(Class<F> tClass) throws ElefanaUnsupportedFieldType {
        if (tClass.equals(Long.class)) { return (FieldStats<F>) new LongFieldStats(); }
        if (tClass.equals(Double.class)) { return (FieldStats<F>) new DoubleFieldStats(); }
        if (tClass.equals(Date.class)) { return (FieldStats<F>) new DateFieldStats(); }
        if (tClass.equals(String.class)) { return (FieldStats<F>) new StringFieldStats(); }
        if (tClass.equals(Boolean.class)) { return (FieldStats<F>) new BooleanFieldStats(); }
        throw new ElefanaUnsupportedFieldType(tClass);
    }

    @Override
    public Field deserializeField(Class tClass, Any json) throws ElefanaUnsupportedFieldType {
        if (tClass.equals(Long.class)) { return getFieldInstance(Long.class, json); }
        if (tClass.equals(Double.class)) { return getFieldInstance(Double.class, json); }
        if (tClass.equals(Date.class)) { return getFieldInstance(Date.class, json); }
        if (tClass.equals(String.class)) { return getFieldInstance(String.class, json); }
        if (tClass.equals(Boolean.class)) { return getFieldInstance(Boolean.class, json); }
        throw new ElefanaUnsupportedFieldType(tClass);
    }

    @Override
    public void registerJsoniterConfig() {
        JsoniterSpi.registerTypeImplementation(Index.class, IndexImpl.class);
        JsoniterSpi.registerTypeDecoder(new TypeLiteral<Field<?>>(){}, new Decoder() {
            @Override
            public Object decode(JsonIterator iter) throws IOException {
                Any field = iter.readAny();
                String fieldType = field.get("type").toString();
                try {
                    Class tClass = Class.forName(fieldType);
                    return FieldsImpl.getInstance().deserializeField(tClass, field);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    public <T> Field<T> getFieldInstance(Class<T> tClass, Any json){
        Map<String, Any> fieldStats = json.get("fieldStats").asMap();
        Map<String, FieldStats<T>> fieldStatsAcc = new HashMap<>();

        fieldStats.forEach((k,v) -> {
            FieldStats<T> fs = getFieldStats(tClass).merge(new FieldStats<T>() {
                @Override
                public T getMinimumValue() {
                    return v.get("minimumValue").as(tClass);
                }

                @Override
                public T getMaximumValue() {
                    return v.get("maximumValue").as(tClass);
                }

                @Override
                public long getDocumentCount() {
                    return v.get("docCount").toLong();
                }

                @Override
                public long getSumDocumentFrequency() {
                    return v.get("sumDoc").toLong();
                }

                @Override
                public long getSumTotalTermFrequency() {
                    return v.get("sumTotal").toLong();
                }

                @Override
                public void updateSingeOccurrence(T value) { }

                @Override
                public FieldStats<T> merge(FieldStats<T> other) {
                    return null;
                }

                @Override
                public FieldStats<T> mergeUnsafe(FieldStats other) {
                    return null;
                }

                @Override
                public double getDensity(Index index) {
                    return 0;
                }

                @Override
                public void updateFieldIsInDocument() { }
            });
            fieldStatsAcc.put(k, fs);
        });

        return new FieldImpl<T>(tClass, fieldStatsAcc);
    }
}

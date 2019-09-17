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

import java.util.Date;



public class FieldsImpl implements Fields {

    private static FieldsImpl fieldsImpl = new FieldsImpl();

    public static FieldsImpl getInstance() {
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
}

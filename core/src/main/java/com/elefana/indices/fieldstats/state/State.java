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

package com.elefana.indices.fieldstats.state;

import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.Field;
import com.elefana.indices.fieldstats.state.index.Index;
import com.elefana.indices.fieldstats.state.index.IndexComponent;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.List;

@ThreadSafe
public interface State {
    // Whole-State Operations
    public void deleteIndex(String name);

    public void load(IndexComponent indexComponent) throws ElefanaWrongFieldStatsTypeException;
    public IndexComponent unload(String indexName);

    public void stopModificationsOfIndex(String index);
    public void resumeModificationsOfIndex(String index);

    public void startIndexModification(String index);
    public void finishIndexModification(String index);

    // Indices
    public Index getIndex(String indexName);
    public Index getIndex(Collection<String> indices);

    // Fields
    public <T> Field<T> getFieldTypeChecked(String fieldName, Class<T> tClass) throws ElefanaWrongFieldStatsTypeException;
    public Field<?> getField(String fieldName);
    public List<String> compileIndexPattern(String indexPattern);
}

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
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.indices.fieldstats.state.index.Index;
import com.elefana.indices.fieldstats.state.index.IndexComponent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.List;

@ThreadSafe
public interface State {
    // Whole-State Operations
    void deleteIndex(String name);

    void load(IndexComponent indexComponent) throws ElefanaWrongFieldStatsTypeException;
    IndexComponent unload(String indexName);

    void stopModificationsOfIndex(String index);
    void resumeModificationsOfIndex(String index);

    void startIndexModification(String index);
    void finishIndexModification(String index);

    // Indices
    Index getIndex(String indexName);
    Index getIndex(Collection<String> indices);
    List<String> compileIndexPattern(String indexPattern);

    //Fields
    @Nonnull
    <T> FieldStats<T> getFieldStatsTypeChecked(String fieldName, Class<T> tClass, String indices) throws ElefanaWrongFieldStatsTypeException;

    @Nullable
    FieldStats getFieldStats(String fieldName, Collection<String> indices);
    @Nullable
    FieldStats getFieldStats(String fieldName, String indices);

}

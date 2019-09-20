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

import java.util.Collection;

public interface Field<T> {
    public FieldStats<T> getIndexFieldStats(String indexName);
    public FieldStats<T> getIndexFieldStats(Collection<String> indices);

    boolean hasIndexFieldStats(String name);

    public FieldStats<T> getFieldStats();

    public void deleteIndexFieldStats(String indexName);

    public Class<T> getFieldType();

    public void load(String indexName, FieldComponent<T> fieldComponent);
}

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

package com.elefana.indices.fieldstats.state.index;

import com.elefana.indices.fieldstats.state.field.FieldComponent;

import java.util.HashMap;
import java.util.Map;

public class IndexComponent {
    public String name;
    public Map<String, FieldComponent> fields = new HashMap<>();
    public long maxDocs;

    public IndexComponent(String name, long maxDocs) {
        this.name = name;
        this.maxDocs = maxDocs;
    }

    public Index construct() {
        return new IndexImpl(maxDocs);
    }

    @Override
    public String toString() {
        return "Name=" + name + ", MaxDocs=" + maxDocs + "\n" + fields.toString();
    }
}

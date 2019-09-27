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

public class ElefanaWrongFieldStatsTypeException extends Exception {
    private boolean tryParsing = false;
    public ElefanaWrongFieldStatsTypeException(String fieldName, Class type, Class expected) {
        super("A scanned document contains the field " + fieldName + " with the type " + type.getTypeName() + " but the current field stats state has the type " + expected.getTypeName() + " registered");
    }
    public ElefanaWrongFieldStatsTypeException(String fieldName, Class type, Class expected, boolean tryParsing) {
        this(fieldName, type, expected);
        this.tryParsing = tryParsing;
    }

    public boolean isTryParsing() {
        return tryParsing;
    }
}

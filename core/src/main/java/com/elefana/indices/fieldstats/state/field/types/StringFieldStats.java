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

package com.elefana.indices.fieldstats.state.field.types;

import com.elefana.indices.fieldstats.state.field.FieldStatsImpl;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@ThreadSafe
public class StringFieldStats extends ComparableFieldStats<String> {

    @Override
    public void updateMin(String value) {
        if(value != null){
            List<String> words = Arrays.stream(value.split("\\W+")).map(String::toLowerCase).collect(Collectors.toList());
            if(words.size() > 0) {
                super.updateMin(Collections.min(words));
            } else {
                super.updateMin(value);
            }
        } else {
            super.updateMin(value);
        }
    }

    @Override
    public void updateMax(String value) {
        if(value != null) {
            List<String> words = Arrays.stream(value.split("\\W+")).map(String::toLowerCase).collect(Collectors.toList());
            if(words.size() > 0) {
                super.updateMax(Collections.max(words));
            } else {
                super.updateMax(value);
            }
        } else {
            super.updateMax(value);
        }
    }

    @Override
    protected long getDocFrequencyAddend(String value) {
        int count = 0;
        int pos = 0;
        int end;
        while((end = value.indexOf(' ', pos)) >= 0) {
            count++;
            pos = end + 1;
        }
        return count + 1;
    }

    @Override
    protected long getTotalTermFrequencyAddend(String value) {
        return -1;
    }

    @Override
    protected FieldStatsImpl<String> instance() {
        return new StringFieldStats();
    }

}

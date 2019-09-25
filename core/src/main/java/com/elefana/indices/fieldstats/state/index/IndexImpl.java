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

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class IndexImpl implements Index {
    private LongAdder maxDocs = new LongAdder();
    private ReadWriteLock indexLock = new ReentrantReadWriteLock();

    public IndexImpl(){}

    IndexImpl(long maxDocs) {
        this.maxDocs.add(maxDocs);
    }

    @Override
    public Lock getCountingLock() {
        return indexLock.readLock();
    }

    @Override
    public Lock getStopCountingLock() {
        return indexLock.writeLock();
    }

    @Override
    public long getMaxDocuments() {
        return maxDocs.sum();
    }

    public void setMaxDocuments(long maxDocs) {
        this.maxDocs.reset();
        this.maxDocs.add(maxDocs);
    }

    @Override
    public void incrementMaxDocuments() {
        maxDocs.increment();
    }

    @Override
    public void decrementMaxDocuments() {
        maxDocs.decrement();
    }

    @Override
    public Index merge(Index other) {
        IndexImpl ret = new IndexImpl();
        ret.maxDocs.add(other.getMaxDocuments());
        ret.maxDocs.add(this.getMaxDocuments());
        return ret;
    }

    @Override
    public void mergeAndModifySelf(Index other){
        this.maxDocs.add(other.getMaxDocuments());
    }

    @Override
    public void delete(){
        this.maxDocs.reset();
    }

}

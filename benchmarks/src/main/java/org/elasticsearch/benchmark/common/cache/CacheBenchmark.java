/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.common.cache;

import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Random;

@State(Scope.Group)
public class CacheBenchmark {

    private static final int SIZE = 2 << 14;
    private static final int MASK = SIZE - 1;
    private static final int ITEMS = SIZE / 3;

    @Param({
        "Cache",
        "Cache2"
    })
    CacheFactory factory;

    Adapter<Integer, Boolean> cache;
    Integer[] values;

    @State(Scope.Thread)
    public static class ThreadState {

        static final Random random = new Random();
        int index = random.nextInt();
    }

    @Setup
    public void setup() {
        values = new Integer[SIZE];
        cache = factory.create(2 * SIZE);

        for (int i = 0; i < 2 * SIZE; i++) {
            cache.put(i, Boolean.TRUE);
        }
        cache.clear();
        cache.refresh();

        NumberGenerator generator = new ScrambledZipfianGenerator(ITEMS);
        for (int i = 0; i < SIZE; i++) {
            values[i] = generator.nextValue().intValue();
            cache.put(values[i], Boolean.TRUE);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        cache.refresh();
    }

    @Benchmark
    @Group("read_only") @GroupThreads(8)
    public Boolean readOnly(ThreadState threadState) {
        return cache.get(values[threadState.index++ & MASK]);
    }

    @Benchmark @Group("write_only") @GroupThreads(8)
    public Boolean writeOnly(ThreadState threadState) {
        return cache.put(values[threadState.index++ & MASK], Boolean.TRUE);

    }

    @Benchmark @Group("readwrite") @GroupThreads(6)
    public Boolean readwrite_get(ThreadState threadState) {
        return cache.get(values[threadState.index++ & MASK]);
    }

    @Benchmark @Group("readwrite") @GroupThreads(2)
    public Boolean readwrite_put(ThreadState threadState) {
        return cache.put(values[threadState.index++ & MASK], Boolean.TRUE);
    }

}

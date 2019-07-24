/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.redisson;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RMap;

import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

public class StringStringSyncTest extends RedissonTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int cacheSize = 0;

    private String[] values;
    private RMap<String, String> map;

    @Setup
    public void setup() {
        values = generateAsciiStrings(valueCount, minValueLength, maxValueLength);
        LocalCachedMapOptions options = LocalCachedMapOptions.defaults()
                .cacheSize(cacheSize)
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU);
        if (cacheSize > 0) {
            map = client.getLocalCachedMap("map", options);
            System.out.println("Near cache with size " + cacheSize + " map size " + this.map.size());
        } else {
            map = client.getMap("map");
            System.out.println("Regular map no cache" + " map size " + this.map.size());
        }
    }

    @Prepare(global = true)
    public void loadInitialData() {
        RMap<String, String> map = client.getMap("map");
        map.clear();
        map.delete();
        System.out.println(" Global setup map size after delete : " + map.size());

        map = client.getMap("map");
        Random random = new Random();
        for (int k = 0; k < keyDomain; k++) {
            int r = random.nextInt(valueCount);
            map.fastPut(Long.toString(k), values[r]);
        }

        System.out.println(" Global setup map size after loading : " + map.size());
    }

    @TimeStep(prob = -1)
    public String get(ThreadState state) {
        return map.get(state.randomKey());
    }

    @TimeStep(prob = -1)
    public void getTest(ThreadState state) {
        String key = state.randomKey();
        String s = map.get(key);
        if (s == null) {
            throw new RuntimeException("map returns null for key : " + key);
        }
    }

    @TimeStep(prob = -1)
    public void fastPut(ThreadState state) {
        map.fastPut(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 1)
    public String put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    public class ThreadState extends BaseThreadState {
        private String randomKey() {
            return Long.toString(randomLong(keyDomain));
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown(global = true)
    public void clear() {
        map.clear();
        map.delete();
    }
}

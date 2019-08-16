package com.hazelcast.simulator.redisson;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RMap;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class ByteByteRepeatedSingleMapTest extends RedissonTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int valueSize = 1000;
    public int cacheSize = 0;
    public int repeatedKeyCount;
    public int threadCount = 10;


    private int perThreadKey;
    private int perThreadRepeatedKey;
    private byte[][] keys;
    private byte[][] values;

    private RMap<byte[], byte[]> map;
    private final AtomicInteger id = new AtomicInteger();


    @Setup
    public void setUp() {
        perThreadKey = keyCount / threadCount;
        System.out.println("perThreadKey " + perThreadKey);

        perThreadRepeatedKey = repeatedKeyCount / threadCount;
        System.out.println("perThreadRepeatedKey " + perThreadRepeatedKey);

        Random random = new Random();
        keys = new byte[keyCount][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }


        if (cacheSize > 0) {

            LocalCachedMapOptions options = LocalCachedMapOptions.defaults()
                    .cacheSize(cacheSize)
                    .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU);

            map = client.getLocalCachedMap("map", options);
            System.out.println(" Map name : " + "map" +
                    " with near cache size  : " + cacheSize +
                    ", map size :" + map.size());
        } else {
            map = client.getMap("map");
            System.out.println(" Map name : " + "map" + "," +
                    " Regular map no cache" +
                    " map size " + map.size());
        }
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            values[i] = generateByteArray(random, valueSize);
        }

        for (byte[] key : keys) {
            map.fastPut(key, values[random.nextInt(values.length)]);
        }
    }

    @Prepare (global = true)
    public void prepareGlobal() {
        System.out.println(" Map name : " + map.getName() +
                " with near cache size  : " + cacheSize +
                ", map size :" + map.size());
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.id = id.incrementAndGet();
        state.base = (state.id) * perThreadKey;
        state.currentBase = state.base;

        System.out.println("Thread " + ", inverval [" + (state.base + "," + (state.base + perThreadKey) + "]"));
    }

    @TimeStep(prob = 0)
    public byte[] getRepeated(ThreadState state) {
        return map.get(state.randomRepeatedKey());
    }

    @TimeStep(prob = 0)
    public byte[] put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public byte[] get(ThreadState state) {
        return map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {
        private int id;
        private int base;
        private int currentBase;
        private int iteration;
        private int count;


        private int randomRepeated() {
            count++;
            if (count == perThreadRepeatedKey * 100) {
                count = 0;
                iteration++;
                currentBase = ((iteration * perThreadRepeatedKey) % perThreadKey) + base;
            }

            return ((count/2) % perThreadRepeatedKey) + currentBase;
        }

        private byte[] randomRepeatedKey() {
            return keys[randomRepeated()];
        }

        private byte[] randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown (global = true)
    public void tearDown() {
        map.deleteAsync();
    }
}
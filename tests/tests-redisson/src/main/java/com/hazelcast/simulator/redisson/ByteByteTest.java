package com.hazelcast.simulator.redisson;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RMap;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class ByteByteTest extends RedissonTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int valueSize = 1000;
    public int cacheSize = 0;
    public int recurringHitKeyCount;
    public int threadCount = 10;


    private int perThreadKeys;
    private byte[][] keys;
    private byte[][] values;

    private RMap<byte[], byte[]> map;

    @Setup
    public void setUp() {
        perThreadKeys = recurringHitKeyCount / threadCount;
        System.out.println("perThreadKeys " + perThreadKeys);

        Random random = new Random();
        keys = new byte[keyCount][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }

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
        System.out.println("Total map size " + map.size());
        System.out.println("Total map memory size " + map.sizeInMemory());
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.base = state.randomInt(keys.length);
    }

    @TimeStep(prob = 0.1)
    public byte[] put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public byte[] get(ThreadState state) {
        return map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {
        private int base;
        private int count;

        private byte[] randomKey() {
            if (count++ == perThreadKeys * 100) {
                count = 0;
                base = randomInt(keys.length);
            }

            return keys[((base + count) % perThreadKeys)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown (global = true)
    public void tearDown() {
        map.delete();
    }

}

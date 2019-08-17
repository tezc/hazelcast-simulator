package com.hazelcast.simulator.tests;


import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class ByteByteRepeatedSingleMapTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int valueSize = 1000;
    public int repeatedKeyCount;
    public int threadCount = 10;


    private int perThreadKey;
    private int perThreadRepeatedKey;
    private byte[][] keys;
    private byte[][] values;

    private IMap<byte[], byte[]> map;
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

        map = targetInstance.getMap("map");
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            values[i] = generateByteArray(random, valueSize);
        }

        Streamer<byte[], byte[]> streamer = StreamerFactory.getInstance(map);
        for (byte[] key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();

        System.out.println("Total map size " + map.size());
    }

    @Prepare (global = true)
    public void prepareGlobal() {
        System.out.println("Total map size " + map.size());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Total map size " + map.size());
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.id = id.getAndIncrement();
        state.base = (state.id) * perThreadKey;
        state.currentBase = state.base;

        System.out.println("Thread " + map.getName() + ", inverval [" + state.base + "," + (state.base + perThreadKey) + "]");
    }

    @TimeStep(prob = 0)
    public byte[] getRepeated(ThreadState state) {
        return map.get(state.randomRepeatedKey());
    }

    @TimeStep(prob = 0)
    public byte[] getRepeated2(ThreadState state) {
        return map.get(state.randomRepeatedKey2());
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

        private int randomRepeated2() {
            count++;
            if (count == perThreadRepeatedKey * 100) {
                count = 0;
                iteration++;
                currentBase = ((iteration * perThreadRepeatedKey) % perThreadKey) + base;
            }

            return ((count/100) % perThreadRepeatedKey) + currentBase;
        }

        private byte[] randomRepeatedKey() {
            return keys[randomRepeated()];
        }

        private byte[] randomRepeatedKey2() {
            return keys[randomRepeated2()];
        }

        private byte[] randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Verify (global = false)
    public void printStats() {
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        if (stats != null) {
            System.out.println("Hits : " + stats.getHits() +
                    " Misses :  " + stats.getMisses() +
                    " Hit ratio : " + stats.getRatio());
        }
    }

    @Teardown (global = true)
    public void tearDown() {
        map.destroy();
    }
}
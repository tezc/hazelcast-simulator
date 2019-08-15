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

public class ByteByteRepeatedTest2 extends HazelcastTest {

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

    private final IMap<byte[], byte[]>[] maps = new IMap[5];
    private final String[] mapNames = {"map", "map33", "map3", "map12", "map22"};
    private final AtomicInteger id = new AtomicInteger();

    @Setup
    public void setUp() {
        perThreadKey = keyCount / threadCount / maps.length;
        System.out.println("perThreadKey " + perThreadKey);

        perThreadRepeatedKey = repeatedKeyCount / threadCount;
        System.out.println("perThreadRepeatedKey " + perThreadRepeatedKey);

        Random random = new Random();
        keys = new byte[keyCount / maps.length][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }

        for (int i = 0; i < maps.length; i++) {
            maps[i] = targetInstance.getMap(mapNames[i]);
            System.out.println("Map name : " + mapNames[i] + ", map size " + maps[i].size());
        }
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            values[i] = generateByteArray(random, valueSize);
        }

        for (IMap<byte[], byte[]> map : maps) {
            Streamer<byte[], byte[]> streamer = StreamerFactory.getInstance(map);
            for (byte[] key : keys) {
                streamer.pushEntry(key, values[random.nextInt(values.length)]);
            }
            streamer.await();
        }

        for (IMap<byte[], byte[]> map : maps) {
            System.out.println("Total map size " + map.size());
        }
    }

    @Prepare (global = true)
    public void prepareGlobal() {
        for (IMap<byte[], byte[]> map : maps) {
            System.out.println("Total map size " + map.size());
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (IMap<byte[], byte[]> map : maps) {
            System.out.println("Total map size " + map.size());
        }
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.id = id.getAndIncrement();
        state.map = maps[state.id % maps.length];
    }

    @TimeStep(prob = 0)
    public byte[] putRepeated(ThreadState state) {
        byte[] ret = state.map.put(state.randomRepeatedKey(), state.randomValue());
        if (ret == null) {
            throw new RuntimeException("Null return");
        }

        return ret;
    }

    @TimeStep(prob = 0)
    public byte[] getRepeated(ThreadState state) {
        byte[] ret = state.map.get(state.randomRepeatedKey());
        if (ret == null) {
            throw new RuntimeException("Null return");
        }

        return ret;
    }

    @TimeStep(prob = 0)
    public byte[] put(ThreadState state) {
        byte[] ret = state.randomMap().put(state.randomKey(), state.randomValue());
        if (ret == null) {
            throw new RuntimeException("Null return");
        }

        return ret;
    }

    @TimeStep(prob = 0)
    public byte[] get(ThreadState state) {
        byte[] ret = state.randomMap().get(state.randomKey());
        if (ret == null) {
            throw new RuntimeException("Null return");
        }

        return ret;
    }

    public class ThreadState extends BaseThreadState {
        private IMap<byte[], byte[]> map;
        private int id;
        private int count;
        private int base;


        private int randomRepeated() {
            int value = count++ / 2;
            if (count == perThreadRepeatedKey * 100) {
                count = 0;
                base = randomInt(keys.length - perThreadRepeatedKey);
            }

            int key = ((value) % perThreadRepeatedKey) + base;
            System.out.println("Key is " + key);

            return key;
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

        private IMap<byte[], byte[]> randomMap() {
            return maps[randomInt(maps.length)];
        }
    }

    @Verify
    public void printStats() {
        for (IMap map : maps) {
            NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
            if (stats != null) {
                System.out.println("Hits : " + stats.getHits() +
                        " Misses :  " + stats.getMisses() +
                        " Hit ratio : " + stats.getRatio());
            }
        }
    }

    @Teardown (global = true)
    public void tearDown() {
        for (IMap map : maps) {
            map.destroy();
        }
    }
}
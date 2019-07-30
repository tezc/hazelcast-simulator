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

public class ByteByteRecurrentTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int valueSize = 1000;
    public int recurrentKeyCount;
    public int threadCount = 10;


    private int perThreadKey;
    private int perThreadRecurrentKey;
    private byte[][] keys;
    private byte[][] values;

    private IMap<byte[], byte[]>[] maps = new IMap[5];
    private String[] mapNames = {"map", "map33", "map3", "map12", "map22"};
    private final AtomicInteger id = new AtomicInteger();

    @Setup
    public void setUp() {
        perThreadKey = keyCount / threadCount;
        System.out.println("perThreadKey " + perThreadKey);

        perThreadRecurrentKey = recurrentKeyCount / threadCount;
        System.out.println("perThreadRecurrentKey " + perThreadRecurrentKey);

        Random random = new Random();
        keys = new byte[keyCount / maps.length][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }

        for (int i = 0; i < maps.length; i++) {
            String mapName = mapNames[i];
            maps[i] = targetInstance.getMap(mapName);
            System.out.println("Map name : " + mapName + ", map size " + maps[i].size());
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
    }

    @Prepare (global = true)
    public void prepareGlobal() {
        for (IMap<byte[], byte[]> map : maps) {
            System.out.println("Total map size " + map.size());
        }
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.id = id.incrementAndGet();
        state.map = maps[state.id % maps.length];
        state.base = (state.id / maps.length) * perThreadKey;
        state.currentBase = state.base;

        System.out.println("Thread " + state.map.getName() + ", inverval [" + state.base + "," + (state.base + perThreadKey) + "]");
    }

    @TimeStep(prob = 0)
    public byte[] putRecurrent(ThreadState state) {
        return state.map.put(state.randomRecurrentKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public byte[] getRecurrent(ThreadState state) {
        return state.map.get(state.randomRecurrentKey());
    }

    @TimeStep(prob = 0)
    public byte[] put(ThreadState state) {
        return state.randomMap().put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public byte[] get(ThreadState state) {
        return state.randomMap().get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {
        private IMap<byte[], byte[]> map;
        private int id;
        private int base;
        private int currentBase;
        private int iteration;
        private int count;


        private int randomRecurrent() {
            if (count++ == perThreadRecurrentKey * 100) {
                count = 0;
                currentBase = ((iteration++ * perThreadRecurrentKey) % perThreadKey) + base;
            }

            return ((count) % perThreadRecurrentKey) + currentBase;
        }

        private byte[] randomRecurrentKey() {
            return keys[randomRecurrent()];
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
                System.out.println("Hit ratio : " + stats.getRatio());
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
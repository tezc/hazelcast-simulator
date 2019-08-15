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

public class ByteByteRepeatedTest extends HazelcastTest {

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
        state.base = (state.id) * perThreadKey;
        state.currentBase = state.base;

        System.out.println("Thread " + state.map.getName() + ", inverval [" + state.base + "," + (state.base + perThreadKey) + "]");
    }

    @TimeStep(prob = 0)
    public byte[] getRepeated(ThreadState state) {
        return state.map.get(state.randomRepeatedKey());
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

        private IMap<byte[], byte[]> randomMap() {
            return maps[randomInt(maps.length)];
        }
    }

    @Verify (global = false)
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
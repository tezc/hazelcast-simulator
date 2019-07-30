package com.hazelcast.simulator.redisson;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RMap;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class ByteByteRepeatedTest extends RedissonTest {

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

    private RMap<byte[], byte[]>[] maps = new RMap[5];
    private String[] mapNames = {"map", "map33", "map3", "map12", "map22"};
    private final AtomicInteger id = new AtomicInteger();


    @Setup
    public void setUp() {
        perThreadKey = keyCount / threadCount;
        System.out.println("perThreadKey " + perThreadKey);

        perThreadRepeatedKey = repeatedKeyCount / threadCount;
        System.out.println("perThreadRepeatedKey " + perThreadRepeatedKey);

        Random random = new Random();
        keys = new byte[keyCount / maps.length][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }

        LocalCachedMapOptions options = LocalCachedMapOptions.defaults()
                .cacheSize(cacheSize)
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU);

        for (int i = 0; i < maps.length; i++) {
            String mapName = mapNames[i];
            if (cacheSize > 0) {
                maps[i] = client.getLocalCachedMap(mapName, options);
                System.out.println(" Map name : " + mapName +
                        " with near cache size  : " + cacheSize +
                        ", map size :" + this.maps[i].size());
            } else {
                maps[i] = client.getMap(mapName);
                System.out.println(" Map name : " + mapName + "," +
                        " Regular map no cache" +
                        " map size " + this.maps[i].size());
            }
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
            for (RMap<byte[], byte[]>map : maps) {
                map.fastPut(key, values[random.nextInt(values.length)]);
            }
        }
    }

    @Prepare (global = true)
    public void prepareGlobal() {
        for (RMap map : maps) {
            System.out.println(" Map name : " + map.getName() +
                    " with near cache size  : " + cacheSize +
                    ", map size :" + map.size());
        }
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.id = id.incrementAndGet();
        state.map = maps[state.id % maps.length];
        state.base = (state.id / maps.length) * perThreadKey;
        state.currentBase = state.base;

        System.out.println("Thread " + state.map.getName() + ", inverval [" + (state.base + "," + state.base + perThreadKey) + "]");
    }

    @TimeStep(prob = 0)
    public byte[] putRepeated(ThreadState state) {
        return state.map.put(state.randomRepeatedKey(), state.randomValue());
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
        private RMap<byte[], byte[]> map;
        private int id;
        private int base;
        private int currentBase;
        private int iteration;
        private int count;


        private int randomRepeated() {
            if (count++ == perThreadRepeatedKey * 100) {
                count = 0;
                currentBase = ((iteration++ * perThreadRepeatedKey) % perThreadKey) + base;
            }

            return ((count) % perThreadRepeatedKey) + currentBase;
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

        private RMap<byte[], byte[]> randomMap() {
            return maps[randomInt(maps.length)];
        }
    }

    @Teardown (global = true)
    public void tearDown() {
        for (RMap map : maps) {
            map.deleteAsync();
        }
    }

}
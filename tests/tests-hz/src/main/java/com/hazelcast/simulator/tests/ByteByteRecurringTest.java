package com.hazelcast.simulator.tests;


import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class ByteByteRecurringTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int valueSize = 1000;
    public int recurringHitKeyCount;
    public int threadCount = 10;


    private int perThreadKeys;
    private byte[][] keys;
    private byte[][] values;

    private IMap<byte[], byte[]> map;

    @Setup
    public void setUp() {
        perThreadKeys = recurringHitKeyCount / threadCount;
        System.out.println("perThreadKeys " + perThreadKeys);

        Random random = new Random();
        keys = new byte[keyCount][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }

        map = targetInstance.getMap("map");
        System.out.println("Regular map no cache" + " map size " + this.map.size());
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
    }

    @Prepare (global = true)
    public void prepareGlobal() {
        System.out.println("Total map size " + map.size());
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.base = state.randomInt(keys.length);
    }

    @TimeStep(prob = 0.1)
    public byte[] putRecurring(ThreadState state) {
        return map.put(state.randomRecurringKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public byte[] getRecurring(ThreadState state) {
        return map.get(state.randomRecurringKey());
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
        private int base;
        private int count;

        private int randomRecurring() {
            if (count++ == perThreadKeys * 100) {
                count = 0;
                base = randomInt(keys.length);
            }

            return ((base + count) % perThreadKeys);
        }

        private byte[] randomRecurringKey() {
            return keys[randomRecurring()];
        }

        private byte[] randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown(global = true)
    public void tearDown() {
        map.destroy();
    }

}

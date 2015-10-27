package com.hazelcast.simulator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;

public class TestEnvironmentUtils {

    private static final Logger LOGGER = Logger.getLogger(TestEnvironmentUtils.class);
    private static final Logger ROOT_LOGGER = Logger.getRootLogger();
    private static final AtomicReference<Level> LOGGER_LEVEL = new AtomicReference<Level>();

    private static String originalUserDir;

    public static void setLogLevel(Level level) {
        if (LOGGER_LEVEL.compareAndSet(null, ROOT_LOGGER.getLevel())) {
            ROOT_LOGGER.setLevel(level);
        }
    }

    public static void resetLogLevel() {
        Level level = LOGGER_LEVEL.get();
        if (level != null && LOGGER_LEVEL.compareAndSet(level, null)) {
            ROOT_LOGGER.setLevel(level);
        }
    }

    public static void setDistributionUserDir() {
        originalUserDir = System.getProperty("user.dir");
        System.setProperty("user.dir", originalUserDir + "/dist/src/main/dist");

        LOGGER.info("original userDir: " + originalUserDir);
        LOGGER.info("actual userDir: " + System.getProperty("user.dir"));
        LOGGER.info("SIMULATOR_HOME: " + getSimulatorHome());
    }

    public static void resetUserDir() {
        System.setProperty("user.dir", originalUserDir);
    }

    public static void deleteLogs() {
        deleteQuiet(new File("dist/src/main/dist/workers"));
        deleteQuiet(new File("logs"));
        deleteQuiet(new File("workers"));
    }
}

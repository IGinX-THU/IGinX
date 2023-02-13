package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.sharedstore.redis.RedisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RedisPerformance {

    private static final Logger logger = LoggerFactory.getLogger(RedisPerformance.class);

    private static final int TEST_TIMES = 100;

    private static final int KEY_SIZE = 30;

    private static final Random random = new Random(0);

    private static final RedisStore redis = RedisStore.getInstance();

    public static void checkPerformance() {
        logger.info("[PerformanceTest][RedisPerformance] start redis performance check");

        long pingLatencySum = 0L;
        for (int i = 0; i < TEST_TIMES; i++) {
            long startTime = System.currentTimeMillis();
            redis.ping();
            pingLatencySum += (System.currentTimeMillis() - startTime);
        }
        logger.info("[PerformanceTest][RedisPerformance] the average latency of ping request is {}", pingLatencySum * 1.0 / TEST_TIMES);

        int[] scales = new int[] {1024, 1024 * 4, 1024 * 16, 1024 * 256, 1024 * 1024, 1024 * 1024 * 4, 1024 * 1024 * 16, 1024 * 1024 * 32, 1024 * 1024 * 64, 1024 * 1024 * 128, 1024 * 1024 * 256}; // 1KB 4KB 16KB 256KB 1MB 4MB 16MB
        Map<Integer, String> scaleNames = new HashMap<>();
        scaleNames.put(1024, "1KB");
        scaleNames.put(1024 * 4, "4KB");
        scaleNames.put(1024 * 16, "16KB");
        scaleNames.put(1024 * 256, "256KB");
        scaleNames.put(1024 * 1024, "1MB");
        scaleNames.put(1024 * 1024 * 4, "4MB");
        scaleNames.put(1024 * 1024 * 16, "16MB");
        scaleNames.put(1024 * 1024 * 32, "32MB");
        scaleNames.put(1024 * 1024 * 64, "64MB");
        scaleNames.put(1024 * 1024 * 128, "128MB");
        scaleNames.put(1024 * 1024 * 256, "256MB");
        for (int scale: scales) {
            long writeLatencySum = 0L;
            long readLatencySum = 0L;
            byte[] value = randomBytes(scale);
            for (int i = 0; i < TEST_TIMES; i++) {
                byte[] key = randomBytes(KEY_SIZE);
                long startTime = System.currentTimeMillis();
                if (!redis.put(key, value)) {
                    logger.info("[PerformanceTest][RedisPerformance] put key error, scale = {}", scaleNames.get(scale));
                }
                writeLatencySum += (System.currentTimeMillis() - startTime);

                startTime = System.currentTimeMillis();
                if (redis.get(key) == null) {
                    logger.info("[PerformanceTest][RedisPerformance] get key error, scale = {}", scaleNames.get(scale));
                }
                readLatencySum += (System.currentTimeMillis() - startTime);
            }

            logger.info("[PerformanceTest][RedisPerformance][scale={}] the average latency of get key is {}ms, put key is {}ms", scaleNames.get(scale), readLatencySum * 1.0 / TEST_TIMES, writeLatencySum * 1.0 / TEST_TIMES);
        }
    }

    private static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

}

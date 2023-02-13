package cn.edu.tsinghua.iginx.sharedstore.redis;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.sharedstore.SharedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisStore implements SharedStore {

    private static final Logger logger = LoggerFactory.getLogger(RedisStore.class);

    private static final RedisStore INSTANCE = new RedisStore();

    private final JedisPool jedisPool;

    private RedisStore() {
        logger.info("[PerformanceTest][RedisStore] redis connection string: {}", ConfigDescriptor.getInstance().getConfig().getShardStorageConnectionString());
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        jedisPool = new JedisPool(config, ConfigDescriptor.getInstance().getConfig().getShardStorageConnectionString());
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return "OK".equals(jedis.set(key, value));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public byte[] get(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public boolean delete(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key) == 1L;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean exists(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    public String ping() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.ping();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    public static RedisStore getInstance() {
        return INSTANCE;
    }

}

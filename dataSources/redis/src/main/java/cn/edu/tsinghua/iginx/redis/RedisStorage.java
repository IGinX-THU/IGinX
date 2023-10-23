package cn.edu.tsinghua.iginx.redis;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.redis.entity.RedisQueryRowStream;
import cn.edu.tsinghua.iginx.redis.tools.DataCoder;
import cn.edu.tsinghua.iginx.redis.tools.DataTransformer;
import cn.edu.tsinghua.iginx.redis.tools.DataViewWrapper;
import cn.edu.tsinghua.iginx.redis.tools.FilterUtils;
import cn.edu.tsinghua.iginx.redis.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisStorage implements IStorage {

  private static final Logger logger = LoggerFactory.getLogger(RedisStorage.class);

  private static final String KEY_DATA_TYPE = "data:type";

  private static final String KEY_FORMAT_HASH_VALUES = "values:%s:%s";

  private static final String KEY_FORMAT_ZSET_KEYS = "keys:%s:%s";

  private static final String KEY_FORMAT_STRING_PATH = "path:%s:%s";

  private static final String EMTPY_STRING = "";

  private static final byte CLOSED_SIGN = (byte) '[';

  private static final String KEY_SPLIT = ":";

  private static final String STAR = "*";

  private static final String TAG_SUFFIX = STAR;

  private static final String SUFFIX_KEY = ".key";

  private static final String SUFFIX_VALUE = ".value";

  private static final String TIMEOUT = "timeout";

  private static final int DEFAULT_TIMEOUT = 5000;

  private final StorageEngineMeta meta;

  private final JedisPool jedisPool;

  public RedisStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    if (!meta.getStorageEngine().equals(StorageEngineType.redis)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    this.jedisPool = createJedisPool();
  }

  private JedisPool createJedisPool() {
    Map<String, String> extraParams = meta.getExtraParams();
    int timeout =
        Integer.parseInt(extraParams.getOrDefault(TIMEOUT, String.valueOf(DEFAULT_TIMEOUT)));
    return new JedisPool(new JedisPoolConfig(), meta.getIp(), meta.getPort(), timeout);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    List<String> queryPaths;
    try {
      queryPaths = determinePathList(storageUnit, project.getPatterns(), project.getTagFilter());
    } catch (PhysicalException e) {
      logger.error("encounter error when delete path: " + e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute delete path task in redis failure", e));
    }

    Filter filter = select.getFilter();
    List<Pair<Long, Long>> keyRanges = FilterUtils.keyRangesFrom(filter);

    List<cn.edu.tsinghua.iginx.redis.entity.Column> columns = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      for (String queryPath : queryPaths) {
        DataType type = DataTransformer.fromStringDataType(jedis.hget(KEY_DATA_TYPE, queryPath));
        if (type != null) {
          byte[] hashKey =
              DataCoder.encode(String.format(KEY_FORMAT_HASH_VALUES, storageUnit, queryPath));
          Map<Long, String> colData = new HashMap<>();
          if (keyRanges == null) {
            Map<byte[], byte[]> allData = jedis.hgetAll(hashKey);
            colData =
                allData.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> DataCoder.decodeToLong(e.getKey()),
                            e -> DataCoder.decodeToString(e.getValue())));
          } else if (!keyRanges.isEmpty()) {
            byte[] zSetKey =
                DataCoder.encode(String.format(KEY_FORMAT_ZSET_KEYS, storageUnit, queryPath));
            List<byte[]> keys = new ArrayList<>();
            for (Pair<Long, Long> keyRange : keyRanges) {
              byte[] beginKeyRange = concat(CLOSED_SIGN, DataCoder.encode(keyRange.getK()));
              byte[] endKeyRange = concat(CLOSED_SIGN, DataCoder.encode(keyRange.getV()));
              keys.addAll(jedis.zrangeByLex(zSetKey, beginKeyRange, endKeyRange));
            }

            if (!keys.isEmpty()) {
              List<byte[]> values = jedis.hmget(hashKey, keys.toArray(new byte[0][0]));
              ListIterator<byte[]> keyIter = keys.listIterator();
              ListIterator<byte[]> valueIter = values.listIterator();
              while (keyIter.hasNext()) {
                byte[] rawKey = keyIter.next();
                byte[] rawValue = valueIter.next();
                long key = DataCoder.decodeToLong(rawKey);
                String value = DataCoder.decodeToString(rawValue);
                colData.put(key, value);
              }
            }
          }

          cn.edu.tsinghua.iginx.redis.entity.Column column =
              new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, type, colData);
          columns.add(column);
        }
      }
    } catch (Exception e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute query path task in redis failure", e));
    }

    return new TaskExecuteResult(new RedisQueryRowStream(columns, filter), null);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    List<String> patterns = project.getPatterns();
    Set<String> queryPaths = new HashSet<>();
    for (String pattern : patterns) {
      if (pattern.contains(STAR)) {
        queryPaths.addAll(getKeysByPattern(pattern));
      } else {
        queryPaths.add(pattern);
      }
    }

    List<cn.edu.tsinghua.iginx.redis.entity.Column> columns = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      for (String queryPath : queryPaths) {
        String type = jedis.type(queryPath);
        switch (type) {
          case "string":
            String value = jedis.get(queryPath);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, value));
            break;
          case "list":
            List<String> listValues = jedis.lrange(queryPath, 0, -1);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, listValues));
            break;
          case "set":
            Set<String> setValues = jedis.smembers(queryPath);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, setValues));
            break;
          case "zset":
            List<String> zSetValues = jedis.zrange(queryPath, 0, -1);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, zSetValues));
            break;
          case "hash":
            Map<String, String> hashValues = jedis.hgetAll(queryPath);
            columns.add(
                new cn.edu.tsinghua.iginx.redis.entity.Column(
                    queryPath + SUFFIX_KEY, hashValues.keySet()));
            columns.add(
                new cn.edu.tsinghua.iginx.redis.entity.Column(
                    queryPath + SUFFIX_VALUE, new ArrayList<>(hashValues.values())));
            break;
          case "none":
            logger.warn("key {} not exists", queryPath);
          default:
            logger.warn("unknown key type, type={}", type);
        }
      }
    } catch (Exception e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              "execute query history task in redis failure", e));
    }

    Filter filter = select.getFilter();
    return new TaskExecuteResult(new RedisQueryRowStream(columns, filter), null);
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    List<String> queryPaths;
    try {
      queryPaths = determinePathList(storageUnit, project.getPatterns(), project.getTagFilter());
    } catch (PhysicalException e) {
      logger.error("encounter error when delete path: " + e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute delete path task in redis failure", e));
    }

    List<cn.edu.tsinghua.iginx.redis.entity.Column> columns = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      for (String queryPath : queryPaths) {
        DataType type = DataTransformer.fromStringDataType(jedis.hget(KEY_DATA_TYPE, queryPath));
        if (type != null) {
          byte[] hashKey =
              DataCoder.encode(String.format(KEY_FORMAT_HASH_VALUES, storageUnit, queryPath));
          Map<byte[], byte[]> allData = jedis.hgetAll(hashKey);
          Map<Long, String> colData =
              allData.entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          e -> DataCoder.decodeToLong(e.getKey()),
                          e -> DataCoder.decodeToString(e.getValue())));
          cn.edu.tsinghua.iginx.redis.entity.Column column =
              new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, type, colData);
          columns.add(column);
        }
      }
    } catch (Exception e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute query path task in redis failure", e));
    }
    return new TaskExecuteResult(new RedisQueryRowStream(columns), null);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    List<String> patterns = project.getPatterns();
    Set<String> queryPaths = new HashSet<>();
    for (String pattern : patterns) {
      if (pattern.contains(STAR)) {
        queryPaths.addAll(getKeysByPattern(pattern));
      } else {
        queryPaths.add(pattern);
      }
    }

    List<cn.edu.tsinghua.iginx.redis.entity.Column> columns = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      for (String queryPath : queryPaths) {
        String type = jedis.type(queryPath);
        switch (type) {
          case "string":
            String value = jedis.get(queryPath);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, value));
            break;
          case "list":
            List<String> listValues = jedis.lrange(queryPath, 0, -1);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, listValues));
            break;
          case "set":
            Set<String> setValues = jedis.smembers(queryPath);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, setValues));
            break;
          case "zset":
            List<String> zSetValues = jedis.zrange(queryPath, 0, -1);
            columns.add(new cn.edu.tsinghua.iginx.redis.entity.Column(queryPath, zSetValues));
            break;
          case "hash":
            Map<String, String> hashValues = jedis.hgetAll(queryPath);
            columns.add(
                new cn.edu.tsinghua.iginx.redis.entity.Column(
                    queryPath + SUFFIX_KEY, hashValues.keySet()));
            columns.add(
                new cn.edu.tsinghua.iginx.redis.entity.Column(
                    queryPath + SUFFIX_VALUE, new ArrayList<>(hashValues.values())));
            break;
          case "none":
            logger.warn("key {} not exists", queryPath);
          default:
            logger.warn("unknown key type, type={}", type);
        }
      }
    } catch (Exception e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              "execute query history task in redis failure", e));
    }
    return new TaskExecuteResult(new RedisQueryRowStream(columns), null);
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    List<String> deletedPaths;
    try {
      deletedPaths = determinePathList(storageUnit, delete.getPatterns(), delete.getTagFilter());
    } catch (PhysicalException e) {
      logger.warn("encounter error when delete path: " + e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute delete path task in redis failure", e));
    }

    if (deletedPaths.isEmpty()) {
      return new TaskExecuteResult(null, null);
    }

    if (delete.getKeyRanges() == null || delete.getKeyRanges().isEmpty()) {
      // 没有传任何 time range, 删除全部数据
      try (Jedis jedis = jedisPool.getResource()) {
        int size = deletedPaths.size();
        String[] deletedPathArray = new String[size * 3];
        for (int i = 0; i < size; i++) {
          String path = deletedPaths.get(i);
          deletedPathArray[i] = String.format(KEY_FORMAT_HASH_VALUES, storageUnit, path);
          deletedPathArray[i + size] = String.format(KEY_FORMAT_ZSET_KEYS, storageUnit, path);
          deletedPathArray[i + 2 * size] = String.format(KEY_FORMAT_STRING_PATH, storageUnit, path);
        }
        jedis.del(deletedPathArray);
        jedis.hdel(KEY_DATA_TYPE, deletedPaths.toArray(new String[0]));
      } catch (Exception e) {
        logger.warn("encounter error when delete path: " + e.getMessage());
        return new TaskExecuteResult(
            new PhysicalException("execute delete path in redis failure", e));
      }
    } else {
      // 删除指定部分数据
      try (Jedis jedis = jedisPool.getResource()) {
        for (String path : deletedPaths) {
          for (KeyRange keyRange : delete.getKeyRanges()) {
            byte[] zSetKey =
                DataCoder.encode(String.format(KEY_FORMAT_ZSET_KEYS, storageUnit, path));
            byte[] beginKeyRange =
                concat(CLOSED_SIGN, DataCoder.encode(keyRange.getActualBeginKey()));
            byte[] endKeyRange = concat(CLOSED_SIGN, DataCoder.encode(keyRange.getActualEndKey()));

            List<byte[]> keys = jedis.zrangeByLex(zSetKey, beginKeyRange, endKeyRange);
            if (!keys.isEmpty()) {
              byte[] hashKey =
                  DataCoder.encode(String.format(KEY_FORMAT_HASH_VALUES, storageUnit, path));
              jedis.hdel(hashKey, keys.toArray(new byte[0][0]));
              jedis.zremrangeByLex(zSetKey, beginKeyRange, endKeyRange);
            }
          }
        }
      } catch (Exception e) {
        logger.warn("encounter error when delete path: " + e.getMessage());
        return new TaskExecuteResult(
            new PhysicalException("execute delete data in redis failure", e));
      }
    }
    return new TaskExecuteResult(null, null);
  }

  private List<String> determinePathList(
      String storageUnit, List<String> patterns, TagFilter tagFilter) throws PhysicalException {
    boolean hasTagFilter = tagFilter != null;
    List<String> paths = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      for (String pattern : patterns) {
        String queryPattern = String.format(KEY_FORMAT_STRING_PATH, storageUnit, pattern);
        queryPattern += TAG_SUFFIX;
        Set<String> set = jedis.keys(queryPattern);
        set.forEach(
            key -> {
              String[] splits = key.split(KEY_SPLIT);
              if (splits.length == 3) {
                paths.add(splits[2]);
              }
            });
      }
    } catch (Exception e) {
      logger.error("get du time series error, cause by: ", e);
      return Collections.emptyList();
    }
    if (!hasTagFilter) {
      return paths;
    }

    List<String> filterPaths = new ArrayList<>();
    for (String path : paths) {
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
      if (TagKVUtils.match(pair.getV(), tagFilter)) {
        filterPaths.add(path);
      }
    }
    return filterPaths;
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    DataViewWrapper data = new DataViewWrapper(insert.getData());
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      String type = DataTransformer.toStringDataType(data.getDataType(i));

      Pair<Map<byte[], byte[]>, Map<byte[], Double>> pair = data.getPathData(i);

      Map<byte[], byte[]> values = pair.getK();
      Map<byte[], Double> scores = pair.getV();

      try (Jedis jedis = jedisPool.getResource()) {
        byte[] hashKey = DataCoder.encode(String.format(KEY_FORMAT_HASH_VALUES, storageUnit, path));
        jedis.hset(hashKey, values);

        byte[] zSetKey = DataCoder.encode(String.format(KEY_FORMAT_ZSET_KEYS, storageUnit, path));
        jedis.zadd(zSetKey, scores);

        jedis.hset(KEY_DATA_TYPE, path, type);
        jedis.set(String.format(KEY_FORMAT_STRING_PATH, storageUnit, path), EMTPY_STRING);
      } catch (Exception e) {
        return new TaskExecuteResult(new PhysicalException("execute insert in redis error", e));
      }
    }
    return new TaskExecuteResult(null, null);
  }

  @Override
  public List<Column> getColumns() {
    List<Column> ret = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      Map<String, String> pathsAndTypes = jedis.hgetAll(KEY_DATA_TYPE);
      pathsAndTypes.forEach(
          (k, v) -> {
            DataType type = DataTransformer.fromStringDataType(v);
            Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(k);
            ret.add(new Column(pair.k, type, pair.v));
          });
    } catch (Exception e) {
      logger.error("get time series error, cause by: ", e);
      return ret;
    }
    return ret;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    List<String> paths = getKeysByPattern(STAR);
    paths.sort(String::compareTo);

    if (paths.isEmpty()) {
      throw new PhysicalTaskExecuteFailureException("no data!");
    }

    ColumnsInterval columnsInterval;
    if (prefix != null) {
      columnsInterval = new ColumnsInterval(prefix);
    } else {
      if (!paths.isEmpty()) {
        columnsInterval =
            new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
      } else {
        columnsInterval = new ColumnsInterval(null, null);
      }
    }
    long minTime = 0, maxTime = Long.MIN_VALUE;
    try (Jedis jedis = jedisPool.getResource()) {
      for (String path : paths) {
        String type = jedis.type(path);
        switch (type) {
          case "string":
            maxTime = Math.max(maxTime, 1);
            break;
          case "list":
            maxTime = Math.max(maxTime, jedis.llen(path));
            break;
          case "set":
            maxTime = Math.max(maxTime, jedis.scard(path));
            break;
          case "zset":
            maxTime = Math.max(maxTime, jedis.zcard(path));
            break;
          case "hash":
            maxTime = Math.max(maxTime, jedis.hlen(path));
            break;
          case "none":
            logger.warn("key {} not exists", path);
          default:
            logger.warn("unknown key type, type={}", type);
        }
      }
    } catch (Exception e) {
      logger.error("get keys' length error, cause by: ", e);
    }
    if (maxTime == Long.MIN_VALUE) {
      maxTime = Long.MAX_VALUE - 1;
    }
    KeyInterval keyInterval = new KeyInterval(minTime, maxTime + 1);
    return new Pair<>(columnsInterval, keyInterval);
  }

  private List<String> getKeysByPattern(String pattern) {
    List<String> paths = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      Set<String> keys = jedis.keys(pattern);
      paths.addAll(keys);
    } catch (Exception e) {
      logger.error("get keys error, cause by: ", e);
    }
    return paths;
  }

  private static byte[] concat(byte prefix, byte[] arr) {
    byte[] ret = new byte[1 + arr.length];
    ret[0] = prefix;
    System.arraycopy(arr, 0, ret, 1, arr.length);
    return ret;
  }

  @Override
  public void release() throws PhysicalException {
    jedisPool.close();
  }
}

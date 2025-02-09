/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.conf;

import cn.edu.tsinghua.iginx.utils.EnvUtils;
import java.io.*;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);

  private final Config config;

  private ConfigDescriptor() {
    config = new Config();
    LOGGER.info("load parameters from config.properties.");
    loadPropsFromFile();
    if (config.isEnableEnvParameter()) {
      LOGGER.info("load parameters from env.");
      loadPropsFromEnv(); // 如果在环境变量中设置了相关参数，则会覆盖配置文件中设置的参数
    }
    if (config.isNeedInitBasicUDFFunctions()) {
      LOGGER.info("load UDF list from file.");
      loadUDFListFromFile();
    }
  }

  public static ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  private void loadPropsFromFile() {
    try (InputStream in =
        new FileInputStream(EnvUtils.loadEnv(Constants.CONF, Constants.CONFIG_FILE))) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));

      Properties properties = new Properties();
      properties.load(bufferedReader);

      // runs/debugged in IDE: IGINX_HOME not set, use user.dir as root
      // runs by script: IGINX_HOME should always have been set
      String iginxHomePath = EnvUtils.loadEnv(Constants.IGINX_HOME, System.getProperty("user.dir"));
      String udfPath = properties.getProperty("defaultUDFDir", "udf_funcs");
      if (!FileUtils.isAbsolutePath(udfPath)) {
        // if relative, build absolute path
        udfPath = String.join(File.separator, iginxHomePath, udfPath);
      }
      config.setDefaultUDFDir(udfPath);
      config.setIp(properties.getProperty("ip", "0.0.0.0"));
      config.setPort(Integer.parseInt(properties.getProperty("port", "6888")));
      config.setUsername(properties.getProperty("username", "root"));
      config.setPassword(properties.getProperty("password", "root"));
      config.setZookeeperConnectionString(
          properties.getProperty("zookeeperConnectionString", "127.0.0.1:2181"));
      config.setStorageEngineList(
          properties.getProperty(
              "storageEngineList",
              "127.0.0.1#6667#iotdb12#username=root#password=root#sessionPoolSize=20#dataDir=/path/to/your/data/"));
      config.setMaxAsyncRetryTimes(
          Integer.parseInt(properties.getProperty("maxAsyncRetryTimes", "3")));
      config.setSyncExecuteThreadPool(
          Integer.parseInt(properties.getProperty("syncExecuteThreadPool", "60")));
      config.setAsyncExecuteThreadPool(
          Integer.parseInt(properties.getProperty("asyncExecuteThreadPool", "20")));
      config.setReplicaNum(Integer.parseInt(properties.getProperty("replicaNum", "1")));

      config.setDatabaseClassNames(
          properties.getProperty(
              "databaseClassNames",
              "iotdb12=cn.edu.tsinghua.iginx.iotdb.IoTDBStorage,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBStorage,relational=cn.edu.tsinghua.iginx.relational.RelationalStorage,mongodb=cn.edu.tsinghua.iginx.mongodb.MongoDBStorage,redis=cn.edu.tsinghua.iginx.redis.RedisStorage,filesystem=cn.edu.tsinghua.iginx.filesystem.FileSystemStorage"));

      config.setPolicyClassName(
          properties.getProperty(
              "policyClassName", "cn.edu.tsinghua.iginx.policy.naive.NaivePolicy"));
      config.setMigrationBatchSize(
          Integer.parseInt(properties.getProperty("migrationBatchSize", "100")));
      config.setEnableMonitor(
          Boolean.parseBoolean(properties.getProperty("enableMonitor", "false")));
      config.setLoadBalanceCheckInterval(
          Integer.parseInt(properties.getProperty("loadBalanceCheckInterval", "10")));
      config.setEnableFragmentCompaction(
          Boolean.parseBoolean(properties.getProperty("enableFragmentCompaction", "false")));
      config.setFragmentCompactionWriteThreshold(
          Long.parseLong(properties.getProperty("fragmentCompactionWriteThreshold", "1000")));
      config.setFragmentCompactionReadThreshold(
          Long.parseLong(properties.getProperty("fragmentCompactionReadThreshold", "1000")));
      config.setFragmentCompactionReadRatioThreshold(
          Double.parseDouble(
              properties.getProperty("fragmentCompactionReadRatioThreshold", "0.1")));
      config.setReshardFragmentTimeMargin(
          Long.parseLong(properties.getProperty("reshardFragmentTimeMargin", "60")));
      config.setMaxReshardFragmentsNum(
          Integer.parseInt(properties.getProperty("maxReshardFragmentsNum", "3")));
      config.setMaxTimeseriesLoadBalanceThreshold(
          Double.parseDouble(properties.getProperty("maxTimeseriesLoadBalanceThreshold", "2")));
      config.setMigrationPolicyClassName(
          properties.getProperty(
              "migrationPolicyClassName",
              "cn.edu.tsinghua.iginx.migration.SimulationBasedMigrationPolicy"));
      config.setEnableEnvParameter(
          Boolean.parseBoolean(properties.getProperty("enableEnvParameter", "false")));

      config.setStatisticsCollectorClassName(
          properties.getProperty("statisticsCollectorClassName", ""));
      config.setStatisticsLogInterval(
          Integer.parseInt(properties.getProperty("statisticsLogInterval", "5000")));

      config.setRestIp(properties.getProperty("restIp", "127.0.0.1"));
      config.setRestPort(Integer.parseInt(properties.getProperty("restPort", "7888")));

      config.setDisorderMargin(Long.parseLong(properties.getProperty("disorderMargin", "10")));
      config.setAsyncRestThreadPool(
          Integer.parseInt(properties.getProperty("asyncRestThreadPool", "100")));

      config.setMaxTimeseriesLength(
          Integer.parseInt(properties.getProperty("maxtimeserieslength", "10")));
      config.setEnableRestService(
          Boolean.parseBoolean(properties.getProperty("enableRestService", "true")));

      config.setMetaStorage(properties.getProperty("metaStorage", "zookeeper"));
      config.setEtcdEndpoints(properties.getProperty("etcdEndpoints", "http://localhost:2379"));

      config.setEnableMQTT(Boolean.parseBoolean(properties.getProperty("enableMqtt", "false")));
      config.setMqttHost(properties.getProperty("mqttHost", "0.0.0.0"));
      config.setMqttPort(Integer.parseInt(properties.getProperty("mqttPort", "1883")));
      config.setMqttHandlerPoolSize(
          Integer.parseInt(properties.getProperty("mqttHandlerPoolSize", "1")));
      config.setMqttPayloadFormatter(
          properties.getProperty(
              "mqttPayloadFormatter", "cn.edu.tsinghua.iginx.mqtt.JsonPayloadFormatter"));
      config.setMqttMaxMessageSize(
          Integer.parseInt(properties.getProperty("mqttMaxMessageSize", "1048576")));

      config.setClients(properties.getProperty("clients", ""));
      config.setInstancesNumPerClient(
          Integer.parseInt(properties.getProperty("instancesNumPerClient", "0")));

      config.setQueryOptimizer(properties.getProperty("queryOptimizer", ""));
      config.setConstraintChecker(properties.getProperty("constraintChecker", "naive"));

      config.setPhysicalOptimizer(properties.getProperty("physicalOptimizer", "naive"));
      config.setMemoryTaskThreadPoolSize(
          Integer.parseInt(properties.getProperty("memoryTaskThreadPoolSize", "200")));
      config.setPhysicalTaskThreadPoolSizePerStorage(
          Integer.parseInt(properties.getProperty("physicalTaskThreadPoolSizePerStorage", "100")));

      config.setMaxCachedPhysicalTaskPerStorage(
          Integer.parseInt(properties.getProperty("maxCachedPhysicalTaskPerStorage", "500")));

      config.setCachedTimeseriesProb(
          Double.parseDouble(properties.getProperty("cachedTimeseriesProb", "0.01")));
      config.setRetryCount(Integer.parseInt(properties.getProperty("retryCount", "10")));
      config.setRetryWait(Integer.parseInt(properties.getProperty("retryWait", "5000")));
      config.setFragmentPerEngine(
          Integer.parseInt(properties.getProperty("fragmentPerEngine", "10")));
      config.setReAllocatePeriod(
          Integer.parseInt(properties.getProperty("reAllocatePeriod", "30000")));
      config.setEnableStorageGroupValueLimit(
          Boolean.parseBoolean(properties.getProperty("enableStorageGroupValueLimit", "true")));
      config.setStorageGroupValueLimit(
          Double.parseDouble(properties.getProperty("storageGroupValueLimit", "200.0")));

      config.setEnablePushDown(
          Boolean.parseBoolean(properties.getProperty("enablePushDown", "true")));
      config.setUseStreamExecutor(
          Boolean.parseBoolean(properties.getProperty("useStreamExecutor", "true")));

      config.setEnableMemoryControl(
          Boolean.parseBoolean(properties.getProperty("enableMemoryControl", "true")));
      config.setSystemResourceMetrics(properties.getProperty("systemResourceMetrics", "default"));
      config.setHeapMemoryThreshold(
          Double.parseDouble(properties.getProperty("heapMemoryThreshold", "0.9")));
      config.setSystemMemoryThreshold(
          Double.parseDouble(properties.getProperty("systemMemoryThreshold", "0.9")));
      config.setSystemCpuThreshold(
          Double.parseDouble(properties.getProperty("systemCpuThreshold", "0.9")));

      config.setEnableMetaCacheControl(
          Boolean.parseBoolean(properties.getProperty("enableMetaCacheControl", "false")));
      config.setFragmentCacheThreshold(
          Long.parseLong(properties.getProperty("fragmentCacheThreshold", "131072")));
      config.setBatchSize(Integer.parseInt(properties.getProperty("batchSize", "50")));
      config.setPythonCMD(properties.getProperty("pythonCMD", "python3"));
      config.setTransformTaskThreadPoolSize(
          Integer.parseInt(properties.getProperty("transformTaskThreadPoolSize", "10")));
      config.setTransformMaxRetryTimes(
          Integer.parseInt(properties.getProperty("transformMaxRetryTimes", "3")));
      config.setDefaultScheduledTransformJobDir(
          properties.getProperty("defaultScheduledTransformJobDir", "transform_jobs"));
      config.setNeedInitBasicUDFFunctions(
          Boolean.parseBoolean(properties.getProperty("needInitBasicUDFFunctions", "false")));
      config.setHistoricalPrefixList(properties.getProperty("historicalPrefixList", ""));
      config.setExpectedStorageUnitNum(
          Integer.parseInt(properties.getProperty("expectedStorageUnitNum", "0")));
      config.setMinThriftWorkerThreadNum(
          Integer.parseInt(properties.getProperty("minThriftWorkerThreadNum", "20")));
      config.setMaxThriftWrokerThreadNum(
          Integer.parseInt(properties.getProperty("maxThriftWorkerThreadNum", "2147483647")));
      config.setParallelFilterThreshold(
          Integer.parseInt(properties.getProperty("parallelFilterThreshold", "10000")));
      config.setParallelGroupByRowsThreshold(
          Integer.parseInt(properties.getProperty("parallelGroupByRowsThreshold", "10000")));
      config.setParallelApplyFuncGroupsThreshold(
          Integer.parseInt(properties.getProperty("parallelApplyFuncGroupsThreshold", "1000")));
      config.setParallelGroupByPoolSize(
          Integer.parseInt(properties.getProperty("parallelGroupByPoolSize", "5")));
      config.setParallelGroupByPoolNum(
          Integer.parseInt(properties.getProperty("parallelGroupByPoolNum", "5")));
      config.setStreamParallelGroupByWorkerNum(
          Integer.parseInt(properties.getProperty("streamParallelGroupByWorkerNum", "5")));
      config.setPipelineParallelism(
          Integer.parseInt(properties.getProperty("pipelineParallelism", "5")));
      config.setExecutionBatchRowCount(
          Integer.parseInt(properties.getProperty("executionBatchRowCount", "3970")));
      config.setGroupByInitialGroupBufferCapacity(
          Integer.parseInt(properties.getProperty("groupByInitialGroupBufferCapacity", "31")));
      config.setBatchSizeImportCsv(
          Integer.parseInt(properties.getProperty("batchSizeImportCsv", "10000")));
      config.setRuleBasedOptimizer(
          properties.getProperty(
              "ruleBasedOptimizer",
              "NotFilterRemoveRule=on,FragmentPruningByFilterRule=on,ColumnPruningRule=on,FragmentPruningByPatternRule=on"));
    } catch (IOException e) {
      config.setUTTestEnv(true);
      config.setNeedInitBasicUDFFunctions(false);
      loadPropsFromEnv();
      LOGGER.warn(
          "Use default config, because fail to load properties(This error may be expected if it occurs during UT testing): ",
          e);
    }
  }

  private void loadPropsFromEnv() {
    config.setIp(EnvUtils.loadEnv("ip", config.getIp()));
    config.setPort(EnvUtils.loadEnv("port", config.getPort()));
    config.setUsername(EnvUtils.loadEnv("username", config.getUsername()));
    config.setPassword(EnvUtils.loadEnv("password", config.getPassword()));
    config.setZookeeperConnectionString(
        EnvUtils.loadEnv("zookeeperConnectionString", config.getZookeeperConnectionString()));
    config.setStorageEngineList(
        EnvUtils.loadEnv("storageEngineList", config.getStorageEngineList()));
    config.setMaxAsyncRetryTimes(
        EnvUtils.loadEnv("maxAsyncRetryTimes", config.getMaxAsyncRetryTimes()));
    config.setSyncExecuteThreadPool(
        EnvUtils.loadEnv("syncExecuteThreadPool", config.getSyncExecuteThreadPool()));
    config.setAsyncExecuteThreadPool(
        EnvUtils.loadEnv("asyncExecuteThreadPool", config.getAsyncExecuteThreadPool()));
    config.setReplicaNum(EnvUtils.loadEnv("replicaNum", config.getReplicaNum()));
    config.setDatabaseClassNames(
        EnvUtils.loadEnv("databaseClassNames", config.getDatabaseClassNames()));
    config.setPolicyClassName(EnvUtils.loadEnv("policyClassName", config.getPolicyClassName()));
    config.setStatisticsCollectorClassName(
        EnvUtils.loadEnv("statisticsCollectorClassName", config.getStatisticsCollectorClassName()));
    config.setStatisticsLogInterval(
        EnvUtils.loadEnv("statisticsLogInterval", config.getStatisticsLogInterval()));
    config.setRestIp(EnvUtils.loadEnv("restIp", config.getRestIp()));
    config.setRestPort(EnvUtils.loadEnv("restPort", config.getRestPort()));
    config.setDisorderMargin(EnvUtils.loadEnv("disorderMargin", config.getDisorderMargin()));
    config.setMaxTimeseriesLength(
        EnvUtils.loadEnv("maxtimeserieslength", config.getMaxTimeseriesLength()));
    config.setAsyncRestThreadPool(
        EnvUtils.loadEnv("asyncRestThreadPool", config.getAsyncRestThreadPool()));
    config.setEnableRestService(
        EnvUtils.loadEnv("enableRestService", config.isEnableRestService()));
    config.setMetaStorage(EnvUtils.loadEnv("metaStorage", config.getMetaStorage()));
    config.setEtcdEndpoints(EnvUtils.loadEnv("etcdEndpoints", config.getEtcdEndpoints()));
    config.setEnableMQTT(EnvUtils.loadEnv("enableMqtt", config.isEnableMQTT()));
    config.setMqttHost(EnvUtils.loadEnv("mqttHost", config.getMqttHost()));
    config.setMqttPort(EnvUtils.loadEnv("mqttPort", config.getMqttPort()));
    config.setMqttHandlerPoolSize(
        EnvUtils.loadEnv("mqttHandlerPoolSize", config.getMqttHandlerPoolSize()));
    config.setMqttPayloadFormatter(
        EnvUtils.loadEnv("mqttPayloadFormatter", config.getMqttPayloadFormatter()));
    config.setMqttMaxMessageSize(
        EnvUtils.loadEnv("mqttMaxMessageSize", config.getMqttMaxMessageSize()));
    config.setQueryOptimizer(EnvUtils.loadEnv("queryOptimizer", config.getQueryOptimizer()));
    config.setConstraintChecker(
        EnvUtils.loadEnv("constraintChecker", config.getConstraintChecker()));
    config.setPhysicalOptimizer(
        EnvUtils.loadEnv("physicalOptimizer", config.getPhysicalOptimizer()));
    config.setMemoryTaskThreadPoolSize(
        EnvUtils.loadEnv("memoryTaskThreadPoolSize", config.getMemoryTaskThreadPoolSize()));
    config.setPhysicalTaskThreadPoolSizePerStorage(
        EnvUtils.loadEnv(
            "physicalTaskThreadPoolSizePerStorage",
            config.getPhysicalTaskThreadPoolSizePerStorage()));
    config.setMaxCachedPhysicalTaskPerStorage(
        EnvUtils.loadEnv(
            "maxCachedPhysicalTaskPerStorage", config.getMaxCachedPhysicalTaskPerStorage()));
    config.setCachedTimeseriesProb(
        EnvUtils.loadEnv("cachedTimeseriesProb", config.getCachedTimeseriesProb()));
    config.setRetryCount(EnvUtils.loadEnv("retryCount", config.getRetryCount()));
    config.setRetryWait(EnvUtils.loadEnv("retryWait", config.getRetryWait()));
    config.setFragmentPerEngine(
        EnvUtils.loadEnv("fragmentPerEngine", config.getFragmentPerEngine()));
    config.setReAllocatePeriod(EnvUtils.loadEnv("reAllocatePeriod", config.getReAllocatePeriod()));
    config.setEnableStorageGroupValueLimit(
        EnvUtils.loadEnv("enableStorageGroupValueLimit", config.isEnableStorageGroupValueLimit()));
    config.setStorageGroupValueLimit(
        EnvUtils.loadEnv("storageGroupValueLimit", config.getStorageGroupValueLimit()));
    config.setEnablePushDown(EnvUtils.loadEnv("enablePushDown", config.isEnablePushDown()));
    config.setUseStreamExecutor(
        EnvUtils.loadEnv("useStreamExecutor", config.isUseStreamExecutor()));
    config.setEnableMemoryControl(
        EnvUtils.loadEnv("enableMemoryControl", config.isEnableMemoryControl()));
    config.setSystemResourceMetrics(
        EnvUtils.loadEnv("systemResourceMetrics", config.getSystemResourceMetrics()));
    config.setHeapMemoryThreshold(
        EnvUtils.loadEnv("heapMemoryThreshold", config.getHeapMemoryThreshold()));
    config.setSystemMemoryThreshold(
        EnvUtils.loadEnv("systemMemoryThreshold", config.getSystemMemoryThreshold()));
    config.setSystemCpuThreshold(
        EnvUtils.loadEnv("systemCpuThreshold", config.getSystemCpuThreshold()));
    config.setEnableMetaCacheControl(
        EnvUtils.loadEnv("enableMetaCacheControl", config.isEnableMetaCacheControl()));
    config.setFragmentCacheThreshold(
        EnvUtils.loadEnv("fragmentCacheThreshold", config.getFragmentCacheThreshold()));
    config.setBatchSize(EnvUtils.loadEnv("batchSize", config.getBatchSize()));
    config.setPythonCMD(EnvUtils.loadEnv("pythonCMD", config.getPythonCMD()));
    config.setTransformTaskThreadPoolSize(
        EnvUtils.loadEnv("transformTaskThreadPoolSize", config.getTransformTaskThreadPoolSize()));
    config.setTransformMaxRetryTimes(
        EnvUtils.loadEnv("transformMaxRetryTimes", config.getTransformMaxRetryTimes()));
    config.setDefaultScheduledTransformJobDir(
        EnvUtils.loadEnv(
            "defaultScheduledTransformJobDir", config.getDefaultScheduledTransformJobDir()));
    config.setNeedInitBasicUDFFunctions(
        EnvUtils.loadEnv("needInitBasicUDFFunctions", config.isNeedInitBasicUDFFunctions()));
    config.setHistoricalPrefixList(
        EnvUtils.loadEnv("historicalPrefixList", config.getHistoricalPrefixList()));
    config.setExpectedStorageUnitNum(
        EnvUtils.loadEnv("expectedStorageUnitNum", config.getExpectedStorageUnitNum()));
    config.setParallelFilterThreshold(
        EnvUtils.loadEnv("parallelFilterThreshold", config.getParallelFilterThreshold()));
    config.setParallelGroupByRowsThreshold(
        EnvUtils.loadEnv("parallelGroupByRowsThreshold", config.getParallelGroupByRowsThreshold()));
    config.setParallelApplyFuncGroupsThreshold(
        EnvUtils.loadEnv(
            "parallelApplyFuncGroupsThreshold", config.getStreamParallelGroupByWorkerNum()));
    config.setParallelGroupByPoolSize(
        EnvUtils.loadEnv("parallelGroupByPoolSize", config.getParallelGroupByPoolSize()));
    config.setParallelGroupByPoolNum(
        EnvUtils.loadEnv("parallelGroupByPoolNum", config.getParallelGroupByPoolNum()));
    config.setStreamParallelGroupByWorkerNum(
        EnvUtils.loadEnv(
            "streamParallelGroupByWorkerNum", config.getStreamParallelGroupByWorkerNum()));
    config.setPipelineParallelism(
        EnvUtils.loadEnv("pipelineParallelism", config.getPipelineParallelism()));
    config.setExecutionBatchRowCount(
        EnvUtils.loadEnv("executionBatchRowCount", config.getExecutionBatchRowCount()));
    config.setGroupByInitialGroupBufferCapacity(
        EnvUtils.loadEnv(
            "groupByInitialGroupBufferCapacity", config.getGroupByInitialGroupBufferCapacity()));
    config.setBatchSizeImportCsv(
        EnvUtils.loadEnv("batchSizeImportCsv", config.getBatchSizeImportCsv()));
    config.setUTTestEnv(EnvUtils.loadEnv("utTestEnv", config.isUTTestEnv()));
    config.setRuleBasedOptimizer(
        EnvUtils.loadEnv("ruleBasedOptimizer", config.getRuleBasedOptimizer()));
  }

  private void loadUDFListFromFile() {
    String UDFFilePath =
        String.join(File.separator, config.getDefaultUDFDir(), Constants.UDF_LIST_FILE);
    try (InputStream in = new FileInputStream(UDFFilePath)) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));

      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        line = line.trim();
        if (line.toLowerCase().startsWith(Constants.UDAF)
            || line.toLowerCase().startsWith(Constants.UDTF)
            || line.toLowerCase().startsWith(Constants.UDSF)
            || line.toLowerCase().startsWith(Constants.TRANSFORM)) {
          config.getUdfList().add(line);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Fail to load udf list: ", e);
    }
  }

  public Config getConfig() {
    return config;
  }

  private static class ConfigDescriptorHolder {
    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }
}

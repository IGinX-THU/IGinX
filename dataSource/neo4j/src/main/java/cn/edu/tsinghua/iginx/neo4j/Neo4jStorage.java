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
package cn.edu.tsinghua.iginx.neo4j;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.neo4j.tools.Constants.IDENTITY_PROPERTY_NAME;
import static cn.edu.tsinghua.iginx.neo4j.tools.DataTransformer.fromIginxType;
import static cn.edu.tsinghua.iginx.neo4j.tools.DataTransformer.fromStringDataType;
import static cn.edu.tsinghua.iginx.neo4j.tools.Neo4jClientUtils.isDummy;
import static cn.edu.tsinghua.iginx.neo4j.tools.Neo4jClientUtils.trimPrefix;
import static cn.edu.tsinghua.iginx.neo4j.tools.TagKVUtils.splitFullName;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.neo4j.entity.Neo4jQueryRowStream;
import cn.edu.tsinghua.iginx.neo4j.tools.Constants;
import cn.edu.tsinghua.iginx.neo4j.tools.Neo4jClientUtils;
import cn.edu.tsinghua.iginx.neo4j.tools.PathUtils;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import io.reactivex.rxjava3.core.Flowable;
import java.util.*;
import java.util.concurrent.*;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStorage.class);
  private final Driver driver;

  private final StorageEngineMeta meta;

  /**
   * 构造函数，用于初始化 Neo4jStorage 实例。
   *
   * @param meta 存储引擎的元数据。
   * @throws StorageInitializationException 如果存储引擎类型不匹配或初始化过程中发生错误。
   */
  public Neo4jStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    LOGGER.info("init neo4j storage {} : {}", meta.getIp(), meta.getPort());
    if (!meta.getStorageEngine().equals(StorageEngineType.neo4j)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    Map<String, String> params = meta.getExtraParams();

    int poolSize =
        Integer.parseInt(
            params.getOrDefault(
                Constants.MAX_CONNECTION_POOL_SIZE,
                String.valueOf(Constants.DEFAULT_MAX_CONNECTION_POOL_SIZE)));
    int connectionTimeout =
        Integer.parseInt(
            params.getOrDefault(
                Constants.CONNECTION_TIMEOUT,
                String.valueOf(Constants.DEFAULT_CONNECTION_TIMEOUT)));
    int connectionCheckTimeout =
        Integer.parseInt(
            params.getOrDefault(
                Constants.CONNECTION_CHECK_TIMEOUT,
                String.valueOf(Constants.DEFAULT_CONNECTION_CHECK_TIMEOUT)));

    Config config =
        Config.builder()
            .withMaxConnectionPoolSize(poolSize)
            .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
            .withConnectionLivenessCheckTimeout(
                connectionCheckTimeout, java.util.concurrent.TimeUnit.SECONDS)
            .build();

    driver =
        createBoltDriver(
            meta.getIp(), meta.getPort(), params.get("username"), params.get("password"), config);
  }

  private Driver createBoltDriver(
      String ip, int port, String user, String password, Config config) {
    return GraphDatabase.driver(
        "bolt://" + ip + ":" + port, AuthTokens.basic(user, password), config);
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    try (Session session = driver.session()) {
      String result = session.run("RETURN '1'").single().get(0).asString();
      if ("1".equals(result)) {
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to validate connection: ", e);
    }
    return false;
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectWithFilter(project, filter, dataArea);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectDummyWithFilter(project, filter);
  }

  private TaskExecuteResult executeProjectWithFilter(
      Project project, Filter filter, DataArea dataArea) {
    try (Session session = driver.session()) {
      Map<String, Map<String, String>> labelToProperties =
          Neo4jClientUtils.determinePaths(
              session,
              project.getPatterns(),
              project.getTagFilter(),
              dataArea.getStorageUnit(),
              false);

      List<cn.edu.tsinghua.iginx.neo4j.entity.Column> columns = new ArrayList<>();
      for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
        String labelName = entry.getKey();
        Map<String, String> propertyMap = entry.getValue();

        columns.addAll(
            Neo4jClientUtils.query(
                session,
                labelName,
                propertyMap,
                filter.copy(),
                isDummy(labelName),
                dataArea.getStorageUnit()));
      }
      return new TaskExecuteResult(new Neo4jQueryRowStream(columns, filter.copy()), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("Execute project task in neo4j failure : %s", e)));
    }
  }

  private TaskExecuteResult executeProjectDummyWithFilter(Project project, Filter filter) {
    try (Session session = driver.session()) {
      Map<String, Map<String, String>> labelToProperties =
          Neo4jClientUtils.determinePaths(
              session, project.getPatterns(), project.getTagFilter(), "", true);

      List<cn.edu.tsinghua.iginx.neo4j.entity.Column> columns = new ArrayList<>();
      for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
        String labelName = entry.getKey();
        Map<String, String> propertyMap = entry.getValue();

        columns.addAll(
            Neo4jClientUtils.query(
                session, labelName, propertyMap, filter.copy(), isDummy(labelName), ""));
      }
      return new TaskExecuteResult(new Neo4jQueryRowStream(columns, filter.copy()), null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              String.format("Execute project task in neo4j failure : %s", e)));
    }
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeProjectWithFilter(project, select.getFilter(), dataArea);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeProjectDummyWithFilter(project, select.getFilter());
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    try (Session session = driver.session()) {
      List<String> paths = delete.getPatterns();
      TagFilter tagFilter = delete.getTagFilter();
      if (delete.getKeyRanges() == null
          || delete.getKeyRanges().isEmpty()
          || (delete.getKeyRanges().size() == 1
              && delete.getKeyRanges().get(0).getActualBeginKey() == 0
              && delete.getKeyRanges().get(0).getEndKey() == Long.MAX_VALUE)) {
        if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
          Neo4jClientUtils.clearDatabase(session);
        } else {
          Map<String, Map<String, String>> labelToProperties =
              Neo4jClientUtils.determinePaths(
                  session, paths, tagFilter, dataArea.getStorageUnit(), false);
          for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
            Neo4jClientUtils.removeProperties(session, entry.getKey(), entry.getValue().keySet());
          }
        }
      } else {
        Map<String, Map<String, String>> labelToProperties =
            Neo4jClientUtils.determinePaths(
                session, paths, tagFilter, dataArea.getStorageUnit(), false);

        for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
          for (KeyRange keyRange : delete.getKeyRanges()) {
            Neo4jClientUtils.deleteByRange(
                session, entry.getKey(), entry.getValue().keySet(), keyRange);
          }
        }
      }

      return new TaskExecuteResult(null, null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(
          new PhysicalException(String.format("execute delete task in neo4j failure: %s", e)));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String databaseName = dataArea.getStorageUnit();
    try (Session session = driver.session()) {
      DataView dataView = insert.getData();
      Exception e = null;
      switch (dataView.getRawDataType()) {
        case Row:
        case NonAlignedRow:
        case Column:
        case NonAlignedColumn:
          e = insertRecords(session, databaseName, dataView);
          break;
      }
      if (e != null) {
        return new TaskExecuteResult(
            null, new PhysicalException(String.format("execute insert task in neo4j failure"), e));
      }
      return new TaskExecuteResult(null, null);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new TaskExecuteResult(null, null);
    }
  }

  private Exception insertRecords(Session session, String databaseName, DataView data) {
    int batchSize = Math.min(data.getKeySize(), 10000);
    try {
      int cnt = 0;
      int rowIndex[] = new int[data.getPathNum()];
      String[] labelNames = new String[data.getPathNum()];
      String[] propertyNames = new String[data.getPathNum()];
      while (cnt < data.getKeySize()) {
        int size = Math.min(data.getKeySize() - cnt, batchSize);
        Map<String, Map<Long, Map<String, Object>>> labelToRowData =
            new HashMap<>(); // <标签名， <id， <列名，值>>>
        for (int i = cnt; i < cnt + size; i++) {
          int colIndex = 0;
          for (int j = 0; j < data.getPathNum(); j++) {
            if (labelNames[j] == null) {
              String path = data.getPaths().get(j);
              Map<String, String> tags = new HashMap<>();
              if (data.getTagsList() != null && !data.getTagsList().isEmpty()) {
                tags = data.getTagsList().get(j);
              }
              Pair<String, String> labelAndProperty =
                  PathUtils.getLabelAndPropertyByPath(path, tags, false);
              String labelName = labelAndProperty.getK();
              labelNames[j] = labelName;
              propertyNames[j] = labelAndProperty.getV();
            }
            Map<Long, Map<String, Object>> dataMap =
                labelToRowData.computeIfAbsent(labelNames[j], k -> new HashMap<>());
            Object obj;
            if (data instanceof RowDataView) {
              if (data.getBitmapView(i).get(j)) {
                obj = data.getValue(i, colIndex++);
              } else {
                obj = null;
              }
            } else {
              if (data.getBitmapView(j).get(i)) {
                obj = data.getValue(j, rowIndex[j]++);
              } else {
                obj = null;
              }
            }

            if (obj != null) {
              Map<String, Object> row =
                  dataMap.computeIfAbsent(
                      data.getKey(i),
                      k -> {
                        Map r = new HashMap<>();
                        r.put(IDENTITY_PROPERTY_NAME, k);
                        return r;
                      });
              row.put(propertyNames[j], fromIginxType(obj));
            }
          }
        }

        for (Map.Entry<String, Map<Long, Map<String, Object>>> labels : labelToRowData.entrySet()) {
          String labelName = labels.getKey();
          if (cnt == 0) {
            Neo4jClientUtils.checkAndCreateUniqueConstraint(
                session, databaseName + "." + labelName, IDENTITY_PROPERTY_NAME);
          }

          Map<Long, Map<String, Object>> dataMap = labels.getValue();
          Collection<Map<String, Object>> dataList = dataMap.values();
          Neo4jClientUtils.bulkInsert(
              session, databaseName + "." + labelName, IDENTITY_PROPERTY_NAME, dataList);
        }
        cnt += size;
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return e;
    }
    return null;
  }

  @Override
  public Flowable<Column> getColumns(Set<String> patternSet, TagFilter tagFilter)
      throws PhysicalException {
    try (Session session = driver.session()) {
      Map<String, Map<String, String>> labelToProperties =
          Neo4jClientUtils.determinePaths(session, patternSet, tagFilter, "", false);
      List<Column> columns = new ArrayList<>();

      for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
        for (Map.Entry<String, String> property : entry.getValue().entrySet()) {
          Pair<String, Map<String, String>> pair =
              splitFullName(entry.getKey() + SEPARATOR + property.getKey());
          Column column =
              new Column(
                  trimPrefix(pair.getK()),
                  fromStringDataType(property.getValue()),
                  pair.getV(),
                  isDummy(entry.getKey()));
          columns.add(column);
        }
      }

      return Flowable.fromIterable(columns);
    } catch (Exception e) {
      throw new PhysicalException(String.format("execute query task in neo4j failure"));
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    try (Session session = driver.session()) {
      ColumnsInterval columnsInterval;
      TreeSet<String> paths = new TreeSet<>();
      Map<String, Map<String, String>> labelToProperties =
          Neo4jClientUtils.determinePaths(session, null, null, "", false);

      for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
        for (Map.Entry<String, String> property : entry.getValue().entrySet()) {
          String path = trimPrefix(entry.getKey() + SEPARATOR + property.getKey());
          if (org.apache.commons.lang3.StringUtils.isNotEmpty(prefix)
              && org.apache.commons.lang3.StringUtils.isNotEmpty(path)
              && !path.startsWith(prefix)) {
            continue;
          }

          Pair<String, Map<String, String>> pair = splitFullName(path);
          paths.add(pair.getK());
        }
      }

      if (paths.isEmpty()) {
        throw new PhysicalException("no data!");
      }

      if (prefix != null) {
        columnsInterval = new ColumnsInterval(prefix);
      } else {
        columnsInterval = new ColumnsInterval(paths.first(), StringUtils.nextString(paths.last()));
      }

      return new Pair<>(columnsInterval, KeyInterval.getDefaultKeyInterval());
    } catch (Exception e) {
      throw new PhysicalException(String.format("execute query task in neo4j failure : %s", e));
    }
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectWithAgg(Project project, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAgg(
      Project project, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public void release() throws PhysicalException {
    LOGGER.info("close neo4j client pool ...");
    driver.close();
  }
}

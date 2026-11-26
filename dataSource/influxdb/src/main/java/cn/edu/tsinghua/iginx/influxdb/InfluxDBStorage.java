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
package cn.edu.tsinghua.iginx.influxdb;

import static cn.edu.tsinghua.iginx.influxdb.tools.TimeUtils.instantToNs;
import static com.influxdb.client.domain.WritePrecision.NS;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilterType;
import cn.edu.tsinghua.iginx.influxdb.exception.InfluxDBException;
import cn.edu.tsinghua.iginx.influxdb.exception.InfluxDBTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBQueryRowStream;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
import cn.edu.tsinghua.iginx.influxdb.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.influxdb.tools.FluxUtils;
import cn.edu.tsinghua.iginx.influxdb.tools.SchemaTransformer;
import cn.edu.tsinghua.iginx.influxdb.tools.TagFilterUtils;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.influxdb.client.BucketsQuery;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.NotFoundException;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import io.reactivex.rxjava3.core.Flowable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBStorage.class);

  private static final WritePrecision WRITE_PRECISION = NS;

  private static final String MEASUREMENTALL = "~ /.*/";

  private static final String FIELDALL = "~ /.+/";

  private static final String QUERY_DATA =
      "from(bucket:\"%s\") |> range(start: time(v: %s), stop: time(v: %s))";

  private static final String QUERY_DATA_ALL =
      "from(bucket:\"%s\") |> range(start: time(v: %s), stop: time(v: %s)) |> filter(fn: (r) => (r._measurement =%s and r._field =%s))";

  private static final String DELETE_MEASUREMENT = "_measurement=\"%s\"";

  private static final String DELETE_ALL_MEASUREMENTS = "";

  // Nanosecond timestamps accepted by the InfluxDB delete API; both bounds are inclusive there.
  private static final long MIN_DELETE_KEY = Long.MIN_VALUE + 2;

  private static final long MAX_DELETE_KEY = Long.MAX_VALUE - 1;

  // the largest page the InfluxDB bucket API accepts
  private static final int BUCKET_PAGE_SIZE = 100;

  private static final String SHOW_TIME_SERIES =
      "from(bucket:\"%s\") |> range(start: time(v: 0), stop: time(v: 9223372036854775807)) |> filter(fn: (r) => (r._measurement =~ /.*/ and r._field =~ /.+/)) |> first()";

  private static final String SHOW_TIME_SERIES_BY_PATTERN =
      "from(bucket:\"%s\") |> range(start: time(v: 0), stop: time(v: 9223372036854775807)) |> filter(fn: (r) => (r._measurement =~ /%s/ and r._field =~ /%s/)) |> first()";

  private final StorageEngineMeta meta;

  private final InfluxDBClient client;

  private final String organizationName;

  private final Organization organization;

  private final Map<String, Bucket> bucketMap = new ConcurrentHashMap<>();

  private final Map<String, Bucket> historyBucketMap = new ConcurrentHashMap<>();

  public InfluxDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    if (!meta.getStorageEngine().equals(StorageEngineType.influxdb)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    if (!testConnection(this.meta)) {
      throw new StorageInitializationException("cannot connect to " + meta);
    }
    Map<String, String> extraParams = meta.getExtraParams();
    String url = extraParams.getOrDefault("url", "http://localhost:8086/");
    OkHttpClient.Builder builder =
        new OkHttpClient.Builder().retryOnConnectionFailure(true).readTimeout(10, TimeUnit.MINUTES);
    InfluxDBClientOptions options =
        InfluxDBClientOptions.builder()
            .url(url)
            .authenticateToken(extraParams.get("token").toCharArray())
            .okHttpClient(builder)
            .build();
    client = InfluxDBClientFactory.create(options);
    organizationName = extraParams.get("organization");
    organization =
        client.getOrganizationsApi().findOrganizations().stream()
            .filter(o -> o.getName().equals(this.organizationName))
            .findFirst()
            .orElseThrow(IllegalStateException::new);
    if (meta.isHasData()) {
      reloadHistoryData();
    }
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    LOGGER.debug("testing influxdb {}", extraParams.toString());
    String url = extraParams.get("url");
    try (InfluxDBClient client =
        InfluxDBClientFactory.create(url, extraParams.get("token").toCharArray())) {
      LOGGER.debug("start testing");
      if (client.ping()) {
        LOGGER.debug("influxdb connection success:{}", meta);
        return true;
      } else {
        LOGGER.error("influxdb connection failed:{}", meta);
        return false;
      }
    } catch (Exception e) {
      LOGGER.error("test connection error: ", e);
      return false;
    }
  }

  private void reloadHistoryData() {
    List<Bucket> buckets = findAllBucketsInOrg();
    for (Bucket bucket : buckets) {
      if (bucket.getType() == Bucket.TypeEnum.SYSTEM) {
        continue;
      }
      if (bucket.getName().startsWith("unit")) {
        continue;
      }
      LOGGER.debug("history data bucket info: {}", bucket);
      historyBucketMap.put(bucket.getName(), bucket);
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    List<String> bucketNames = new ArrayList<>(historyBucketMap.keySet());
    if (bucketNames.isEmpty()) {
      throw new InfluxDBTaskExecuteFailureException("InfluxDB has no bucket!");
    }
    // 筛选出符合dataPrefix的bucket，
    if (dataPrefix != null) {
      bucketNames =
          bucketNames.stream()
              .filter(
                  bucketName ->
                      bucketName.startsWith(dataPrefix) || dataPrefix.startsWith(bucketName))
              .collect(Collectors.toList());
    }
    bucketNames.sort(String::compareTo);
    ColumnsInterval columnsInterval;

    // 只需要查询首尾两端的记录即可
    String minPath = findExtremeRecordPath(bucketNames, dataPrefix, true);
    String maxPath = findExtremeRecordPath(bucketNames, dataPrefix, false) + "~";

    if (minPath == null || maxPath == null) {
      throw new InfluxDBTaskExecuteFailureException(
          "InfluxDB has no valid data! Maybe there are no data in each bucket or no data with the given data prefix!");
    }

    KeyInterval keyInterval = new KeyInterval(Long.MIN_VALUE + 1, Long.MAX_VALUE);
    columnsInterval = new ColumnsInterval(minPath, maxPath);
    return new Pair<>(columnsInterval, keyInterval);
  }

  private String findExtremeRecordPath(
      List<String> bucketNames, String dataPrefix, boolean findMin) {
    String extremePath = null;
    int startIndex = findMin ? 0 : bucketNames.size() - 1;
    int endIndex = findMin ? bucketNames.size() : -1;
    int step = findMin ? 1 : -1;

    for (int i = startIndex; i != endIndex; i += step) {
      String bucketName = bucketNames.get(i);
      String statement = String.format(SHOW_TIME_SERIES, FluxUtils.escapeStringLiteral(bucketName));
      List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
      if (tables.isEmpty()) {
        continue;
      }
      for (FluxTable fluxTable : tables) {
        // 找到measurement和field
        String measurement = fluxTable.getRecords().get(0).getMeasurement();
        String field = fluxTable.getRecords().get(0).getField();
        String path = bucketName + "." + measurement + "." + field;
        if (dataPrefix != null && !path.startsWith(dataPrefix)) {
          continue;
        }
        if (extremePath == null
            || (findMin ? path.compareTo(extremePath) < 0 : path.compareTo(extremePath) > 0)) {
          extremePath = path;
        }
      }
      break;
    }
    return extremePath;
  }

  @Override
  public Flowable<Column> getColumns(Set<String> patterns, TagFilter tagFilter) {
    List<Column> timeseries = new ArrayList<>();
    for (Bucket bucket : findAllBucketsInOrg()) {
      // query all the series by querying all the data with first()

      String unitPattern = "unit\\d{10}"; // unit后跟10个数字
      boolean isUnit = bucket.getName().matches(unitPattern);
      boolean isDummy =
          meta.isHasData()
              && (meta.getDataPrefix() == null
                  || bucket
                      .getName()
                      .startsWith(
                          meta.getDataPrefix().substring(0, meta.getDataPrefix().indexOf("."))));
      if (bucket.getType() == Bucket.TypeEnum.SYSTEM || (!isUnit && !isDummy)) {
        continue;
      }

      List<FluxTable> tables = new ArrayList<>();
      String measPattern, fieldPattern, statement, bucketPattern;
      // <measurementPattern, fieldPattern>
      List<Pair<String, String>> patternPairs = new ArrayList<>();

      if (patterns == null
          || patterns.size() == 0
          || patterns.contains("*")
          || patterns.contains("*.*")) {
        statement =
            String.format(SHOW_TIME_SERIES, FluxUtils.escapeStringLiteral(bucket.getName()));
        tables = client.getQueryApi().query(statement, organization.getId());
      } else {
        boolean thisBucketIsQueried = false;
        for (String p : patterns) {
          if (isDummy && !isUnit) {
            bucketPattern = p.substring(0, p.indexOf("."));
            // dummy path starts with <bucketName>.
            if (!Pattern.matches(StringUtils.reformatPath(bucketPattern), bucket.getName())) {
              continue;
            }
            // * can match multiple layers.
            if (p.startsWith("*.")) {
              // match one layer first
              p = p.substring(2);
              if (p.contains(".")) {
                // pattern *.xx.xx
                patternPairs.add(
                    new Pair<>(
                        StringUtils.reformatPath(p.substring(0, p.indexOf("."))),
                        StringUtils.reformatPath(p.substring(p.indexOf(".") + 1))));
              }
              // match multiple layers later.
              p = "*.*." + p;
            }
            // remove <bucketName>. part from pattern
            p = p.substring(p.indexOf(".") + 1);
          }
          thisBucketIsQueried = true;

          if (p.startsWith("*.")) {
            // match one layer first
            patternPairs.add(
                new Pair<>(
                    StringUtils.reformatPath(p.substring(0, p.indexOf("."))),
                    StringUtils.reformatPath(p.substring(p.indexOf(".") + 1))));
            // match multiple layers
            patternPairs.add(
                new Pair<>(StringUtils.reformatPath("*"), StringUtils.reformatPath(p)));
          } else if (p.equals("*")) {
            patternPairs.add(
                new Pair<>(StringUtils.reformatPath("*"), StringUtils.reformatPath("*")));
          } else if (p.contains(".")) {
            patternPairs.add(
                new Pair<>(
                    StringUtils.reformatPath(p.substring(0, p.indexOf("."))),
                    StringUtils.reformatPath(p.substring(p.indexOf(".") + 1))));
          }
          for (Pair<String, String> patternPair : patternPairs) {
            measPattern = patternPair.k;
            fieldPattern = patternPair.v;
            // query time series based on pattern
            statement =
                String.format(
                    SHOW_TIME_SERIES_BY_PATTERN,
                    FluxUtils.escapeStringLiteral(bucket.getName()),
                    measPattern,
                    fieldPattern);
            LOGGER.info("executing column query: {}", statement);
            tables.addAll(client.getQueryApi().query(statement, organization.getId()));
          }
        }
        // if bucket is dummy && all patterns do not match(<bucketName>.*), move on to next bucket
        if (!thisBucketIsQueried) {
          continue;
        }
      }

      for (FluxTable table : tables) {
        List<FluxColumn> column = table.getColumns();
        // get the path
        String path =
            table.getRecords().get(0).getMeasurement() + "." + table.getRecords().get(0).getField();
        Map<String, String> tag = new HashMap<>();
        int len = column.size();
        // get the tag cause the 8 is the begin index of the tag information
        for (int i = 8; i < len; i++) {
          String key = column.get(i).getLabel();
          String val = (String) table.getRecords().get(0).getValues().get(key);
          tag.put(key, val);
        }
        if (isDummy && !isUnit) {
          path = bucket.getName() + "." + path;
        }
        // get columns by tag filter
        if (tagFilter != null && !TagKVUtils.match(tag, tagFilter)) {
          continue;
        }

        DataType dataType;
        switch (column.get(5).getDataType()) { // the index 1 is the type of the data
          case "boolean":
            dataType = DataType.BOOLEAN;
            break;
          case "float":
            dataType = DataType.FLOAT;
            break;
          case "string":
            dataType = DataType.BINARY;
            break;
          case "double":
            dataType = DataType.DOUBLE;
            break;
          case "int":
            dataType = DataType.INTEGER;
            break;
          case "long":
            dataType = DataType.LONG;
            break;
          default:
            dataType = DataType.BINARY;
            LOGGER.warn("DataType don't match and default is String");
            break;
        }
        timeseries.add(new Column(path, dataType, tag, isDummy));
      }
    }

    return Flowable.fromIterable(timeseries);
  }

  @Override
  public void release() {
    client.close();
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter = select.getFilter();

    if (findBucketInOrg(storageUnit) == null) {
      LOGGER.warn("storage engine {} doesn't exist", storageUnit);
      return new TaskExecuteResult(
          new InfluxDBQueryRowStream(Collections.emptyList(), project, filter));
    }

    String statement =
        generateQueryStatement(
            storageUnit,
            project.getPatterns(),
            project.getTagFilter(),
            filter,
            keyInterval.getStartKey(),
            keyInterval.getEndKey(),
            false);

    List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
    return buildQueryResult(tables, project, filter, new ArrayList<>());
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Map<String, String> bucketQueries = new HashMap<>();
    TagFilter tagFilter = project.getTagFilter();
    Filter filter = select.getFilter();
    getBucketQueriesForExecuteDummy(project, bucketQueries, tagFilter);

    List<FluxTable> tables = new ArrayList<>();
    List<String> BucketNames = new ArrayList<>();
    for (String bucket : bucketQueries.keySet()) {
      String statement =
          generateQueryStatement(
              bucket,
              project.getPatterns(),
              project.getTagFilter(),
              cutBucketForDummyFilter(filter.copy()),
              keyInterval.getStartKey(),
              keyInterval.getEndKey(),
              true);

      LOGGER.info("execute query: {}", statement);
      for (FluxTable table : client.getQueryApi().query(statement, organization.getId())) {
        tables.add(table);
        BucketNames.add(bucket);
      }
    }

    return buildQueryResult(tables, project, filter, BucketNames);
  }

  @Override
  public TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
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
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    KeyInterval keyInterval = dataArea.getKeyInterval();

    if (findBucketInOrg(storageUnit) == null) {
      LOGGER.warn("storage engine {} doesn't exist", storageUnit);
      return new TaskExecuteResult(
          new InfluxDBQueryRowStream(Collections.emptyList(), project, null));
    }

    String statement =
        generateQueryStatement(
            storageUnit,
            project.getPatterns(),
            project.getTagFilter(),
            null,
            keyInterval.getStartKey(),
            keyInterval.getEndKey(),
            false);

    List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
    return buildQueryResult(tables, project, null, new ArrayList<>());
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Map<String, String> bucketQueries = new HashMap<>();
    TagFilter tagFilter = project.getTagFilter();
    getBucketQueriesForExecuteDummy(project, bucketQueries, tagFilter);

    long startKey = keyInterval.getStartKey();
    long endKey = keyInterval.getEndKey();

    List<FluxTable> tables = new ArrayList<>();
    List<String> BucketNames = new ArrayList<>();
    for (String bucket : bucketQueries.keySet()) {
      String statement =
          String.format(
              "from(bucket:\"%s\") |> range(start: time(v: %s), stop: time(v: %s))",
              FluxUtils.escapeStringLiteral(bucket), startKey, endKey);
      if (!bucketQueries.get(bucket).equals("()")) {
        statement += String.format(" |> filter(fn: (r) => %s)", bucketQueries.get(bucket));
      }
      LOGGER.info("execute query: {}", statement);
      for (FluxTable table : client.getQueryApi().query(statement, organization.getId())) {
        tables.add(table);
        BucketNames.add(bucket);
      }
    }

    return buildQueryResult(tables, project, null, BucketNames);
  }

  private void getBucketQueriesForExecuteDummy(
      Project project, Map<String, String> bucketQueries, TagFilter tagFilter) {
    for (String pattern : project.getPatterns()) {
      Pair<String, String> pair = SchemaTransformer.processPatternForQuery(pattern, tagFilter);
      String bucketName = pair.k;
      String query = pair.v;
      List<String> bucketNameList = new ArrayList<>();

      if (bucketName.equals("*")) {
        // 通配符需要特殊判断，InfluxDB无法在bucket name上使用正则表达式匹配，并且仅查询type为user的bucket
        List<Bucket> buckets = findAllBucketsInOrg();
        for (Bucket bucket : buckets) {
          if (bucket.getType() == Bucket.TypeEnum.USER) {
            bucketNameList.add(bucket.getName());
          }
        }
      } else if (findBucketInOrg(bucketName) == null) {
        LOGGER.warn("storage engine {} doesn't exist", bucketName);
        continue;
      } else {
        bucketNameList.add(bucketName);
      }

      for (String bucket : bucketNameList) {
        String fullQuery = "";
        if (bucketQueries.containsKey(bucket)) {
          fullQuery = bucketQueries.get(bucket);
          fullQuery += " or ";
        }
        fullQuery += query;
        bucketQueries.put(bucket, fullQuery);
      }
    }
  }

  private String generateQueryStatement(
      String bucketName,
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      long startTime,
      long endTime,
      boolean isDummy) {
    String statement =
        String.format(QUERY_DATA, FluxUtils.escapeStringLiteral(bucketName), startTime, endTime);
    if (paths.size() != 1 || !paths.get(0).equals("*")) {
      StringBuilder filterStr = new StringBuilder(" |> filter(fn: (r) => ");
      filterStr.append('('); // make the or statement together
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        if (isDummy && path.indexOf('.') == path.lastIndexOf('.')) continue;
        InfluxDBSchema schema = new InfluxDBSchema(path, null, isDummy);
        if (i != 0) {
          filterStr.append(" or ");
        }
        filterStr.append('(');

        String measurement = schema.getMeasurement();
        if (measurement.contains("*")) {
          String safeRegex = FluxUtils.escapeRegexWithWildcard(measurement);
          filterStr.append("r._measurement =~ /").append(safeRegex).append("/");
        } else {
          // 精确匹配：使用 FluxUtils.escapeStringLiteral 处理双引号
          filterStr
              .append("r._measurement == \"")
              .append(FluxUtils.escapeStringLiteral(measurement))
              .append("\"");
        }

        String field = schema.getField();
        field = FluxUtils.escapeRegexWithWildcard(field);
        filterStr.append(" and ");
        filterStr.append("r._field =~ /").append(field).append("/");

        Map<String, String> tags = schema.getTags();
        if (!tags.isEmpty()) {
          filterStr.append(" and ");
          assert tags.size() == 1;
          String key = InfluxDBSchema.TAG;
          String value = tags.get(key);

          if (value.contains("*")) {
            // 处理 * 为 .*，并转义其他正则特殊字符（如括号、加号等）
            String regexValue = FluxUtils.escapeRegexWithWildcard(value);
            filterStr.append("r.").append(key).append(" =~ /");
            filterStr.append(regexValue);
            filterStr.append("/");
          } else {
            // 精确匹配：使用 FluxUtils.escapeStringLiteral
            filterStr.append("r.").append(key).append(" == \"");
            filterStr.append(FluxUtils.escapeStringLiteral(value));
            filterStr.append("\"");
          }
        }

        filterStr.append(')');
      }
      filterStr.append(')');
      if (tagFilter != null && tagFilter.getType() != TagFilterType.WithoutTag) {
        filterStr.append(" and ").append(TagFilterUtils.transformToFilterStr(tagFilter));
      }
      filterStr.append(')');
      statement += filterStr;
    }

    // TODO：filter中的path有多个tag时暂未实现下推
    List<String> filterPaths = FilterUtils.getAllPathsFromFilter(filter);
    List<Column> columns = getColumns(new HashSet<>(filterPaths), tagFilter).toList().blockingGet();
    boolean hasMultiTags = hasMultiTags(columns);

    if (filter != null && !hasMultiTags) {
      boolean patternHasMeasurementWildCards = false;
      for (String path : paths) {
        if (path.startsWith("*")) {
          patternHasMeasurementWildCards = true;
          break;
        }
      }

      Map<String, List<String>> measurementToFieldsMap = getMeasurementToFields(bucketName);
      // pivot、union的结果不一定会按照时间顺序排列，需要增加一个sort操作
      String pivotFormat =
          " |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")"
              + " %s"
              + " |> sort(columns: [\"_time\"], desc: false)";
      if (filterHasMeasurementWildCards(filter) || patternHasMeasurementWildCards) {
        String prefix = statement;
        StringBuilder statementBuilder = new StringBuilder();
        int index = 0;
        for (Map.Entry<String, List<String>> entry : measurementToFieldsMap.entrySet()) {
          String measurement = entry.getKey();
          String safeMeasurementRegex = FluxUtils.escapeRegexWithWildcard(measurement);
          String pivotStr =
              String.format(
                  pivotFormat,
                  generateFilterStatement(bucketName, measurement, filter, measurementToFieldsMap));
          statementBuilder
              .append("t")
              .append(index)
              .append(" = ")
              .append(
                  prefix.replace(
                      "r._measurement =~ /.+/", "r._measurement =~ /" + safeMeasurementRegex + "/"))
              .append(pivotStr)
              .append("\n");
          index++;
        }
        if (index != 1) {
          statementBuilder.append("union(tables: [");
          for (int i = 0; i < index; i++) {
            statementBuilder.append("t").append(i);
            if (i != index - 1) {
              statementBuilder.append(", ");
            }
          }
          statementBuilder.append("])");
        } else {
          statementBuilder.delete(0, 5); // 删掉开头的"t0 = "
        }
        statement = statementBuilder.toString();

      } else {
        String pivotStr =
            String.format(
                pivotFormat,
                generateFilterStatement(bucketName, null, filter, measurementToFieldsMap));

        statement += pivotStr;
      }
    }

    LOGGER.info("generate query: {}", statement);
    return statement;
  }

  private boolean hasMultiTags(List<Column> columns) {
    Map<String, List<Map<String, String>>> tagsMap = new HashMap<>();
    for (Column column : columns) {
      String path = column.getPath();
      if (!tagsMap.containsKey(path)) {
        tagsMap.put(path, new ArrayList<>(Collections.singletonList(column.getTags())));
      } else if (!tagsMap.get(path).contains(column.getTags())) {
        return true;
      }
    }
    return false;
  }

  private Filter cutBucketForDummyFilter(Filter filter) {
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {
            String path = filter.getPath();
            filter.setPath(path.substring(path.indexOf(".") + 1));
          }

          @Override
          public void visit(PathFilter filter) {
            String pathA = filter.getPathA();
            String pathB = filter.getPathB();
            filter.setPathA(pathA.substring(pathA.indexOf(".") + 1));
            filter.setPathB(pathB.substring(pathB.indexOf(".") + 1));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            List<Expression> expressions = new ArrayList<>();
            expressions.add(filter.getExpressionA());
            expressions.add(filter.getExpressionB());
            for (Expression expr : expressions) {
              expr.accept(
                  new ExpressionVisitor() {
                    @Override
                    public void visit(BaseExpression expression) {
                      String path = expression.getPathName();
                      expression.setPathName(path.substring(path.indexOf(".") + 1));
                    }

                    @Override
                    public void visit(BinaryExpression expression) {}

                    @Override
                    public void visit(BracketExpression expression) {}

                    @Override
                    public void visit(ConstantExpression expression) {}

                    @Override
                    public void visit(FromValueExpression expression) {}

                    @Override
                    public void visit(FuncExpression expression) {}

                    @Override
                    public void visit(MultipleExpression expression) {}

                    @Override
                    public void visit(UnaryExpression expression) {}

                    @Override
                    public void visit(CaseWhenExpression expression) {}

                    @Override
                    public void visit(KeyExpression expression) {}

                    @Override
                    public void visit(SequenceExpression expression) {}
                  });
            }
          }

          @Override
          public void visit(InFilter filter) {
            String path = filter.getPath();
            filter.setPath(path.substring(path.indexOf(".") + 1));
          }
        });
    return filter;
  }

  private Filter setTrueByMeasurement(Filter filter, String measurementName) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        andChildren.replaceAll(child -> setTrueByMeasurement(child, measurementName));
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        orChildren.replaceAll(child -> setTrueByMeasurement(child, measurementName));
        return new OrFilter(orChildren);
      case Not:
        return new NotFilter(
            setTrueByMeasurement(((NotFilter) filter).getChild(), measurementName));
      case Value:
        String path = ((ValueFilter) filter).getPath();
        InfluxDBSchema schema = new InfluxDBSchema(path);
        if (!schema.getMeasurement().equals(measurementName)) {
          return new BoolFilter(true);
        }
        break;
      case In:
        String inPath = ((InFilter) filter).getPath();
        InfluxDBSchema inSchema = new InfluxDBSchema(inPath);
        if (!inSchema.getMeasurement().equals(measurementName)) {
          return new BoolFilter(true);
        }
        break;
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        InfluxDBSchema schemaA = new InfluxDBSchema(pathA);
        InfluxDBSchema schemaB = new InfluxDBSchema(pathB);
        if (!schemaA.getMeasurement().equals(measurementName)
            || !schemaB.getMeasurement().equals(measurementName)) {
          return new BoolFilter(true);
        }
      default:
        break;
    }
    return filter;
  }

  private boolean filterHasMeasurementWildCards(Filter filter) {
    boolean res = false;
    switch (filter.getType()) {
      case And:
        for (Filter child : ((AndFilter) filter).getChildren()) {
          res = filterHasMeasurementWildCards(child);
          if (res) {
            break;
          }
        }
        break;
      case Or:
        for (Filter child : ((OrFilter) filter).getChildren()) {
          res = filterHasMeasurementWildCards(child);
          if (res) {
            break;
          }
        }
        break;
      case Not:
        res = filterHasMeasurementWildCards(((NotFilter) filter).getChild());
        break;
      case Value:
        if (((ValueFilter) filter).getPath().startsWith("*")) {
          res = true;
        }
        break;
      case In:
        if (((InFilter) filter).getPath().startsWith("*")) {
          res = true;
        }
        break;
      case Path:
        if (((PathFilter) filter).getPathA().startsWith("*")
            || ((PathFilter) filter).getPathB().startsWith("*")) {
          res = true;
        }
        break;
      default:
        break;
    }
    return res;
  }

  /** 获取给定Filter中所有带有通配符 * 的path，将其填入参数中的map中。 */
  private void getAllPathFromFilterWithWildCards(Filter filter, Map<String, List<String>> map) {
    if (filter == null) {
      return;
    }

    switch (filter.getType()) {
      case And:
        if (filter instanceof AndFilter) {
          ((AndFilter) filter)
              .getChildren()
              .forEach(child -> getAllPathFromFilterWithWildCards(child, map));
        }
      case Or:
        if (filter instanceof OrFilter) {
          ((OrFilter) filter)
              .getChildren()
              .forEach(child -> getAllPathFromFilterWithWildCards(child, map));
        }
      case Not:
        if (filter instanceof NotFilter) {
          getAllPathFromFilterWithWildCards(((NotFilter) filter).getChild(), map);
        }
      case Value:
        if (filter instanceof ValueFilter) {
          String path = ((ValueFilter) filter).getPath();
          if (path.contains("*") && !map.containsKey(path)) {
            map.put(path, null);
          }
        }
      case Path:
        if (filter instanceof PathFilter) {
          String pathA = ((PathFilter) filter).getPathA();
          String pathB = ((PathFilter) filter).getPathB();
          if (pathA.contains("*") && !map.containsKey(pathA)) {
            map.put(pathA, null);
          }
          if (pathB.contains("*") && !map.containsKey(pathB)) {
            map.put(pathB, null);
          }
        }
      case In:
        if (filter instanceof InFilter) {
          String path = ((InFilter) filter).getPath();
          if (path.contains("*") && !map.containsKey(path)) {
            map.put(path, null);
          }
        }
      case Key:
      default:
        return;
    }
  }

  private Filter generateFilterByWildCardEntry(
      Filter filter, Map.Entry<String, List<String>> entry) {
    String wildcardsPath = entry.getKey();
    InfluxDBSchema schema = new InfluxDBSchema(wildcardsPath);
    String measurement = schema.getMeasurement();

    List<String> matchedPaths = entry.getValue();

    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = new ArrayList<>(((AndFilter) filter).getChildren());
        for (Filter child : andChildren) {
          Filter newChild = generateFilterByWildCardEntry(child, entry);
          andChildren.set(andChildren.indexOf(child), newChild);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = new ArrayList<>(((OrFilter) filter).getChildren());
        for (Filter child : orChildren) {
          Filter newChild = generateFilterByWildCardEntry(child, entry);
          orChildren.set(orChildren.indexOf(child), newChild);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        return generateFilterByWildCardEntry(notChild, entry);
      case In:
        InFilter inFilter = (InFilter) filter;
        InFilter.InOp inOp = inFilter.getInOp();
        if (inFilter.getPath().equals(wildcardsPath)) {
          if (matchedPaths == null) {
            return new BoolFilter(true);
          }

          List<Filter> newInValueChildren = new ArrayList<>();
          for (String matchedPath : matchedPaths) {
            InFilter newInFilter =
                new InFilter(measurement + "." + matchedPath, inOp, inFilter.getValues());
            newInValueChildren.add(newInFilter);
          }
          if (newInValueChildren.size() == 1) {
            return newInValueChildren.get(0);
          }

          if (!inOp.isOrOp()) {
            return new AndFilter(newInValueChildren);
          }
          return new OrFilter(newInValueChildren);
        }
        break;
      case Value:
        ValueFilter valueFilter = (ValueFilter) filter;
        if (valueFilter.getPath().equals(wildcardsPath)) {
          if (matchedPaths == null) {
            return new BoolFilter(true);
          }

          List<Filter> newValueChildren = new ArrayList<>();
          for (String matchedPath : matchedPaths) {
            ValueFilter newValueFilter =
                new ValueFilter(
                    measurement + "." + matchedPath, valueFilter.getOp(), valueFilter.getValue());
            newValueChildren.add(newValueFilter);
          }
          if (newValueChildren.size() == 1) {
            return newValueChildren.get(0);
          }

          if (Op.isAndOp(valueFilter.getOp())) {
            return new AndFilter(newValueChildren);
          }

          return new OrFilter(newValueChildren);
        }
        break;
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        String pathA = pathFilter.getPathA();
        String pathB = pathFilter.getPathB();
        if (pathA.equals(wildcardsPath)) {
          if (matchedPaths == null) {
            return new BoolFilter(true);
          }

          List<Filter> newValueChildren = new ArrayList<>();
          for (String matchedPath : matchedPaths) {
            PathFilter newPathFilter =
                new PathFilter(measurement + "." + matchedPath, pathFilter.getOp(), pathB);
            newValueChildren.add(newPathFilter);
          }
          if (newValueChildren.size() == 1) {
            return newValueChildren.get(0);
          }

          if (Op.isAndOp(pathFilter.getOp())) {
            return new AndFilter(newValueChildren);
          }

          return new OrFilter(newValueChildren);
        }

        if (pathB.equals(wildcardsPath)) {
          if (matchedPaths == null) {
            return new BoolFilter(true);
          }

          // 如果filter已经不是PathFilter了，说明在PathA时已经被修改成了AndFilter或OrFilter,需要继续调用函数递归
          if (filter.getType() != FilterType.Path) {
            generateFilterByWildCardEntry(pathFilter, entry);
          } else {
            List<Filter> newValueChildren = new ArrayList<>();
            for (String matchedPath : matchedPaths) {
              PathFilter newPathFilter =
                  new PathFilter(pathA, pathFilter.getOp(), measurement + "." + matchedPath);
              newValueChildren.add(newPathFilter);
            }
            if (newValueChildren.size() == 1) {
              return newValueChildren.get(0);
            }

            if (Op.isAndOp(pathFilter.getOp())) {
              return new AndFilter(newValueChildren);
            }
            return new OrFilter(newValueChildren);
          }
        }
        break;
      case Key:
      default:
        break;
    }
    return filter;
  }

  private String generateFilterStatement(
      String bucketName,
      String measurementName,
      Filter filter,
      Map<String, List<String>> measurementToFieldsMap) {
    if (filter == null) {
      return "";
    }

    String filterStr = FilterTransformer.toString(filter);

    // 检查语句中是否存在*通配符，如果存在需要手动解析
    if (filter.toString().contains("*")) {
      Map<String, List<String>> fieldMap = new HashMap<>();
      getAllPathFromFilterWithWildCards(filter, fieldMap);

      if (!fieldMap.isEmpty()) {
        for (Map.Entry<String, List<String>> mtfEntry : measurementToFieldsMap.entrySet()) {
          if (measurementName != null && !measurementName.equals(mtfEntry.getKey())) {
            continue;
          }
          String tableMeasurement = mtfEntry.getKey();
          List<String> tableFields = mtfEntry.getValue();
          for (String tableField : tableFields) {
            for (Map.Entry<String, List<String>> entry : fieldMap.entrySet()) {
              String path = entry.getKey();
              InfluxDBSchema schema = new InfluxDBSchema(path, null);
              String measurement = schema.getMeasurement();
              String field = schema.getField();
              if (measurement.equals(tableMeasurement) || measurement.equals("*")) {
                List<String> fields = entry.getValue();
                String fieldRegex = "^" + StringUtils.reformatPath(field) + "$";
                if (Pattern.matches(fieldRegex, tableField)) {
                  if (fields == null) {
                    fields = new ArrayList<>();
                  }
                  fields.add(tableField);
                  entry.setValue(fields);
                }
              }
            }
          }
        }

        // 根据通配符对应的字段生成filter语句
        Filter matchFilter = filter.copy();
        for (Map.Entry<String, List<String>> entry : fieldMap.entrySet()) {
          matchFilter = generateFilterByWildCardEntry(matchFilter, entry);
        }

        if (measurementName != null) {
          matchFilter = setTrueByMeasurement(matchFilter, measurementName);
        }

        matchFilter = LogicalFilterUtils.mergeTrue(matchFilter);

        if (matchFilter.getType() == FilterType.Bool) {
          return "";
        }

        filterStr = FilterTransformer.toString(matchFilter);
      }
    }

    // 没有通配符则直接返回正常拼接的语句
    if (filterStr.isEmpty()) {
      return "";
    }
    return " |> filter(fn: (r) => " + filterStr + ")";
  }

  private Map<String, List<String>> getMeasurementToFields(String bucketName) {
    // 获取所有通配符对应的字段
    String getFirstRowStatement =
        "from(bucket: \""
            + bucketName
            + "\") |> range(start: 0)"
            + "|> filter(fn: (r) => r._measurement =~ /.+/) |> first() ";

    List<FluxTable> firstTableList =
        client.getQueryApi().query(getFirstRowStatement, organization.getId());

    Map<String, List<String>> measurementToFieldsMap = new HashMap<>();
    for (FluxTable table : firstTableList) {
      String tableMeasurement = table.getRecords().get(0).getValueByKey("_measurement").toString();
      String tableField = table.getRecords().get(0).getValueByKey("_field").toString();

      List<String> fields;
      if (measurementToFieldsMap.containsKey(tableMeasurement)) {
        fields = measurementToFieldsMap.get(tableMeasurement);
      } else {
        fields = new ArrayList<>();
      }
      fields.add(tableField);
      measurementToFieldsMap.put(tableMeasurement, fields);
    }

    return measurementToFieldsMap;
  }

  // 将 FluxTable 立即转为 IGinX Table，避免在内存中长期持有 FluxTable 导致的 OOM
  // 由于 FluxTable 包含完整的所有数据，而不是流式读取接口，
  // 所以即便返回 InfluxDBQueryRowStream 也不是真正的流式读取
  private TaskExecuteResult buildQueryResult(
      List<FluxTable> tables, Project project, Filter filter, List<String> bucketNames) {
    try (InfluxDBQueryRowStream rowStream =
        new InfluxDBQueryRowStream(tables, project, filter, bucketNames)) {
      Header header = rowStream.getHeader();
      List<Row> rowList = new ArrayList<>();
      while (rowStream.hasNext()) {
        rowList.add(rowStream.next());
      }
      Table table = new Table(header, rowList);
      return new TaskExecuteResult(table);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    DataView dataView = insert.getData();
    Exception e = null;
    switch (dataView.getRawDataType()) {
      case Row:
      case NonAlignedRow:
        e = insertRowRecords((RowDataView) dataView, storageUnit);
        break;
      case Column:
      case NonAlignedColumn:
        e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
        break;
    }
    if (e != null) {
      return new TaskExecuteResult(
          null, new InfluxDBException("execute insert task in influxdb failure", e));
    }
    return new TaskExecuteResult(null, null);
  }

  private Exception insertRowRecords(RowDataView data, String storageUnit) {
    Bucket bucket = bucketMap.get(storageUnit);
    if (bucket == null) {
      synchronized (this) {
        bucket = bucketMap.get(storageUnit);
        if (bucket == null) {
          bucket = findBucketInOrg(storageUnit);
          if (bucket == null) {
            bucket = client.getBucketsApi().createBucket(storageUnit, organization);
          }
          bucketMap.put(storageUnit, bucket);
        }
      }
    }
    if (bucket == null) {
      return new InfluxDBTaskExecuteFailureException("create bucket failure!");
    }

    List<InfluxDBSchema> schemas = new ArrayList<>();
    for (int i = 0; i < data.getPathNum(); i++) {
      schemas.add(new InfluxDBSchema(data.getPath(i), data.getTags(i)));
    }

    List<Point> points = new ArrayList<>();
    for (int i = 0; i < data.getKeySize(); i++) {
      BitmapView bitmapView = data.getBitmapView(i);
      int index = 0;
      for (int j = 0; j < data.getPathNum(); j++) {
        if (bitmapView.get(j)) {
          InfluxDBSchema schema = schemas.get(j);
          switch (data.getDataType(j)) {
            case BOOLEAN:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (boolean) data.getValue(i, index))
                      .time(data.getKey(i), WRITE_PRECISION));
              break;
            case INTEGER:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (int) data.getValue(i, index))
                      .time(data.getKey(i), WRITE_PRECISION));
              break;
            case LONG:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (long) data.getValue(i, index))
                      .time(data.getKey(i), WRITE_PRECISION));
              break;
            case FLOAT:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (float) data.getValue(i, index))
                      .time(data.getKey(i), WRITE_PRECISION));
              break;
            case DOUBLE:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (double) data.getValue(i, index))
                      .time(data.getKey(i), WRITE_PRECISION));
              break;
            case BINARY:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), new String((byte[]) data.getValue(i, index)))
                      .time(data.getKey(i), WRITE_PRECISION));
              break;
          }

          index++;
        }
      }
    }
    try {
      LOGGER.info("开始数据写入");
      client.getWriteApiBlocking().writePoints(bucket.getId(), organization.getId(), points);
    } catch (Exception e) {
      return new InfluxDBTaskExecuteFailureException(
          "encounter error when write points to influxdb: ", e);
    } finally {
      LOGGER.info("数据写入完毕！");
    }
    return null;
  }

  private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
    Bucket bucket = bucketMap.get(storageUnit);
    if (bucket == null) {
      synchronized (this) {
        bucket = bucketMap.get(storageUnit);
        if (bucket == null) {
          bucket = findBucketInOrg(storageUnit);
          if (bucket == null) {
            bucket = client.getBucketsApi().createBucket(storageUnit, organization);
          }
          bucketMap.put(storageUnit, bucket);
        }
      }
    }
    if (bucket == null) {
      return new InfluxDBTaskExecuteFailureException("create bucket failure!");
    }

    List<Point> points = new ArrayList<>();
    for (int i = 0; i < data.getPathNum(); i++) {
      InfluxDBSchema schema = new InfluxDBSchema(data.getPath(i), data.getTags(i));
      BitmapView bitmapView = data.getBitmapView(i);
      int index = 0;
      for (int j = 0; j < data.getKeySize(); j++) {
        if (bitmapView.get(j)) {
          switch (data.getDataType(i)) {
            case BOOLEAN:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (boolean) data.getValue(i, index))
                      .time(data.getKey(j), WRITE_PRECISION));
              break;
            case INTEGER:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (int) data.getValue(i, index))
                      .time(data.getKey(j), WRITE_PRECISION));
              break;
            case LONG:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (long) data.getValue(i, index))
                      .time(data.getKey(j), WRITE_PRECISION));
              break;
            case FLOAT:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (float) data.getValue(i, index))
                      .time(data.getKey(j), WRITE_PRECISION));
              break;
            case DOUBLE:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), (Number) (double) data.getValue(i, index))
                      .time(data.getKey(j), WRITE_PRECISION));
              break;
            case BINARY:
              points.add(
                  Point.measurement(schema.getMeasurement())
                      .addTags(schema.getTags())
                      .addField(schema.getField(), new String((byte[]) data.getValue(i, index)))
                      .time(data.getKey(j), WRITE_PRECISION));
              break;
          }
          index++;
        }
      }
    }

    try {
      LOGGER.info("开始数据写入");
      client.getWriteApiBlocking().writePoints(bucket.getId(), organization.getId(), points);
    } catch (Exception e) {
      return new InfluxDBTaskExecuteFailureException(
          "encounter error when write points to influxdb: ", e);
    } finally {
      LOGGER.info("数据写入完毕！");
    }

    return null;
  }

  /**
   * 删除一个存储单元（对应 InfluxDB 中的一个 bucket）里的数据。
   *
   * <p>InfluxDB 的删除接口只能按“时间范围 + measurement”删除，无法按 field（即路径第一级之后的部分）删除，也不支持按 tag
   * 过滤条件做精确删除。因此这里只支持两类删除：
   *
   * <ul>
   *   <li>删除全部数据：路径为 "*" 或 "*.*"；
   *   <li>删除整个 measurement：路径形如 "measurement.*"。
   * </ul>
   *
   * 其余请求（如删除某一列 "a.b"、带通配符的 measurement "a*.*"、带 tag 过滤条件）一律报错，宁可拒绝也不多删或少删。
   *
   * <p>整体流程：
   *
   * <ol>
   *   <li>先校验删除请求，得到要删除的 measurement 列表，不支持的请求直接返回错误，不访问 InfluxDB；
   *   <li>查找存储单元对应的 bucket，不存在说明从未写入过数据，直接视为删除成功；
   *   <li>没有 key 范围且删除全部数据时，直接删除整个 bucket；
   *   <li>否则按 measurement 构造删除谓词，对每个 key 范围逐一调用删除接口。
   * </ol>
   */
  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    // 第一步：校验并解析删除路径。返回 null 表示删除全部 measurement，否则为要删除的 measurement 列表
    List<String> measurements;
    try {
      measurements = getDeletedMeasurements(delete.getPatterns(), delete.getTagFilter());
    } catch (InfluxDBTaskExecuteFailureException e) {
      // 不支持的删除请求，在真正删除之前就返回错误，保证不会删掉任何数据
      return new TaskExecuteResult(null, e);
    }

    try {
      // 第二步：查找存储单元对应的 bucket
      Bucket bucket = findBucket(storageUnit);
      if (bucket == null) {
        // bucket 只在第一次写入时创建，不存在说明该存储单元没有数据，无需删除
        return new TaskExecuteResult(null, null);
      }
      List<KeyRange> keyRanges = delete.getKeyRanges();
      if (keyRanges == null || keyRanges.isEmpty()) {
        // 第三步：没有 key 范围，表示删除对应路径下的全部数据（如 DELETE COLUMNS 或删除整个分片）
        if (measurements == null) {
          // 删除全部数据时直接删掉整个 bucket，同时清除缓存，之后再写入时会重新创建 bucket
          bucketMap.remove(storageUnit);
          client.getBucketsApi().deleteBucket(bucket);
          return new TaskExecuteResult(null, null);
        }
        // 只删除部分 measurement 时，用 InfluxDB 允许的最大时间范围来覆盖全部数据
        keyRanges =
            Collections.singletonList(new KeyRange(MIN_DELETE_KEY, true, MAX_DELETE_KEY, true));
      }

      // 第四步：构造删除谓词。空谓词表示删除 bucket 内所有 measurement，
      // 否则每个 measurement 对应一个 _measurement="xxx" 谓词（InfluxDB 删除谓词不支持 OR，只能逐个删除）
      List<String> predicates = new ArrayList<>();
      if (measurements == null) {
        predicates.add(DELETE_ALL_MEASUREMENTS);
      } else {
        for (String measurement : measurements) {
          // 对 measurement 名中的引号、反斜杠等字符转义，避免破坏谓词语法
          predicates.add(
              String.format(DELETE_MEASUREMENT, FluxUtils.escapeStringLiteral(measurement)));
        }
      }
      // 第五步：对每个 key 范围、每个谓词调用一次删除接口
      for (KeyRange keyRange : keyRanges) {
        // 写入时 key 直接作为纳秒精度的时间戳，因此 key 范围就是纳秒时间范围。
        // getActualBeginKey/getActualEndKey 已把开闭区间换算成闭区间，与 InfluxDB 删除接口的 start、stop
        // 都包含端点的语义一致；再截断到 InfluxDB 允许的时间范围内，超出会被服务端拒绝
        long begin = Math.max(keyRange.getActualBeginKey(), MIN_DELETE_KEY);
        long end = Math.min(keyRange.getActualEndKey(), MAX_DELETE_KEY);
        if (begin > end) {
          // 截断后为空范围，没有可删除的数据
          continue;
        }
        for (String predicate : predicates) {
          client
              .getDeleteApi()
              .delete(nsToDateTime(begin), nsToDateTime(end), predicate, bucket, organization);
        }
      }
    } catch (Exception e) {
      LOGGER.error("encounter error when delete data in influxdb: ", e);
      return new TaskExecuteResult(
          null,
          new InfluxDBTaskExecuteFailureException("execute delete task in influxdb failure", e));
    }
    return new TaskExecuteResult(null, null);
  }

  /**
   * 校验删除请求，并解析出要删除的 measurement。
   *
   * <p>IGinX 路径写入 InfluxDB 时，第一级作为 measurement，其余部分作为 field。InfluxDB 只能按 measurement 和时间删除，不能按
   * field 删除，所以只接受能精确映射到 measurement 的路径，其余情况抛出异常。
   *
   * @param patterns 要删除的路径，多个路径之间是并集关系
   * @param tagFilter 删除时的 tag 过滤条件
   * @return 要删除的 measurement 列表（已去重）；返回 null 表示删除存储单元内的全部 measurement
   * @throws InfluxDBTaskExecuteFailureException 删除请求无法在 InfluxDB 中精确执行时抛出
   */
  static List<String> getDeletedMeasurements(List<String> patterns, TagFilter tagFilter)
      throws InfluxDBTaskExecuteFailureException {
    // InfluxDB 删除谓词只支持 tag 等值条件的 AND 组合，无法表达 IGinX 的通用 tag 过滤条件，统一拒绝
    if (tagFilter != null) {
      throw new InfluxDBTaskExecuteFailureException(
          "InfluxDB does not support deleting data with a tag filter");
    }
    // 路径为空表示删除整个存储单元，例如数据压缩（compaction）后清理旧分片
    if (patterns == null || patterns.isEmpty()) {
      return null;
    }
    // 写入 InfluxDB 的路径至少有两级（measurement.field），所以 "*.*" 和 "*" 一样能匹配全部数据。
    // 多个路径之间是并集，只要包含其中之一就等价于删除全部，其他路径无需再校验，
    // 例如 ["a.b", "*"] 会删除全部数据而不是报错
    if (patterns.contains("*") || patterns.contains("*.*")) {
      return null;
    }
    // 用 LinkedHashSet 去重并保持原有顺序，避免对同一个 measurement 重复发送删除请求
    Set<String> measurements = new LinkedHashSet<>();
    for (String pattern : patterns) {
      // 以第一个 '.' 为界，前半部分是 measurement，后半部分必须恰好是 ".*"
      int index = pattern.indexOf('.');
      String measurement = index > 0 ? pattern.substring(0, index) : "";
      // 以下情况都无法精确删除，直接报错：
      // 1. 没有 '.' 或以 '.' 开头，取不到 measurement，如 "a"、".*"；
      // 2. measurement 中带通配符，如 "a*.*"，而 InfluxDB 删除谓词只支持等值匹配，不支持通配符；
      // 3. '.' 之后不是 ".*"，即只删除部分 field，如 "a.b"、"a.b.*"、"a.*.c"
      if (measurement.isEmpty()
          || measurement.contains("*")
          || !pattern.substring(index).equals(".*")) {
        throw new InfluxDBTaskExecuteFailureException(
            String.format(
                "InfluxDB only supports deleting all data (\"*\") or whole measurements"
                    + " (\"<measurement>.*\"), but got \"%s\"",
                pattern));
      }
      measurements.add(measurement);
    }
    return new ArrayList<>(measurements);
  }

  /**
   * 查找存储单元对应的 bucket，找不到时返回 null。
   *
   * <p>先查本地缓存；缓存未命中时（例如 IGinX 重启后缓存为空）再到本组织下按名称查询，查到后放入缓存，避免之后每次删除都发一次 HTTP 请求。
   */
  private Bucket findBucket(String storageUnit) {
    Bucket bucket = bucketMap.get(storageUnit);
    if (bucket != null) {
      return bucket;
    }
    bucket = findBucketInOrg(storageUnit);
    if (bucket != null) {
      bucketMap.put(storageUnit, bucket);
    }
    return bucket;
  }

  /**
   * Bucket names are only unique within an organization, and listing buckets returns only the first
   * page, so the lookup is filtered by both on the server.
   */
  private Bucket findBucketInOrg(String name) {
    BucketsQuery query = new BucketsQuery();
    query.setOrgID(organization.getId());
    query.setName(name);
    try {
      List<Bucket> buckets = client.getBucketsApi().findBuckets(query);
      return buckets.isEmpty() ? null : buckets.get(0);
    } catch (NotFoundException e) { // returned instead of an empty list when filtered by orgID
      return null;
    }
  }

  /** Lists every bucket of this organization; a single request returns at most one page. */
  private List<Bucket> findAllBucketsInOrg() {
    List<Bucket> buckets = new ArrayList<>();
    BucketsQuery query = new BucketsQuery();
    query.setOrgID(organization.getId());
    query.setLimit(BUCKET_PAGE_SIZE);
    while (true) {
      query.setOffset(buckets.size());
      List<Bucket> page = client.getBucketsApi().findBuckets(query);
      buckets.addAll(page);
      if (page.size() < BUCKET_PAGE_SIZE) {
        return buckets;
      }
    }
  }

  private static OffsetDateTime nsToDateTime(long ns) {
    return OffsetDateTime.ofInstant(
        Instant.ofEpochSecond(Math.floorDiv(ns, 1_000_000_000L), Math.floorMod(ns, 1_000_000_000L)),
        ZoneOffset.UTC);
  }

  public static String toString(FluxTable table, FluxRecord record) {
    StringBuilder str =
        new StringBuilder(
            "measurement: "
                + record.getMeasurement()
                + ", field: "
                + record.getField()
                + ", value: "
                + record.getValue()
                + ", time: "
                + instantToNs(record.getTime()));
    for (int i = 8; i < table.getColumns().size(); i++) {
      str.append(", ")
          .append(table.getColumns().get(i).getLabel())
          .append(" = ")
          .append(record.getValueByIndex(i));
    }
    return str.toString();
  }
}

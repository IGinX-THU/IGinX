/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.influxdb;

import static cn.edu.tsinghua.iginx.influxdb.tools.TimeUtils.instantToNs;
import static com.influxdb.client.domain.WritePrecision.NS;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilterType;
import cn.edu.tsinghua.iginx.influxdb.exception.InfluxDBException;
import cn.edu.tsinghua.iginx.influxdb.exception.InfluxDBTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBHistoryQueryRowStream;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBQueryRowStream;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
import cn.edu.tsinghua.iginx.influxdb.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.influxdb.tools.SchemaTransformer;
import cn.edu.tsinghua.iginx.influxdb.tools.TagFilterUtils;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

  private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\"";

  private static final String SHOW_TIME_SERIES =
      "from(bucket:\"%s\") |> range(start: time(v: 0), stop: time(v: 9223372036854775807)) |> filter(fn: (r) => (r._measurement =~ /.*/ and r._field =~ /.+/)) |> first()";

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
    if (!testConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    Map<String, String> extraParams = meta.getExtraParams();
    String url = extraParams.getOrDefault("url", "http://localhost:8086/");
    OkHttpClient.Builder builder = new OkHttpClient.Builder().retryOnConnectionFailure(true);
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

  private boolean testConnection() {
    Map<String, String> extraParams = meta.getExtraParams();
    String url = extraParams.get("url");
    try {
      InfluxDBClient client =
          InfluxDBClientFactory.create(url, extraParams.get("token").toCharArray());
      client.close();
    } catch (Exception e) {
      LOGGER.error("test connection error: ", e);
      return false;
    }
    return true;
  }

  private void reloadHistoryData() {
    List<Bucket> buckets = client.getBucketsApi().findBucketsByOrg(organization);
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
      String statement = String.format(SHOW_TIME_SERIES, bucketName);
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
  public List<Column> getColumns() {
    List<Column> timeseries = new ArrayList<>();

    for (Bucket bucket :
        client.getBucketsApi().findBucketsByOrgName(organization.getName())) { // get all the bucket
      // query all the series by querying all the data with first()

      String unitPattern = "unit\\d{10}"; // unit后跟10个数字
      boolean isUnit = bucket.getName().matches(unitPattern);
      boolean isDummy =
          meta.isHasData()
              && (meta.getDataPrefix() == null
                  || bucket.getName().startsWith(meta.getDataPrefix()));
      if (bucket.getType() == Bucket.TypeEnum.SYSTEM || (!isUnit && !isDummy)) {
        continue;
      }

      String statement = String.format(SHOW_TIME_SERIES, bucket.getName());
      List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());

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
        if (isDummy && !isUnit) {
          path = bucket.getName() + "." + path;
        }
        timeseries.add(new Column(path, dataType, tag));
      }
    }

    return timeseries;
  }

  @Override
  public void release() throws PhysicalException {
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

    if (client.getBucketsApi().findBucketByName(storageUnit) == null) {
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
            keyInterval.getEndKey());

    List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
    InfluxDBQueryRowStream rowStream = new InfluxDBQueryRowStream(tables, project, filter);
    return new TaskExecuteResult(rowStream);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Map<String, String> bucketQueries = new HashMap<>();
    TagFilter tagFilter = project.getTagFilter();
    Filter filter = select.getFilter();
    getBucketQueriesForExecuteDummy(project, bucketQueries, tagFilter);

    Map<String, List<FluxTable>> bucketQueryResults = new HashMap<>();
    for (String bucket : bucketQueries.keySet()) {
      String statement =
          generateQueryStatement(
              bucket,
              project.getPatterns(),
              project.getTagFilter(),
              filter,
              keyInterval.getStartKey(),
              keyInterval.getEndKey());

      LOGGER.info("execute query: {}", statement);
      bucketQueryResults.put(bucket, client.getQueryApi().query(statement, organization.getId()));
    }

    InfluxDBHistoryQueryRowStream rowStream =
        new InfluxDBHistoryQueryRowStream(bucketQueryResults, project.getPatterns(), filter);
    return new TaskExecuteResult(rowStream);
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    KeyInterval keyInterval = dataArea.getKeyInterval();

    if (client.getBucketsApi().findBucketByName(storageUnit) == null) {
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
            keyInterval.getEndKey());

    List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
    InfluxDBQueryRowStream rowStream = new InfluxDBQueryRowStream(tables, project, null);
    return new TaskExecuteResult(rowStream);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Map<String, String> bucketQueries = new HashMap<>();
    TagFilter tagFilter = project.getTagFilter();
    getBucketQueriesForExecuteDummy(project, bucketQueries, tagFilter);

    long startKey = keyInterval.getStartKey();
    long endKey = keyInterval.getEndKey();

    Map<String, List<FluxTable>> bucketQueryResults = new HashMap<>();
    for (String bucket : bucketQueries.keySet()) {
      String statement =
          String.format(
              "from(bucket:\"%s\") |> range(start: time(v: %s), stop: time(v: %s))",
              bucket, startKey, endKey);
      if (!bucketQueries.get(bucket).equals("()")) {
        statement += String.format(" |> filter(fn: (r) => %s)", bucketQueries.get(bucket));
      }
      LOGGER.info("execute query: {}", statement);
      bucketQueryResults.put(bucket, client.getQueryApi().query(statement, organization.getId()));
    }

    InfluxDBHistoryQueryRowStream rowStream =
        new InfluxDBHistoryQueryRowStream(bucketQueryResults, project.getPatterns(), null);
    return new TaskExecuteResult(rowStream);
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
        List<Bucket> buckets = client.getBucketsApi().findBucketsByOrg(organization);
        for (Bucket bucket : buckets) {
          if (bucket.getType() == Bucket.TypeEnum.USER) {
            bucketNameList.add(bucket.getName());
          }
        }
      } else if (client.getBucketsApi().findBucketByName(bucketName) == null) {
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
      long endTime) {
    String statement = String.format(QUERY_DATA, bucketName, startTime, endTime);
    if (paths.size() != 1 || !paths.get(0).equals("*")) {
      StringBuilder filterStr = new StringBuilder(" |> filter(fn: (r) => ");
      filterStr.append('('); // make the or statement together
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        InfluxDBSchema schema = new InfluxDBSchema(path);
        if (i != 0) {
          filterStr.append(" or ");
        }
        filterStr.append('(');

        String measurement = schema.getMeasurement();
        if (measurement.contains("*")) {
          measurement = StringUtils.reformatPath(measurement);
        }
        filterStr.append(
            schema.getMeasurement().contains("*")
                ? "r._measurement =~ /" + measurement + "/"
                : "r._measurement == \"" + measurement + "\"");

        String field = schema.getField();
        field = StringUtils.reformatPath(field);
        filterStr.append(" and ");
        filterStr.append("r._field =~ /").append(field).append("/");

        Map<String, String> tags = schema.getTags();
        if (!tags.isEmpty()) {
          filterStr.append(" and ");
          assert tags.size() == 1;
          String key = InfluxDBSchema.TAG;
          String value = tags.get(key);
          if (value.contains("*")) {
            StringBuilder valueBuilder = new StringBuilder();
            for (Character character : value.toCharArray()) {
              if (character.equals('.')) {
                valueBuilder.append("\\.");
              } else if (character.equals('*')) {
                valueBuilder.append(".*");
              } else {
                valueBuilder.append(character);
              }
            }

            value = valueBuilder.toString();
            filterStr.append("r.").append(key).append(" =~ /");
            filterStr.append(value);
            filterStr.append("/");
          } else {
            filterStr.append("r.").append(key).append(" == \"");
            filterStr.append(value);
            filterStr.append("\"");
          }
        }

        filterStr.append(')');
      }
      filterStr.append(')'); // make the or statement together
      if (tagFilter != null && tagFilter.getType() != TagFilterType.WithoutTag) {
        filterStr.append(" and ").append(TagFilterUtils.transformToFilterStr(tagFilter));
      }
      filterStr.append(')');
      statement += filterStr;
    }

    if (filter != null) {
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
                      "r._measurement =~ /.+/", "r._measurement =~ /" + measurement + "/"))
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
              InfluxDBSchema schema = new InfluxDBSchema(path);
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
          List<Bucket> bucketList =
              client.getBucketsApi().findBucketsByOrgName(this.organizationName).stream()
                  .filter(b -> b.getName().equals(storageUnit))
                  .collect(Collectors.toList());
          if (bucketList.isEmpty()) {
            bucket = client.getBucketsApi().createBucket(storageUnit, organization);
          } else {
            bucket = bucketList.get(0);
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
          List<Bucket> bucketList =
              client.getBucketsApi().findBucketsByOrgName(this.organizationName).stream()
                  .filter(b -> b.getName().equals(storageUnit))
                  .collect(Collectors.toList());
          if (bucketList.isEmpty()) {
            bucket = client.getBucketsApi().createBucket(storageUnit, organization);
          } else {
            bucket = bucketList.get(0);
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

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    if (delete.getKeyRanges() == null || delete.getKeyRanges().size() == 0) { // 没有传任何 time range
      Bucket bucket = bucketMap.get(storageUnit);
      if (bucket == null) {
        return new TaskExecuteResult(null, null);
      }
      bucketMap.remove(storageUnit);
      client.getBucketsApi().deleteBucket(bucket);
      return new TaskExecuteResult(null, null);
    }
    // 删除某些序列的某一段数据
    Bucket bucket = bucketMap.get(storageUnit);
    if (bucket == null) {
      synchronized (this) {
        bucket = bucketMap.get(storageUnit);
        if (bucket == null) {
          List<Bucket> bucketList =
              client.getBucketsApi().findBucketsByOrgName(this.organizationName).stream()
                  .filter(b -> b.getName().equals(storageUnit))
                  .collect(Collectors.toList());
          if (bucketList.isEmpty()) {
            bucket = client.getBucketsApi().createBucket(storageUnit, organization);
          } else {
            bucket = bucketList.get(0);
          }
          bucketMap.put(storageUnit, bucket);
        }
      }
    }
    if (bucket == null) { // 没有数据，当然也不用删除
      return new TaskExecuteResult(null, null);
    }

    List<InfluxDBSchema> schemas =
        delete.getPatterns().stream().map(InfluxDBSchema::new).collect(Collectors.toList());
    for (InfluxDBSchema schema : schemas) {
      for (KeyRange keyRange : delete.getKeyRanges()) {
        client
            .getDeleteApi()
            .delete(
                OffsetDateTime.ofInstant(
                    Instant.ofEpochMilli(keyRange.getActualBeginKey()), ZoneId.of("UTC")),
                OffsetDateTime.ofInstant(
                    Instant.ofEpochMilli(keyRange.getActualEndKey()), ZoneId.of("UTC")),
                String.format(DELETE_DATA, schema.getMeasurement(), schema.getField()),
                bucket,
                organization);
      }
    }
    return new TaskExecuteResult(null, null);
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

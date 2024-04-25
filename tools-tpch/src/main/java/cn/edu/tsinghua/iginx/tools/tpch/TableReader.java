package cn.edu.tsinghua.iginx.tools.tpch;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TableReader {

  private final String path;

  private final String tableName;

  private final List<String> columns;

  private final List<DataType> types;

  private final int batch;

  private final BufferedReader reader;

  private String str;

  private final Session session;

  private int indexKey = 0;

  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  public TableReader(
      String path,
      String tableName,
      List<String> columns,
      List<DataType> types,
      int batch,
      String host,
      int port)
      throws Exception {
    this.path = path;
    this.tableName = tableName;
    this.types = types;
    this.batch = batch;

    this.columns = new ArrayList<>(columns.size());
    for (String suffix : columns) {
      this.columns.add(tableName + "." + suffix);
    }

    FileInputStream inputStream = new FileInputStream(path + "/" + tableName + ".tbl");
    this.reader = new BufferedReader(new InputStreamReader(inputStream));
    this.str = reader.readLine();

    this.session = new Session(host, port, "root", "root");
    this.session.openSession();
  }

  public boolean hasNext() {
    return str != null;
  }

  public void loadNextBatch() throws IOException {
    int cnt = 0;
    List<String> lines = new ArrayList<>();
    while (str != null && cnt < batch) {
      lines.add(str);
      str = reader.readLine();
      cnt++;
    }

    long[] keys = new long[lines.size()];
    Object[] values = new Object[lines.size()];

    for (int i = 0; i < lines.size(); i++) {
      String[] rowStrValues = lines.get(i).split("\\|");
      Object[] rowValues = new Object[columns.size()];
      assert rowValues.length == rowStrValues.length;
      for (int j = 0; j < rowValues.length; j++) {
        rowValues[j] = getValueByType(rowStrValues[j], columns.get(j), types.get(j));
      }

      keys[i] = indexKey;
      values[i] = rowValues;

      str = reader.readLine();
      cnt++;
      indexKey++;
    }

    lines.clear();

    try {
      session.insertRowRecords(columns, keys, values, types, null, TimePrecision.NS);
    } catch (SessionException | ExecutionException | NullPointerException e) {
      throw new RuntimeException(e);
    }
  }

  private Object getValueByType(String strValue, String colName, DataType type) {
    switch (type) {
      case LONG:
        if (colName.endsWith("date")) {
          Date date = null;
          try {
            date = dateFormat.parse(strValue);
          } catch (ParseException e) {
            e.printStackTrace();
            return 0;
          }
          return date.getTime() / 1000;
        } else {
          return Long.parseLong(strValue);
        }
      case DOUBLE:
        return Double.parseDouble(strValue);
      default:
        return strValue.getBytes(StandardCharsets.UTF_8);
    }
  }

  public void close() throws SessionException {
    session.closeSession();
  }
}

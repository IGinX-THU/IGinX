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
package cn.edu.tsinghua.iginx.integration.tpch;

import static cn.edu.tsinghua.iginx.integration.tpch.TPCHDataGeneratorIT.FieldType.DATE;
import static cn.edu.tsinghua.iginx.integration.tpch.TPCHDataGeneratorIT.FieldType.NUM;
import static cn.edu.tsinghua.iginx.integration.tpch.TPCHDataGeneratorIT.FieldType.STR;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHDataGeneratorIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TPCHDataGeneratorIT.class);

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  // .tbl文件所在目录
  static final String DATA_DIR = System.getProperty("user.dir") + "/../tpc/TPC-H V3.0.1/data";

  // udf文件所在目录
  static final String UDF_DIR = "src/test/resources/tpch/udf/";

  static final String SHOW_FUNCTION = "SHOW FUNCTIONS;";

  static final String SINGLE_UDF_REGISTER_SQL = "CREATE FUNCTION %s \"%s\" FROM \"%s\" IN \"%s\";";

  protected static Session session;

  List<Integer> queryIds;

  enum FieldType {
    NUM,
    STR,
    DATE
  }

  public TPCHDataGeneratorIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    queryIds = conf.getQueryIds();
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
    session.openSession();
  }

  @AfterClass
  public static void close() throws SessionException {
    session.closeSession();
  }

  @Before
  public void prepare() {
    List<String> tableList =
        Arrays.asList(
            "region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem");

    List<List<String>> fieldsList = new ArrayList<>();
    List<List<FieldType>> typesList = new ArrayList<>();

    // region表
    fieldsList.add(Arrays.asList("r_regionkey", "r_name", "r_comment"));
    typesList.add(Arrays.asList(NUM, STR, STR));

    // nation表
    fieldsList.add(Arrays.asList("n_nationkey", "n_name", "n_regionkey", "n_comment"));
    typesList.add(Arrays.asList(NUM, STR, NUM, STR));

    // supplier表
    fieldsList.add(
        Arrays.asList(
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment"));
    typesList.add(Arrays.asList(NUM, STR, STR, NUM, STR, NUM, STR));

    // part表
    fieldsList.add(
        Arrays.asList(
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment"));
    typesList.add(Arrays.asList(NUM, STR, STR, STR, STR, NUM, STR, NUM, STR));

    // partsupp表
    fieldsList.add(
        Arrays.asList("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"));
    typesList.add(Arrays.asList(NUM, NUM, NUM, NUM, STR));

    // customer表
    fieldsList.add(
        Arrays.asList(
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment"));
    typesList.add(Arrays.asList(NUM, STR, STR, NUM, STR, NUM, STR, STR));

    // orders表
    fieldsList.add(
        Arrays.asList(
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment"));
    typesList.add(Arrays.asList(NUM, NUM, STR, NUM, DATE, STR, STR, NUM, STR));

    // lineitem表
    fieldsList.add(
        Arrays.asList(
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment"));
    typesList.add(
        Arrays.asList(
            NUM, NUM, NUM, NUM, NUM, NUM, NUM, NUM, STR, STR, DATE, DATE, DATE, STR, STR, STR));

    // 插入数据
    for (int i = 0; i < 8; i++) {
      insertTable(tableList.get(i), fieldsList.get(i), typesList.get(i));
    }

    List<List<String>> UDFInfos = new ArrayList<>();
    UDFInfos.add(Arrays.asList("UDTF", "extractYear", "UDFExtractYear", "udtf_extract_year.py"));
    // 注册UDF函数
    for (List<String> UDFInfo : UDFInfos) {
      registerUDF(UDFInfo);
    }
  }

  private void insertTable(String table, List<String> fields, List<FieldType> types) {
    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(table);
    builder.append("(key, ");
    for (String field : fields) {
      builder.append(field);
      builder.append(", ");
    }
    builder.setLength(builder.length() - 2);
    builder.append(") VALUES ");
    String insertPrefix = builder.toString();

    long count = 0;
    try (BufferedReader br =
        new BufferedReader(new FileReader(String.format("%s/%s.tbl", DATA_DIR, table)))) {
      StringBuilder sb = new StringBuilder(insertPrefix);
      String line;
      while ((line = br.readLine()) != null) {
        String[] items = line.split("\\|");
        sb.append("(");
        sb.append(count); // 插入自增key列
        count++;
        sb.append(", ");
        assert fields.size() == items.length;
        for (int i = 0; i < items.length; i++) {
          switch (types.get(i)) {
            case NUM:
              sb.append(items[i]);
              sb.append(", ");
              break;
            case STR: // 字符串类型在外面需要包一层引号
              sb.append("\"");
              sb.append(items[i]);
              sb.append("\", ");
              break;
            case DATE: // 日期类型需要转为时间戳
              SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
              dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
              long time = dateFormat.parse(items[i]).getTime();
              sb.append(time);
              sb.append(", ");
              break;
            default:
              break;
          }
        }
        sb.setLength(sb.length() - 2);
        sb.append("), ");

        // 每次最多插入10000条数据
        if (count % 10000 == 0) {
          sb.setLength(sb.length() - 2);
          sb.append(";");
          session.executeSql(sb.toString());
          sb = new StringBuilder(insertPrefix);
        }
      }
      // 插入剩余数据
      if (sb.length() != insertPrefix.length()) {
        sb.setLength(sb.length() - 2);
        sb.append(";");
        session.executeSql(sb.toString());
      }
      LOGGER.info("Insert {} records into table [{}].", count, table);
    } catch (IOException | ParseException | SessionException e) {
      LOGGER.error("Insert into table {} fail. Caused by:", table, e);
      fail();
    }
  }

  private void registerUDF(List<String> UDFInfo) {
    String result = "";
    try {
      result = session.executeSql(SHOW_FUNCTION).getResultInString(false, "");
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", SHOW_FUNCTION, e);
      fail();
    }
    // UDF已注册
    if (result.contains(UDFInfo.get(1))) {
      return;
    }
    File udfFile = new File(UDF_DIR + UDFInfo.get(3));
    String register =
        String.format(
            SINGLE_UDF_REGISTER_SQL,
            UDFInfo.get(0),
            UDFInfo.get(1),
            UDFInfo.get(2),
            udfFile.getAbsolutePath());
    try {
      LOGGER.info("Execute register UDF statement: {}", register);
      session.executeRegisterTask(register, false);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", register, e);
      fail();
    }
  }

  // 插入TPC-H测试中的临时表
  @Test
  public void insertTmpTable() {
    for (int queryId : queryIds) {
      String sqlString = null;
      try {
        sqlString =
            TPCHUtils.readSqlFileAsString("src/test/resources/tpch/queries/q" + queryId + ".sql");
      } catch (IOException e) {
        LOGGER.error("Fail to read sql file: q{}.sql. Caused by: ", queryId, e);
        fail();
      }
      String[] sqls = sqlString.split(";");
      if (sqls.length < 2) {
        LOGGER.error("q{}.sql has no ';' in the end. Caused by: ", queryId);
        fail();
      } else if (sqls.length == 2) {
        // 只有一条查询语句，无需插入临时表
        continue;
      }
      // 处理插入临时表语句
      for (int i = 0; i < sqls.length - 2; i++) {
        String sql = sqls[i] + ";";
        try {
          LOGGER.info("Execute Statement: \"{}\"", sql);
          session.executeSql(sql);
        } catch (SessionException e) {
          LOGGER.error("Statement: \"{}\" execute fail. Caused by:", sql, e);
          fail();
        }
      }
    }
  }
}

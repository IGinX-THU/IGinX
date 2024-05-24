package cn.edu.tsinghua.iginx.integration.polybench;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.bson.Document;
import org.junit.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class TPCHDataInsertionIT {
  private static final String dataPath =
      System.getProperty("user.dir") + "/../tpc/TPC-H V3.0.1/data";

  public void TPCHDataInsertionIT() {}

  private static MongoClient connect(int port) {
    ServerAddress address = new ServerAddress("127.0.0.1", port);
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(address)))
            .build();

    return MongoClients.create(settings);
  }

  @Test
  public void insertDataIntoMongoDB() {
    System.out.println(System.getProperty("user.dir"));
    int port = 27017;
    String databaseName = "mongotpch";
    try (MongoClient client = connect(port)) {
      ExecutorService executor = Executors.newFixedThreadPool(2); // 使用2个线程
      List<Future<?>> futures = new ArrayList<>();

      // 读取 lineitem.tbl 文件并插入数据
      futures.add(executor.submit(() -> insertLineItemFromFile(client, databaseName)));

      // 读取 orders.tbl 文件并插入数据
      futures.add(executor.submit(() -> insertOrdersFromFile(client, databaseName)));

      // 等待所有任务完成
      for (Future<?> future : futures) {
        future.get();
      }

      executor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void insertLineItemFromFile(MongoClient client, String databaseName) {
    try {
      String collectionName = "lineitem";
      String fileName = "lineitem.tbl";
      Long start_time = System.currentTimeMillis();
      List<Document> documents = new ArrayList<>();
      try (BufferedReader br =
          new BufferedReader(new FileReader(String.format("%s/%s", dataPath, fileName)))) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] fields = line.split("\\|");
          // 解析数据并构建 Document 对象
          // 将字符型字段转换为相应类型
          int l_orderkey = Integer.parseInt(fields[0]);
          int l_partkey = Integer.parseInt(fields[1]);
          int l_suppkey = Integer.parseInt(fields[2]);
          int l_linenumber = Integer.parseInt(fields[3]);
          double l_quantity = Double.parseDouble(fields[4]);
          double l_extendedprice = Double.parseDouble(fields[5]);
          double l_discount = Double.parseDouble(fields[6]);
          double l_tax = Double.parseDouble(fields[7]);
          String l_returnflag = fields[8];
          String l_linestatus = fields[9];
          // 解析日期字段
          Date l_shipdate = null, l_commitdate = null, l_receiptdate = null;
          try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            l_shipdate = dateFormat.parse(fields[10]);
            l_commitdate = dateFormat.parse(fields[11]);
            l_receiptdate = dateFormat.parse(fields[12]);
          } catch (ParseException e) {
            e.printStackTrace();
          }
          String l_shipinstruct = fields[13];
          String l_shipmode = fields[14];
          String l_comment = fields[15];

          // 构建 MongoDB 文档
          Document document =
              new Document()
                  .append("l_orderkey", l_orderkey)
                  .append("l_partkey", l_partkey)
                  .append("l_suppkey", l_suppkey)
                  .append("l_linenumber", l_linenumber)
                  .append("l_quantity", l_quantity)
                  .append("l_extendedprice", l_extendedprice)
                  .append("l_discount", l_discount)
                  .append("l_tax", l_tax)
                  .append("l_returnflag", l_returnflag)
                  .append("l_linestatus", l_linestatus)
                  .append("l_shipdate", l_shipdate)
                  .append("l_commitdate", l_commitdate)
                  .append("l_receiptdate", l_receiptdate)
                  .append("l_shipinstruct", l_shipinstruct)
                  .append("l_shipmode", l_shipmode)
                  .append("l_comment", l_comment);
          // 将 Document 对象添加到列表中
          documents.add(document);
          if (documents.size() >= 10000) {
            // 每次插入 10000 条数据
            MongoCollection<Document> collection =
                client.getDatabase(databaseName).getCollection(collectionName);
            collection.insertMany(documents);
            documents.clear();
            System.out.println("Inserted 10000 records into " + collectionName);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
      // 插入数据到 MongoDB
      MongoCollection<Document> collection =
          client.getDatabase(databaseName).getCollection(collectionName);
      collection.insertMany(documents);
      System.out.println(
          "Data loaded successfully to collection "
              + collectionName
              + " in "
              + (System.currentTimeMillis() - start_time)
              + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void insertOrdersFromFile(MongoClient client, String databaseName) {
    try {
      String collectionName = "orders";
      String fileName = "orders.tbl";
      Long start_time = System.currentTimeMillis();
      List<Document> documents = new ArrayList<>();
      try (BufferedReader br =
          new BufferedReader(new FileReader(String.format("%s/%s", dataPath, fileName)))) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] fields = line.split("\\|");
          // 将字符型字段转换为相应类型
          int o_orderkey = Integer.parseInt(fields[0]);
          int o_custkey = Integer.parseInt(fields[1]);
          String o_orderstatus = fields[2];
          double o_totalprice = Double.parseDouble(fields[3]);
          Date o_orderdate = null;
          try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            o_orderdate = dateFormat.parse(fields[4]);
          } catch (ParseException e) {
            e.printStackTrace();
          }
          String o_orderpriority = fields[5];
          String o_clerk = fields[6];
          int o_shippriority = Integer.parseInt(fields[7]);
          String o_comment = fields[8];

          // 构建 MongoDB 文档
          Document document =
              new Document()
                  .append("o_orderkey", o_orderkey)
                  .append("o_custkey", o_custkey)
                  .append("o_orderstatus", o_orderstatus)
                  .append("o_totalprice", o_totalprice)
                  .append("o_orderdate", o_orderdate)
                  .append("o_orderpriority", o_orderpriority)
                  .append("o_clerk", o_clerk)
                  .append("o_shippriority", o_shippriority)
                  .append("o_comment", o_comment);
          // 将 Document 对象添加到列表中
          documents.add(document);
          if (documents.size() >= 10000) {
            // 每次插入 10000 条数据
            MongoCollection<Document> collection =
                client.getDatabase(databaseName).getCollection(collectionName);
            collection.insertMany(documents);
            documents.clear();
            System.out.println("Inserted 10000 records into " + collectionName);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
      // 插入数据到 MongoDB
      MongoCollection<Document> collection =
          client.getDatabase(databaseName).getCollection(collectionName);
      collection.insertMany(documents);
      System.out.println(
          "Data loaded successfully to collection "
              + collectionName
              + " in "
              + (System.currentTimeMillis() - start_time)
              + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void insertDataIntoPostgreSQL() {
    int port = 5432;
    String databaseName = "tpchdata";
    // PostgreSQL连接参数
    String url = String.format("jdbc:postgresql://localhost:%s/postgres", port);
    String user = "postgres";
    String password = "postgres";

    // 待执行的 SQL 语句
    String[] sqlStatements = {
      "DROP TABLE IF EXISTS customer",
      "DROP TABLE IF EXISTS supplier",
      "DROP TABLE IF EXISTS region",
      "DROP TABLE IF EXISTS part",
      "DROP TABLE IF EXISTS partsupp",
      "CREATE TABLE IF NOT EXISTS customer ( c_custkey INT, c_name VARCHAR(25), c_address VARCHAR(40), c_nationkey INT, c_phone VARCHAR(15), c_acctbal FLOAT, c_mktsegment VARCHAR(10), c_comment VARCHAR(117), c_dummy VARCHAR(2), PRIMARY KEY (c_custkey))",
      "CREATE TABLE IF NOT EXISTS region ( r_regionkey INT, r_name VARCHAR(25), r_comment VARCHAR(152), r_dummy VARCHAR(2), PRIMARY KEY (r_regionkey))",
      "CREATE TABLE IF NOT EXISTS supplier ( s_suppkey INT, s_name VARCHAR(25), s_address VARCHAR(40), s_nationkey INT, s_phone VARCHAR(15), s_acctbal FLOAT, s_comment VARCHAR(101), s_dummy VARCHAR(2), PRIMARY KEY (s_suppkey))",
      "CREATE TABLE IF NOT EXISTS part ( p_partkey  INT, p_name   VARCHAR(55), p_mfgr  VARCHAR(25), p_brand  VARCHAR(10), p_type  VARCHAR(25), p_size  INT, p_container  VARCHAR(10), p_retailprice FLOAT , p_comment VARCHAR(23) , p_dummy VARCHAR(2), PRIMARY KEY (p_partkey))",
      "CREATE TABLE IF NOT EXISTS partsupp ( ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost  FLOAT, ps_comment VARCHAR(199), ps_dummy   VARCHAR(2))",
    };

    try (Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement()) {
      if (conn != null) {
        System.out.println("Connected to the PostgreSQL server successfully.");
        stmt.executeUpdate(String.format("DROP DATABASE IF EXISTS %s", databaseName));
        stmt.executeUpdate(String.format("CREATE DATABASE %s", databaseName));
        System.out.println(String.format("Database '%s' created successfully.", databaseName));
        // 关闭当前连接
        stmt.close();
        conn.close();
      }
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      e.printStackTrace();
    }
    // 连接到新创建的数据库
    String newDbUrl = String.format("jdbc:postgresql://localhost:5432/%s", databaseName);
    try (Connection conn = DriverManager.getConnection(newDbUrl, user, password);
        Statement stmt = conn.createStatement()) {
      if (conn != null) {
        System.out.println(String.format("Connected to '%s' successfully.", databaseName));

        // 依次执行每条 SQL 语句
        for (String sql : sqlStatements) {
          stmt.execute(sql);
          System.out.println("Executed SQL statement: " + sql);
        }
        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        List<String> tableNames =
            Arrays.asList("customer", "supplier", "region", "part", "partsupp");
        for (String tableName : tableNames) {
          String filePath = String.format("%s/%s.tbl", dataPath, tableName);
          FileReader fileReader = new FileReader(filePath);
          // 使用 CopyManager 执行 COPY 命令将数据从 CSV 文件加载到数据库表中
          copyManager.copyIn(
              "COPY " + tableName + " FROM STDIN WITH DELIMITER '|' CSV", fileReader);
          System.out.println("Data loaded successfully from CSV to table " + tableName);
        }
      } else {
        System.out.println("Failed to make connection to the PostgreSQL server.");
      }
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

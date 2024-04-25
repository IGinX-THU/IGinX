package cn.edu.tsinghua.iginx.tools.tpch;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataReader {

  public static void main(String[] args) {

    assert args.length == 3;

    String path = args[0];

    String host = args[1];
    int port = Integer.parseInt(args[2]);

    Map<String, List<String>> tableColumns = new HashMap<>();
    tableColumns.put(
        "supplier",
        Arrays.asList(
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment"));
    tableColumns.put("nation", Arrays.asList("n_nationkey", "n_name", "n_regionkey", "n_comment"));
    tableColumns.put("region", Arrays.asList("r_regionkey", "r_name", "r_comment"));
    tableColumns.put(
        "customer",
        Arrays.asList(
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment"));
    tableColumns.put(
        "part",
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
    tableColumns.put(
        "partsupp",
        Arrays.asList("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"));
    tableColumns.put(
        "orders",
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
    tableColumns.put(
        "lineitem",
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

    Map<String, List<DataType>> tableTypes = new HashMap<>();
    tableTypes.put(
        "supplier",
        Arrays.asList(
            DataType.LONG,
            DataType.BINARY,
            DataType.BINARY,
            DataType.LONG,
            DataType.BINARY,
            DataType.DOUBLE,
            DataType.BINARY));
    tableTypes.put(
        "nation", Arrays.asList(DataType.LONG, DataType.BINARY, DataType.LONG, DataType.BINARY));
    tableTypes.put("region", Arrays.asList(DataType.LONG, DataType.BINARY, DataType.BINARY));
    tableTypes.put(
        "customer",
        Arrays.asList(
            DataType.LONG,
            DataType.BINARY,
            DataType.BINARY,
            DataType.LONG,
            DataType.BINARY,
            DataType.DOUBLE,
            DataType.BINARY,
            DataType.BINARY));
    tableTypes.put(
        "part",
        Arrays.asList(
            DataType.LONG,
            DataType.BINARY,
            DataType.BINARY,
            DataType.BINARY,
            DataType.BINARY,
            DataType.LONG,
            DataType.BINARY,
            DataType.DOUBLE,
            DataType.BINARY));
    tableTypes.put(
        "partsupp",
        Arrays.asList(
            DataType.LONG, DataType.LONG, DataType.LONG, DataType.DOUBLE, DataType.BINARY));
    tableTypes.put(
        "orders",
        Arrays.asList(
            DataType.LONG,
            DataType.LONG,
            DataType.BINARY,
            DataType.DOUBLE,
            DataType.LONG,
            DataType.BINARY,
            DataType.BINARY,
            DataType.LONG,
            DataType.BINARY));
    tableTypes.put(
        "lineitem",
        Arrays.asList(
            DataType.LONG,
            DataType.LONG,
            DataType.LONG,
            DataType.LONG,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.DOUBLE,
            DataType.DOUBLE,
            DataType.BINARY,
            DataType.BINARY,
            DataType.LONG,
            DataType.LONG,
            DataType.LONG,
            DataType.BINARY,
            DataType.BINARY,
            DataType.BINARY));

    for (String table : tableColumns.keySet()) {
      TableReader reader = null;
      List<String> columns = tableColumns.get(table);
      List<DataType> types = tableTypes.get(table);

      try {
        reader = new TableReader(path, table, columns, types, 100000, host, port);

        int index = 0;
        System.out.println("loading " + table);
        while (reader.hasNext()) {
          System.out.println("loading " + index++ + " batch");
          reader.loadNextBatch();
        }
        reader.close();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    }
  }
}

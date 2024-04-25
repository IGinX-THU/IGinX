package cn.edu.tsinghua.iginx.tools.benchmark;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyQueryBenchmark {

  public static final String PREFIX = "explain physical ";

  public static final String Q1 =
      "select\n"
          + "    lineitem.l_returnflag as l_returnflag,\n"
          + "    lineitem.l_linestatus as l_linestatus,\n"
          + "    sum(lineitem.l_quantity) as sum_qty,\n"
          + "    sum(lineitem.l_extendedprice) as sum_base_price,\n"
          + "    sum(tmp1) as sum_disc_price, \n"
          + "    sum(tmp2) as sum_charge, \n"
          + "    avg(lineitem.l_quantity) as avg_qty,\n"
          + "    avg(lineitem.l_extendedprice) as avg_price,\n"
          + "    avg(lineitem.l_discount) as avg_disc,\n"
          + "    count(*) as count_order\n"
          + "from (\n"
          + "    select\n"
          + "        l_returnflag,\n"
          + "        l_linestatus,\n"
          + "        l_quantity,\n"
          + "        l_extendedprice,\n"
          + "        l_discount,\n"
          + "        l_extendedprice * (1 - l_discount) as tmp1,\n"
          + "        l_extendedprice * (1 - l_discount) * (1 + l_tax) as tmp2\n"
          + "    from\n"
          + "        lineitem\n"
          + "    where\n"
          + "        lineitem.l_shipdate <= 9124416000\n"
          + ")\n"
          + "group by\n"
          + "    lineitem.l_returnflag,\n"
          + "    lineitem.l_linestatus\n"
          + "order by\n"
          + "    lineitem.l_returnflag,\n"
          + "    lineitem.l_linestatus;";

  public static final String Q2 =
      "select\n"
          + "    supplier.s_acctbal,\n"
          + "    supplier.s_name,\n"
          + "    nation.n_name,\n"
          + "    part.p_partkey,\n"
          + "    part.p_mfgr,\n"
          + "    supplier.s_address,\n"
          + "    supplier.s_phone,\n"
          + "    supplier.s_comment\n"
          + "from\n"
          + "    part\n"
          + "    join partsupp on part.p_partkey = partsupp.ps_partkey\n"
          + "    join supplier on supplier.s_suppkey = partsupp.ps_suppkey\n"
          + "    join nation on supplier.s_nationkey = nation.n_nationkey\n"
          + "    join region on nation.n_regionkey = region.r_regionkey\n"
          + "where\n"
          + "    partsupp.ps_supplycost = (\n"
          + "        select\n"
          + "            min(partsupp.ps_supplycost)\n"
          + "        from\n"
          + "            partsupp\n"
          + "            join supplier on supplier.s_suppkey = partsupp.ps_suppkey\n"
          + "            join nation on supplier.s_nationkey = nation.n_nationkey\n"
          + "            join region on nation.n_regionkey = region.r_regionkey\n"
          + "        where\n"
          + "            part.p_partkey = partsupp.ps_partkey \n"
          + "            and region.r_name = 'AMERICA'\n"
          + "    )\n"
          + "order by\n"
          + "    supplier.s_acctbal,\n"
          + "    nation.n_name,\n"
          + "    supplier.s_name,\n"
          + "    part.p_partkey;";

  public static final String Q3 =
      "select\n"
          + "    lineitem.l_orderkey,\n"
          + "    orders.o_orderdate,\n"
          + "    orders.o_shippriority\n"
          + "from\n"
          + "    customer\n"
          + "    join orders on customer.c_custkey = orders.o_custkey\n"
          + "    join lineitem on lineitem.l_orderkey = orders.o_orderkey\n"
          + "where\n"
          + "    customer.c_mktsegment = 'BUILDING'\n"
          + "    and orders.o_orderdate < 9124416000\n"
          + "    and lineitem.l_shipdate > 0\n"
          + "group by\n"
          + "    lineitem.l_orderkey,\n"
          + "    orders.o_orderdate,\n"
          + "    orders.o_shippriority\n"
          + "order by\n"
          + "    orders.o_orderdate;";

  public static final String Q4 =
      "select\n"
          + "    orders.o_orderpriority,\n"
          + "    count(*) as order_count\n"
          + "from\n"
          + "    orders\n"
          + "where\n"
          + "    orders.o_orderdate >= 0\n"
          + "    and orders.o_orderdate < 9124416000\n"
          + "    and exists (\n"
          + "        select\n"
          + "            *\n"
          + "        from\n"
          + "            lineitem\n"
          + "        where\n"
          + "            lineitem.l_orderkey = orders.o_orderkey\n"
          + "            and lineitem.l_commitdate < lineitem.l_receiptdate\n"
          + "    )\n"
          + "group by\n"
          + "    orders.o_orderpriority\n"
          + "order by\n"
          + "    orders.o_orderpriority;";

  public static final String Q5 =
      "select \n"
          + "    nation.n_name,\n"
          + "    revenue\n"
          + "from (\n"
          + "    select\n"
          + "        nation.n_name,\n"
          + "        sum(tmp) as revenue\n"
          + "    from (\n"
          + "        select\n"
          + "            nation.n_name,\n"
          + "            lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp\n"
          + "        from\n"
          + "            customer\n"
          + "            join orders on customer.c_custkey = orders.o_custkey\n"
          + "            join lineitem on lineitem.l_orderkey = orders.o_orderkey\n"
          + "            join supplier on lineitem.l_suppkey = supplier.s_suppkey and customer.c_nationkey = supplier.s_nationkey\n"
          + "            join nation on supplier.s_nationkey = nation.n_nationkey\n"
          + "            join region on nation.n_regionkey = region.r_regionkey\n"
          + "        where\n"
          + "            region.r_name = \"EUROPE\"\n"
          + "            and orders.o_orderdate >= 0\n"
          + "            and orders.o_orderdate < 9124416000\n"
          + ")\n"
          + "group by\n"
          + "    nation.n_name\n"
          + ")\n"
          + "order by\n"
          + "    revenue desc;";

  public static final String Q6 =
      "select\n"
          + "    sum(tmp) as revenue\n"
          + "from (\n"
          + "    select\n"
          + "        l_extendedprice * l_discount as tmp\n"
          + "    from\n"
          + "        lineitem\n"
          + "    where\n"
          + "        lineitem.l_shipdate >= 0\n"
          + "        and lineitem.l_shipdate < 9124416000\n"
          + "        and lineitem.l_discount >= 0.02 \n"
          + "        and lineitem.l_discount < 0.09\n"
          + "        and lineitem.l_quantity < 25\n"
          + ");";

  public static final String Q9 =
      "select\n"
          + "    nation,\n"
          + "    date,\n"
          + "    sum(amount) as sum_profit\n"
          + "from (\n"
          + "    select\n"
          + "        nation.n_name as nation,\n"
          + "        orders.o_orderdate as date,\n"
          + "        lineitem.l_extendedprice * (1 - lineitem.l_discount) - partsupp.ps_supplycost * lineitem.l_quantity as amount\n"
          + "    from\n"
          + "        part\n"
          + "        join lineitem on part.p_partkey = lineitem.l_partkey\n"
          + "        join supplier on supplier.s_suppkey = lineitem.l_suppkey\n"
          + "        join partsupp on partsupp.ps_suppkey = lineitem.l_suppkey and partsupp.ps_partkey = lineitem.l_partkey\n"
          + "        join orders on orders.o_orderkey = lineitem.l_orderkey\n"
          + "        join nation on supplier.s_nationkey = nation.n_nationkey\n"
          + "    where\n"
          + "        part.p_name like '.*yellow.*'\n"
          + ")\n"
          + "group by\n"
          + "    nation,\n"
          + "    date\n"
          + "order by\n"
          + "    nation,\n"
          + "    date desc;";

  public static final String Q10 =
      "select\n"
          + "    customer.c_custkey,\n"
          + "    customer.c_name,\n"
          + "    revenue,\n"
          + "    customer.c_acctbal,\n"
          + "    nation.n_name,\n"
          + "    customer.c_address,\n"
          + "    customer.c_phone,\n"
          + "    customer.c_comment\n"
          + "from (\n"
          + "    select\n"
          + "        customer.c_custkey,\n"
          + "        customer.c_name,\n"
          + "        sum(tmp) as revenue,\n"
          + "        customer.c_acctbal,\n"
          + "        nation.n_name,\n"
          + "        customer.c_address,\n"
          + "        customer.c_phone,\n"
          + "        customer.c_comment\n"
          + "    from (\n"
          + "        select\n"
          + "            customer.c_custkey,\n"
          + "            customer.c_name,\n"
          + "            lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp,\n"
          + "            customer.c_acctbal,\n"
          + "            nation.n_name,\n"
          + "            customer.c_address,\n"
          + "            customer.c_phone,\n"
          + "            customer.c_comment\n"
          + "        from\n"
          + "            customer\n"
          + "            join orders on customer.c_custkey = orders.o_custkey\n"
          + "            join lineitem on lineitem.l_orderkey = orders.o_orderkey\n"
          + "            join nation on customer.c_nationkey = nation.n_nationkey\n"
          + "        where\n"
          + "            orders.o_orderdate >= 0\n"
          + "            and orders.o_orderdate < 9124416000\n"
          + "            and lineitem.l_returnflag = 'R'\n"
          + "    )\n"
          + "    group by\n"
          + "        customer.c_custkey,\n"
          + "        customer.c_name,\n"
          + "        customer.c_acctbal,\n"
          + "        customer.c_phone,\n"
          + "        nation.n_name,\n"
          + "        customer.c_address,\n"
          + "        customer.c_comment\n"
          + ")\n"
          + "order by\n"
          + "    revenue desc;";

  public static final String Q13 =
      "select\n"
          + "    c_count, \n"
          + "    custdist\n"
          + "from (\n"
          + "    select\n"
          + "        c_count, \n"
          + "        count(*) as custdist\n"
          + "    from (\n"
          + "        select\n"
          + "            customer.c_custkey as c_custkey,\n"
          + "            count(orders.o_orderkey) as c_count\n"
          + "        from\n"
          + "            customer left outer join orders on customer.c_custkey = orders.o_custkey and !(orders.o_comment like '.*pending.*')\n"
          + "        group by\n"
          + "            customer.c_custkey\n"
          + "    )\n"
          + "    group by\n"
          + "        c_count\n"
          + ")\n"
          + "order by\n"
          + "    custdist,\n"
          + "    c_count desc;";

  public static final String Q16 =
      "select\n"
          + "    p_brand,\n"
          + "    p_type,\n"
          + "    p_size,\n"
          + "    supplier_cnt\n"
          + "from (\n"
          + "    select\n"
          + "        part.p_brand as p_brand,\n"
          + "        part.p_type as p_type,\n"
          + "        part.p_size as p_size,\n"
          + "        count(distinct partsupp.ps_suppkey) as supplier_cnt\n"
          + "    from\n"
          + "        partsupp\n"
          + "        join part on part.p_partkey = partsupp.ps_partkey\n"
          + "    where\n"
          + "        part.p_brand != 'Brand#13'\n"
          + "        and !(part.p_type like '.*BURNISHED.*')\n"
          + "        and (\n"
          + "            part.p_size = 7\n"
          + "            or part.p_size = 8\n"
          + "            or part.p_size = 9\n"
          + "            or part.p_size = 21\n"
          + "            or part.p_size = 23\n"
          + "            or part.p_size = 26\n"
          + "            or part.p_size = 31\n"
          + "            or part.p_size = 49\n"
          + "        )\n"
          + "        and partsupp.ps_suppkey not in (\n"
          + "            select\n"
          + "                s_suppkey\n"
          + "            from\n"
          + "                supplier\n"
          + "            where\n"
          + "                supplier.s_comment like '.*Customer.*Complaints.*'\n"
          + "        )\n"
          + "    group by\n"
          + "        part.p_brand,\n"
          + "        part.p_type,\n"
          + "        part.p_size\n"
          + ")\n"
          + "order by\n"
          + "    supplier_cnt,\n"
          + "    p_brand,\n"
          + "    p_type,\n"
          + "    p_size;";

  public static final String Q17 =
      "select \n"
          + "    tmp2 / 7 as avg_yearly\n"
          + "from (\n"
          + "    select\n"
          + "        sum(lineitem.l_extendedprice) as tmp2\n"
          + "    from\n"
          + "        lineitem\n"
          + "        join part on part.p_partkey = lineitem.l_partkey\n"
          + "    where\n"
          + "        part.p_brand = 'Brand#13'\n"
          + "        and part.p_container = 'JUMBO PKG'\n"
          + "        and lineitem.l_quantity < (\n"
          + "            select\n"
          + "                0.2 * tmp\n"
          + "            from (\n"
          + "                select\n"
          + "                    avg(l_quantity) as tmp\n"
          + "                from\n"
          + "                    lineitem\n"
          + "                where\n"
          + "                    lineitem.l_partkey = part.p_partkey\n"
          + "            )\n"
          + "        )\n"
          + ");";

  public static final String Q18 =
      "select\n"
          + "    customer.c_name,\n"
          + "    customer.c_custkey,\n"
          + "    orders.o_orderkey,\n"
          + "    orders.o_orderdate,\n"
          + "    orders.o_totalprice,\n"
          + "    sum(lineitem.l_quantity)\n"
          + "from\n"
          + "    customer\n"
          + "    join orders on customer.c_custkey = orders.o_custkey\n"
          + "    join lineitem on orders.o_orderkey = lineitem.l_orderkey\n"
          + "where\n"
          + "    orders.o_orderkey in (\n"
          + "        select\n"
          + "            lineitem.l_orderkey\n"
          + "        from (\n"
          + "            select\n"
          + "                l_orderkey,\n"
          + "                sum(l_quantity)\n"
          + "            from\n"
          + "                lineitem\n"
          + "            group by\n"
          + "                l_orderkey \n"
          + "            having\n"
          + "                sum(lineitem.l_quantity) > 250\n"
          + "        )\n"
          + "    )\n"
          + "group by\n"
          + "    customer.c_name,\n"
          + "    customer.c_custkey,\n"
          + "    orders.o_orderkey,\n"
          + "    orders.o_orderdate,\n"
          + "    orders.o_totalprice\n"
          + "order by\n"
          + "    orders.o_totalprice,\n"
          + "    orders.o_orderdate;";

  public static final String Q19 =
      "select \n"
          + "    sum(tmp) as revenue\n"
          + "from (\n"
          + "    select\n"
          + "        lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp\n"
          + "    from\n"
          + "        lineitem\n"
          + "        join part on part.p_partkey = lineitem.l_partkey\n"
          + "    where (\n"
          + "        part.p_brand = 'Brand#12'\n"
          + "        and (\n"
          + "            part.p_container = 'MED PKG'\n"
          + "            or part.p_container = 'JUMBO CASE'\n"
          + "            or part.p_container = 'MED BAG'\n"
          + "            or part.p_container = 'JUMBO CAN'\n"
          + "        )\n"
          + "        and lineitem.l_quantity >= 1 and lineitem.l_quantity <= 11\n"
          + "        and part.p_size >= 1 and part.p_size < 5\n"
          + "        and (lineitem.l_shipmode = 'AIR' or lineitem.l_shipmode = 'AIR REG')\n"
          + "        and lineitem.l_shipinstruct = 'DELIVER IN PERSON'\n"
          + "    )\n"
          + "    or (\n"
          + "        part.p_brand = 'Brand#22'\n"
          + "        and (\n"
          + "            part.p_container = 'MED PKG'\n"
          + "            or part.p_container = 'MED DRUM'\n"
          + "            or part.p_container = 'MED BAG'\n"
          + "            or part.p_container = 'JUMBO CAN'\n"
          + "        )\n"
          + "        and lineitem.l_quantity >= 17 and lineitem.l_quantity <= 27\n"
          + "        and part.p_size >= 1 and part.p_size < 10\n"
          + "        and (lineitem.l_shipmode = 'AIR' or lineitem.l_shipmode = 'AIR REG')\n"
          + "        and lineitem.l_shipinstruct = 'DELIVER IN PERSON'\n"
          + "    )\n"
          + "    or (\n"
          + "        part.p_brand = 'Brand#32'\n"
          + "        and (\n"
          + "            part.p_container = 'SM BAG'\n"
          + "            or part.p_container = 'SM CASE'\n"
          + "            or part.p_container = 'MED BOX'\n"
          + "            or part.p_container = 'LG PKG'\n"
          + "        )\n"
          + "        and lineitem.l_quantity >= 5 and lineitem.l_quantity <= 15\n"
          + "        and part.p_size >= 1 and part.p_size < 15\n"
          + "        and (lineitem.l_shipmode = 'AIR' or lineitem.l_shipmode = 'AIR REG')\n"
          + "        and lineitem.l_shipinstruct = 'DELIVER IN PERSON'\n"
          + "    )\n"
          + ");";

  public static final String Q20 =
      "select\n"
          + "    supplier.s_name,\n"
          + "    supplier.s_address\n"
          + "from\n"
          + "    supplier\n"
          + "    join nation on supplier.s_nationkey = nation.n_nationkey\n"
          + "where\n"
          + "    supplier.s_suppkey in (\n"
          + "        select\n"
          + "            ps_suppkey\n"
          + "        from\n"
          + "            partsupp\n"
          + "        where\n"
          + "            partsupp.ps_partkey in (\n"
          + "                select\n"
          + "                    p_partkey\n"
          + "                from\n"
          + "                    part\n"
          + "                where\n"
          + "                    part.p_name like '.*yellow.*'\n"
          + "            )\n"
          + "            and partsupp.ps_availqty > (\n"
          + "                select\n"
          + "                    0.5 * tmp\n"
          + "                from (\n"
          + "                    select\n"
          + "                        sum(l_quantity) as tmp\n"
          + "                    from\n"
          + "                        lineitem\n"
          + "                    where\n"
          + "                        lineitem.l_partkey = partsupp.ps_partkey\n"
          + "                        and lineitem.l_suppkey = partsupp.ps_suppkey\n"
          + "                        and lineitem.l_shipdate >= 0\n"
          + "                        and lineitem.l_shipdate < 9124416000\n"
          + "                )\n"
          + "            )\n"
          + "    )\n"
          + "    and nation.n_name = '[NATION]'\n"
          + "order by\n"
          + "    supplier.s_name;";

  public static final String Q22 =
      "select\n"
          + "    customer.c_phone,\n"
          + "    count(*) as numcust,\n"
          + "    sum(customer.c_acctbal) as totacctbal\n"
          + "from (\n"
          + "    select\n"
          + "        c_phone,\n"
          + "        c_acctbal\n"
          + "    from\n"
          + "        customer\n"
          + "    where\n"
          + "        customer.c_acctbal > (\n"
          + "            select\n"
          + "                avg(c_acctbal)\n"
          + "            from\n"
          + "                customer\n"
          + "            where\n"
          + "                customer.c_acctbal > 0.00\n"
          + "            )\n"
          + "            and not exists (\n"
          + "                select\n"
          + "                    *\n"
          + "                from\n"
          + "                    orders\n"
          + "                where\n"
          + "                    orders.o_custkey = customer.c_custkey\n"
          + "            )\n"
          + "        )\n"
          + "group by\n"
          + "    customer.c_phone\n"
          + "order by\n"
          + "    customer.c_phone;";

  public static final Map<String, String> QUERY_MAP = new HashMap<>();

  static {
    QUERY_MAP.put("q1", Q1);
    QUERY_MAP.put("q2", Q2);
    QUERY_MAP.put("q3", Q3);
    QUERY_MAP.put("q4", Q4);
    QUERY_MAP.put("q5", Q5);
    QUERY_MAP.put("q6", Q6);
    QUERY_MAP.put("q9", Q9);
    QUERY_MAP.put("q10", Q10);
    QUERY_MAP.put("q13", Q13);
    QUERY_MAP.put("q16", Q16);
    QUERY_MAP.put("q17", Q17);
    QUERY_MAP.put("q18", Q18);
    QUERY_MAP.put("q19", Q19);
    QUERY_MAP.put("q20", Q20);
    QUERY_MAP.put("q22", Q22);
  }

  public static final ExecutorService pool = Executors.newCachedThreadPool();

  public static final Random random = new Random();

  public static void main(String[] args)
      throws SessionException, ExecutionException, InterruptedException {
    assert args.length >= 4;
    String ip = args[0];
    int port = Integer.parseInt(args[1]);
    int workerNum = Integer.parseInt(args[2]);
    long timeout = Long.parseLong(args[3]);

    List<String> queryList;
    List<String> queryNames;
    if (args.length >= 5) {
      queryList = new ArrayList<>();
      queryNames = new ArrayList<>();
      String[] queryCmds = args[4].split(",");
      for (String cmd : queryCmds) {
        cmd = cmd.trim().toLowerCase();
        if (QUERY_MAP.containsKey(cmd)) {
          queryList.add(QUERY_MAP.get(cmd));
          queryNames.add(cmd);
        }
      }
    } else {
      queryList =
          new ArrayList<>(Arrays.asList(Q1, Q2, Q3, Q5, Q6, Q9, Q10, Q13, Q16, Q17, Q18, Q19));
      queryNames =
          new ArrayList<>(
              Arrays.asList(
                  "q1", "q2", "q3", "q5", "q6", "q9", "q10", "q13", "q16", "q17", "q18", "q19"));
    }

    long startTime = System.currentTimeMillis();
    long endTime = startTime + timeout;
    CountDownLatch latch = new CountDownLatch(workerNum);
    for (int i = 0; i < workerNum; i++) {
      int finalI = i;
      pool.submit(
          () -> {
            Session session = new Session(ip, port);
            try {
              session.openSession();
              long curTime = System.currentTimeMillis();
              while (curTime < endTime) {
                int randomId = random.nextInt(queryList.size());
                String name = queryNames.get(randomId);
                String query = queryList.get(randomId);
                session.executeSql(PREFIX + query);
                curTime = System.currentTimeMillis();
                System.out.printf("worker%s,q%s,%s\n", finalI, name, curTime - startTime);
              }
              session.closeSession();
              latch.countDown();
              System.out.printf("work%s quit.\n", finalI);
            } catch (SessionException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });
    }
    latch.await();
    System.out.println("all workers finished tasks");
  }
}

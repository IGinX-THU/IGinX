package cn.edu.tsinghua.iginx.integration.polybench;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

public class TPCHRunner {
    // host info
    protected static String defaultTestHost = "127.0.0.1";
    protected static int defaultTestPort = 6888;
    protected static String defaultTestUser = "root";
    protected static String defaultTestPass = "root";
    protected static MultiConnection conn;
    public static void TPCRunner(String[] args) {}

    private List<List<String>> csvReader (String filePath) {
        List<List<String>> data = new ArrayList<>();
        boolean skipHeader = true;
        try (Scanner scanner = new Scanner(Paths.get(filePath))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if(skipHeader) {
                    skipHeader = false;
                    continue;
                }
                List<String> row = Arrays.asList(line.split("\\|"));
                data.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(data);
        return data;
    }

    @Test
    public void test() {
        System.out.println("start");
        try {
            conn =
                    new MultiConnection(
                            new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
            conn.openSession();

            // 输出所有存储引擎
            String clusterInfo = conn.executeSql("SHOW CLUSTER INFO;").getResultInString(false, "");
            System.out.println(clusterInfo);

            // 添加存储引擎
            System.out.println("start adding storage engine");
            long startTime = System.currentTimeMillis();
            Map<String, String> pgMap = new HashMap<>();
            pgMap.put("has_data", "true");
            pgMap.put("is_read_only", "true");
            pgMap.put("username", "postgres");
            pgMap.put("password", "postgres");
            pgMap = Collections.unmodifiableMap(pgMap);
            conn.addStorageEngine(
                    "127.0.0.1",
                    5432,
                    StorageEngineType.postgresql,
                    pgMap
            );
            Map<String, String> mongoMap = new HashMap<>();
            mongoMap.put("has_data", "true");
            mongoMap.put("is_read_only", "true");
            mongoMap.put("schema.sample.size", "1000");
            mongoMap.put("dummy.sample.size", "0");
            conn.addStorageEngine(
                    "127.0.0.1",
                    27017,
                    StorageEngineType.mongodb,
                    mongoMap
            );
            System.out.println("end adding storage engine, time cost: " + (System.currentTimeMillis() - startTime) + "ms");

            // 输出所有存储引擎
            clusterInfo = conn.executeSql("SHOW CLUSTER INFO;").getResultInString(false, "");
            System.out.println(clusterInfo);

            String sqlString = "select \n" +
                    "    nation.n_name, revenue\n" +
                    "from (\n" +
                    "    select\n" +
                    "        nation.n_name,\n" +
                    "        sum(tmp) as revenue\n" +
                    "    from (\n" +
                    "        select\n" +
                    "            nation.n_name,\n" +
                    "            mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp\n" +
                    "        from\n" +
                    "            postgres.customer\n" +
                    "            join mongotpch.orders on postgres.customer.c_custkey = mongotpch.orders.o_custkey\n" +
                    "            join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey\n" +
                    "            join postgres.supplier on mongotpch.lineitem.l_suppkey = postgres.supplier.s_suppkey and postgres.customer.c_nationkey = postgres.supplier.s_nationkey\n" +
                    "            join nation on postgres.supplier.s_nationkey = nation.n_nationkey\n" +
                    "            join postgres.region on nation.n_regionkey = postgres.region.r_regionkey\n" +
                    "        where\n" +
                    "            postgres.region.r_name = \"ASIA\"\n" +
                    "            and mongotpch.orders.o_orderdate >= 757353600000\n" +
                    "            and mongotpch.orders.o_orderdate < 788889600000\n" +
                    "    )\n" +
                    "    group by\n" +
                    "        nation.n_name\n" +
                    ")\n" +
                    "order by\n" +
                    "    revenue desc;";

            // 开始 tpch 查询
            System.out.println("start tpch query");
            startTime = System.currentTimeMillis();

            // 执行查询语句
            SessionExecuteSqlResult result = conn.executeSql(sqlString);
            result.print(false, "");
            List<List<Object>> values = result.getValues();
            List<List<String>> answers = csvReader("src/test/resources/polybench/sf0.1/q05.csv");
            if (values.size() != answers.size()) {
                throw new RuntimeException("size not equal");
            }
            for (int i = 0; i < values.size(); i++) {
                String nation = new String((byte[]) values.get(i).get(0), StandardCharsets.UTF_8);
                double number = (double) values.get(i).get(1);

                System.out.println("nation： " + nation);
                System.out.println("Number: " + number);

                String answerString = answers.get(i).get(0);
                double answerNumber = Double.parseDouble(answers.get(i).get(1));
                System.out.println("Answer string: " + answerString);
                System.out.println("Answer number: " + answerNumber);

                assert nation.equals(answerString);
                assert answerNumber - number < 1e-5 && number - answerNumber < 1e-5;
            }

            // 关闭会话
            conn.closeSession();
            System.out.println("end tpch query, time cost: " + (System.currentTimeMillis() - startTime) + "ms");
        } catch (SessionException e) {
            throw new RuntimeException(e);
        }
    }
}

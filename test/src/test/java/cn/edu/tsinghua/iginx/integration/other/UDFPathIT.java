package cn.edu.tsinghua.iginx.integration.other;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFPathIT {

  private static final Logger logger = LoggerFactory.getLogger(UDFPathIT.class);

  private static Session session;

  // host info
  private static String defaultTestHost = "127.0.0.1";
  private static int defaultTestPort = 6888;
  private static String defaultTestUser = "root";
  private static String defaultTestPass = "root";

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  @Test
  public void testUDFFuncList() {
    String statement = "show register python task";
    String expectedRes =
        "Register task infos:\n"
            + "+------------+--------------+--------------------+-------+--------+\n"
            + "|        NAME|    CLASS_NAME|           FILE_NAME|     IP|UDF_TYPE|\n"
            + "+------------+--------------+--------------------+-------+--------+\n"
            + "|     udf_sum|        UDFSum|          udf_sum.py|0.0.0.0|    UDAF|\n"
            + "|         cos|        UDFCos|         udtf_cos.py|0.0.0.0|    UDTF|\n"
            + "|reverse_rows|UDFReverseRows|udsf_reverse_rows.py|0.0.0.0|    UDSF|\n"
            + "|     udf_max|        UDFMax|          udf_max.py|0.0.0.0|    UDAF|\n"
            + "|   udf_count|      UDFCount|        udf_count.py|0.0.0.0|    UDAF|\n"
            + "|    multiply|   UDFMultiply|    udtf_multiply.py|0.0.0.0|    UDTF|\n"
            + "|     udf_min|        UDFMin|          udf_min.py|0.0.0.0|    UDAF|\n"
            + "|     udf_avg|        UDFAvg|          udf_avg.py|0.0.0.0|    UDAF|\n"
            + "+------------+--------------+--------------------+-------+--------+\n";

    SQLTestTools.executeAndCompare(session, statement, expectedRes);
  }

  // Test cos() to make sure udf scripts really exists.
  @Test
  public void testUDFFunction() throws SessionException, ExecutionException {
    String insertStatement = "insert into aa.bb(key, value) values(1, 2)";
    session.executeSql(insertStatement);

    String statement = "select cos(value) from aa.bb";
    String expectedRes =
        "ResultSets:\n"
            + "+---+-------------------+\n"
            + "|key|   cos(aa.bb.value)|\n"
            + "+---+-------------------+\n"
            + "|  1|-0.4161468365471424|\n"
            + "+---+-------------------+\n"
            + "Total line number = 1";

    SQLTestTools.executeAndCompare(session, statement, expectedRes);
  }
}

package cn.edu.tsinghua.iginx.integration.other;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFPathIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFPathIT.class);

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
    String statement = "SHOW FUNCTIONS;";
    String expectedRes =
        "Functions info:\n"
            + "+----------------+---------------+---------------------+-------+--------+\n"
            + "|            NAME|     CLASS_NAME|            FILE_NAME|     IP|UDF_TYPE|\n"
            + "+----------------+---------------+---------------------+-------+--------+\n"
            + "|         udf_sum|         UDFSum|           udf_sum.py|0.0.0.0|    UDAF|\n"
            + "|udf_max_with_key|  UDFMaxWithKey|  udf_max_with_key.py|0.0.0.0|    UDAF|\n"
            + "|             cos|         UDFCos|          udtf_cos.py|0.0.0.0|    UDTF|\n"
            + "|    columnExpand|UDFColumnExpand|udtf_column_expand.py|0.0.0.0|    UDTF|\n"
            + "|         udf_min|         UDFMin|           udf_min.py|0.0.0.0|    UDAF|\n"
            + "|         udf_avg|         UDFAvg|           udf_avg.py|0.0.0.0|    UDAF|\n"
            + "|     key_add_one|   UDFKeyAddOne|  udtf_key_add_one.py|0.0.0.0|    UDTF|\n"
            + "|             pow|         UDFPow|          udtf_pow.py|0.0.0.0|    UDTF|\n"
            + "|    reverse_rows| UDFReverseRows| udsf_reverse_rows.py|0.0.0.0|    UDSF|\n"
            + "|         udf_max|         UDFMax|           udf_max.py|0.0.0.0|    UDAF|\n"
            + "|       transpose|   UDFTranspose|    udsf_transpose.py|0.0.0.0|    UDSF|\n"
            + "|       udf_count|       UDFCount|         udf_count.py|0.0.0.0|    UDAF|\n"
            + "|        multiply|    UDFMultiply|     udtf_multiply.py|0.0.0.0|    UDTF|\n"
            + "+----------------+---------------+---------------------+-------+--------+\n";

    SQLTestTools.executeAndCompare(session, statement, expectedRes);
  }
}

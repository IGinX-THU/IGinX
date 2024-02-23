package cn.edu.tsinghua.iginx.integration.other;

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
  private static final String defaultTestHost = "127.0.0.1";
  private static final int defaultTestPort = 6888;
  private static final String defaultTestUser = "root";
  private static final String defaultTestPass = "root";

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
    String statement = "show register python task;";
    String expectedRes = SQLTestTools.execute(session, statement);
    assert expectedRes.contains("Register task infos:");
  }
}

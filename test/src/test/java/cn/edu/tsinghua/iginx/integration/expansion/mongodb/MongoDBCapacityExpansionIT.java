package cn.edu.tsinghua.iginx.integration.expansion.mongodb;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.expPort;
import static cn.edu.tsinghua.iginx.integration.tool.DBType.mongodb;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBCapacityExpansionIT extends BaseCapacityExpansionIT {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBCapacityExpansionIT.class);

  public MongoDBCapacityExpansionIT() {
    super(mongodb, null);
    Constant.oriPort = 27017;
    Constant.expPort = 27018;
    Constant.readOnlyPort = 27019;
  }

  @Override
  protected void testQueryHistoryDataOriHasData() {
    String statement = "select * from mn";
    String expect =
        "ResultSets:\n"
            + "+---+-----------+-------------------+------------------------+\n"
            + "|key|mn.wf01._id|mn.wf01.wt01.status|mn.wf01.wt01.temperature|\n"
            + "+---+-----------+-------------------+------------------------+\n"
            + "|  1|          0|               true|                   15.27|\n"
            + "|  2|          1|              false|                   20.71|\n"
            + "+---+-----------+-------------------+------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select count(*) from mn.wf01";
    expect =
        "ResultSets:\n"
            + "+------------------+--------------------------+-------------------------------+\n"
            + "|count(mn.wf01._id)|count(mn.wf01.wt01.status)|count(mn.wf01.wt01.temperature)|\n"
            + "+------------------+--------------------------+-------------------------------+\n"
            + "|                 2|                         2|                              2|\n"
            + "+------------------+--------------------------+-------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  @Override
  protected void testQueryHistoryDataExpHasData() {
    String statement = "select * from nt.wf03";
    String expect =
        "ResultSets:\n"
            + "+---+-----------+-------------------+\n"
            + "|key|nt.wf03._id|nt.wf03.wt01.status|\n"
            + "+---+-----------+-------------------+\n"
            + "|  1|          0|               true|\n"
            + "|  2|          1|              false|\n"
            + "+---+-----------+-------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from nt.wf04";
    expect =
        "ResultSets:\n"
            + "+---+-----------+------------------------+\n"
            + "|key|nt.wf04._id|nt.wf04.wt01.temperature|\n"
            + "+---+-----------+------------------------+\n"
            + "|  1|          0|                   66.23|\n"
            + "|  2|          1|                   77.71|\n"
            + "+---+-----------+------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  @Override
  protected void testQueryHistoryDataReadOnly() {
    String statement = "select count(*) from d0.c0.objects";
    String expect =
        "ResultSets:\n"
            + "+----------------------------------+--------------------------------------+--------------------------------------+------------------------------+---------------------------------+--------------------------------+----------------------------------+-----------------------------------+-------------------------+-----------------------------------+--------------------------------+------------------------------+---------------------------------+--------------------------------+----------------------------------+-----------------------------------+-------------------------+-----------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+--------------------------------+\n"
            + "|count(d0.c0.objects.0.bitmap.data)|count(d0.c0.objects.0.bitmap.origin.0)|count(d0.c0.objects.0.bitmap.origin.1)|count(d0.c0.objects.0.classId)|count(d0.c0.objects.0.classTitle)|count(d0.c0.objects.0.createdAt)|count(d0.c0.objects.0.description)|count(d0.c0.objects.0.geometryType)|count(d0.c0.objects.0.id)|count(d0.c0.objects.0.labelerLogin)|count(d0.c0.objects.0.updatedAt)|count(d0.c0.objects.1.classId)|count(d0.c0.objects.1.classTitle)|count(d0.c0.objects.1.createdAt)|count(d0.c0.objects.1.description)|count(d0.c0.objects.1.geometryType)|count(d0.c0.objects.1.id)|count(d0.c0.objects.1.labelerLogin)|count(d0.c0.objects.1.points.exterior.0.0)|count(d0.c0.objects.1.points.exterior.0.1)|count(d0.c0.objects.1.points.exterior.1.0)|count(d0.c0.objects.1.points.exterior.1.1)|count(d0.c0.objects.1.updatedAt)|\n"
            + "+----------------------------------+--------------------------------------+--------------------------------------+------------------------------+---------------------------------+--------------------------------+----------------------------------+-----------------------------------+-------------------------+-----------------------------------+--------------------------------+------------------------------+---------------------------------+--------------------------------+----------------------------------+-----------------------------------+-------------------------+-----------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+--------------------------------+\n"
            + "|                                 1|                                     1|                                     1|                             1|                                1|                               1|                                 1|                                  1|                        1|                                  1|                               1|                             1|                                1|                               1|                                 1|                                  1|                        1|                                  1|                                         1|                                         1|                                         1|                                         1|                               1|\n"
            + "+----------------------------------+--------------------------------------+--------------------------------------+------------------------------+---------------------------------+--------------------------------+----------------------------------+-----------------------------------+-------------------------+-----------------------------------+--------------------------------+------------------------------+---------------------------------+--------------------------------+----------------------------------+-----------------------------------+-------------------------+-----------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+--------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select area, bbox from d0.c0.annotations.0";
    expect =
        "ResultSets:\n"
            + "+---+------------------------+---------------------------------------------------------------------------+\n"
            + "|key|d0.c0.annotations.0.area|                                                   d0.c0.annotations.0.bbox|\n"
            + "+---+------------------------+---------------------------------------------------------------------------+\n"
            + "|  1|       468549.3681311881|[4.106930693069307,12.319672131147541,1025.7059405940595,478.4139344262295]|\n"
            + "+---+------------------------+---------------------------------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select information from d0.c0";
    expect =
        "ResultSets:\n"
            + "+---+-------------------------------------------------------------------------------------------------------------------------------------------+\n"
            + "|key|                                                                                                                          d0.c0.information|\n"
            + "+---+-------------------------------------------------------------------------------------------------------------------------------------------+\n"
            + "|  1|{\"year\": 2022, \"version\": \"1.0\", \"description\": \"\", \"contributor\": \"Label Studio\", \"url\": \"\", \"date_created\": \"2022-12-12 08:37:26.832616\"}|\n"
            + "+---+-------------------------------------------------------------------------------------------------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select name from d0.c0.categories";
    expect =
        "ResultSets:\n"
            + "+---+--------------------------------------------+\n"
            + "|key|                       d0.c0.categories.name|\n"
            + "+---+--------------------------------------------+\n"
            + "|  1|[\"Blur\",\"Phone\",\"ReflectLight\",\"Reflection\"]|\n"
            + "+---+--------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select 0 from d0.c0.annotations.bbox";
    expect =
        "ResultSets:\n"
            + "+---+--------------------------------------------------------+\n"
            + "|key|                                d0.c0.annotations.bbox.0|\n"
            + "+---+--------------------------------------------------------+\n"
            + "|  1|[4.106930693069307,39.015841584158416,693.0445544554455]|\n"
            + "+---+--------------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select 0 from d0.c0.annotations.segmentation.*";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------------------------------+\n"
            + "|key|                    d0.c0.annotations.segmentation.0.0|\n"
            + "+---+------------------------------------------------------+\n"
            + "|  1|[4.106930693069307,57.4970297029703,693.0445544554455]|\n"
            + "+---+------------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select 0 from d0.c0.annotations.segmentation.*";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------------------------------+\n"
            + "|key|                    d0.c0.annotations.segmentation.0.0|\n"
            + "+---+------------------------------------------------------+\n"
            + "|  1|[4.106930693069307,57.4970297029703,693.0445544554455]|\n"
            + "+---+------------------------------------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select exterior from d0.c0.objects.points";
    expect =
        "ResultSets:\n"
            + "+---+-----------------------------+\n"
            + "|key|d0.c0.objects.points.exterior|\n"
            + "+---+-----------------------------+\n"
            + "|  1|        [[[0,236],[582,872]]]|\n"
            + "+---+-----------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  @Override
  protected void testAddAndRemoveStorageEngineWithPrefix() {
    addStorageEngine(expPort, true, true, "nt.wf03", "p1");
    addStorageEngine(expPort, true, true, "nt.wf03", "p2");
    addStorageEngine(expPort, true, true, "nt.wf03", null);

    String res = addStorageEngine(expPort, true, true, "nt.wf03", null);
    if (res != null && !res.contains("unexpected repeated add")) {
      fail();
    }
    addStorageEngine(expPort, true, true, "nt.wf03", "p3");
    addStorageEngine(expPort, true, true, "nt.wf04", "p3");

    String statement = "select * from p1.nt.wf03";
    String expect =
        "ResultSets:\n"
            + "+---+--------------+----------------------+\n"
            + "|key|p1.nt.wf03._id|p1.nt.wf03.wt01.status|\n"
            + "+---+--------------+----------------------+\n"
            + "|  1|             0|                  true|\n"
            + "|  2|             1|                 false|\n"
            + "+---+--------------+----------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from p2.nt.wf03";
    expect =
        "ResultSets:\n"
            + "+---+--------------+----------------------+\n"
            + "|key|p2.nt.wf03._id|p2.nt.wf03.wt01.status|\n"
            + "+---+--------------+----------------------+\n"
            + "|  1|             0|                  true|\n"
            + "|  2|             1|                 false|\n"
            + "+---+--------------+----------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from nt.wf03";
    expect =
        "ResultSets:\n"
            + "+---+-----------+-------------------+\n"
            + "|key|nt.wf03._id|nt.wf03.wt01.status|\n"
            + "+---+-----------+-------------------+\n"
            + "|  1|          0|               true|\n"
            + "|  2|          1|              false|\n"
            + "+---+-----------+-------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from p3.nt.wf03";
    expect =
        "ResultSets:\n"
            + "+---+--------------+----------------------+\n"
            + "|key|p3.nt.wf03._id|p3.nt.wf03.wt01.status|\n"
            + "+---+--------------+----------------------+\n"
            + "|  1|             0|                  true|\n"
            + "|  2|             1|                 false|\n"
            + "+---+--------------+----------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    try {
      session.removeHistoryDataSource(
          Arrays.asList(
              new RemovedStorageEngineInfo("127.0.0.1", expPort, "p2", "nt.wf03"),
              new RemovedStorageEngineInfo("127.0.0.1", expPort, "p3", "nt.wf03")));
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through session api error: {}", e.getMessage());
    }

    statement = "select * from p2.nt.wf03";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from p3.nt.wf04";
    expect =
        "ResultSets:\n"
            + "+---+--------------+---------------------------+\n"
            + "|key|p3.nt.wf04._id|p3.nt.wf04.wt01.temperature|\n"
            + "+---+--------------+---------------------------+\n"
            + "|  1|             0|                      66.23|\n"
            + "|  2|             1|                      77.71|\n"
            + "+---+--------------+---------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    String removeStatement = "remove historydataresource (\"127.0.0.1\", %d, \"%s\", \"%s\")";
    try {
      session.executeSql(String.format(removeStatement, expPort, "p1", "nt.wf03"));
      session.executeSql(String.format(removeStatement, expPort, "p3", "nt.wf04"));
      session.executeSql(String.format(removeStatement, expPort, "", "nt.wf03"));
    } catch (ExecutionException | SessionException e) {
      logger.error("remove history data source through sql error: {}", e.getMessage());
    }

    statement = "select * from p1.nt.wf03";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    try {
      session.executeSql(String.format(removeStatement, expPort, "p1", "nt.wf03"));
    } catch (ExecutionException | SessionException e) {
      if (!e.getMessage().contains("dummy storage engine does not exist.")) {
        logger.error(
            "'remove history data source should throw error when removing the node that does not exist");
        fail();
      }
    }
  }
}

package cn.edu.tsinghua.iginx.integration.expansion.mongodb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.mongodb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
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
  protected void testQuerySpecialHistoryData() {
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
            + "|  2|{\"year\": 2022, \"version\": \"1.0\", \"description\": \"\", \"contributor\": \"Label Studio\", \"url\": \"\", \"date_created\": \"2022-12-12 08:37:26.832616\"}|\n"
            + "+---+-------------------------------------------------------------------------------------------------------------------------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select name from d0.c0.categories";
    expect =
        "ResultSets:\n"
            + "+---+------------------------------------+\n"
            + "|key|               d0.c0.categories.name|\n"
            + "+---+------------------------------------+\n"
            + "|  1|[Blur,Phone,ReflectLight,Reflection]|\n"
            + "+---+------------------------------------+\n"
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
}

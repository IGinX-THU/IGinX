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
    testProject();
    testFilter();
  }

  private static void testProject() {
    // wildcard query
    String statement = "select * from d0.c0.objects;";
    String expect =
        "ResultSets:\n"
            + "+----------+-----------------------------+-----------------------------+---------------------+------------------------+------------------------+-------------------------+--------------------------+----------------+--------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+------------------------+\n"
            + "|       key|d0.c0.objects.bitmap.origin.0|d0.c0.objects.bitmap.origin.1|d0.c0.objects.classId|d0.c0.objects.classTitle| d0.c0.objects.createdAt|d0.c0.objects.description|d0.c0.objects.geometryType|d0.c0.objects.id|d0.c0.objects.labelerLogin|d0.c0.objects.points.exterior.0.0|d0.c0.objects.points.exterior.0.1|d0.c0.objects.points.exterior.1.0|d0.c0.objects.points.exterior.1.1| d0.c0.objects.updatedAt|\n"
            + "+----------+-----------------------------+-----------------------------+---------------------+------------------------+------------------------+-------------------------+--------------------------+----------------+--------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+------------------------+\n"
            + "|4294967296|                          535|                           66|              1661571|                  person|2020-08-07T11:09:51.054Z|                         |                    bitmap|       497521359|                    alexxx|                             null|                             null|                             null|                             null|2020-08-07T11:09:51.054Z|\n"
            + "|4294967297|                         null|                         null|              1661574|                    bike|2020-08-07T11:09:51.054Z|                         |                 rectangle|       497521358|                    alexxx|                                0|                              236|                              582|                              872|2020-08-07T11:09:51.054Z|\n"
            + "+----------+-----------------------------+-----------------------------+---------------------+------------------------+------------------------+-------------------------+--------------------------+----------------+--------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // key assignment
    statement = "select id from d0.c0.images;";
    expect =
        "ResultSets:\n"
            + "+----------+---------------+\n"
            + "|       key|d0.c0.images.id|\n"
            + "+----------+---------------+\n"
            + "|4294967296|              0|\n"
            + "|8589934592|              0|\n"
            + "|8589934593|              1|\n"
            + "+----------+---------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // array index
    statement = "select `3` from d0.c0.annotations.segmentation.`0`;";
    expect =
        "ResultSets:\n"
            + "+----------+----------------------------------+\n"
            + "|       key|d0.c0.annotations.segmentation.0.3|\n"
            + "+----------+----------------------------------+\n"
            + "|4294967296|                 25.66598360655738|\n"
            + "|4294967297|                 55.43852459016394|\n"
            + "|4294967298|                26.692622950819672|\n"
            + "+----------+----------------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // field name
    statement = "select area, category_id from d0.c0.annotations;";
    expect =
        "ResultSets:\n"
            + "+----------+----------------------+-----------------------------+\n"
            + "|       key|d0.c0.annotations.area|d0.c0.annotations.category_id|\n"
            + "+----------+----------------------+-----------------------------+\n"
            + "|4294967296|     468549.3681311881|                            0|\n"
            + "|4294967297|    188048.61386138643|                            3|\n"
            + "|4294967298|    33132.500309405965|                            3|\n"
            + "+----------+----------------------+-----------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // type convert: string -> number
    statement = "select year from d0.c0.information;";
    expect =
        "ResultSets:\n"
            + "+-----------+----------------------+\n"
            + "|        key|d0.c0.information.year|\n"
            + "+-----------+----------------------+\n"
            + "| 4294967296|                  2021|\n"
            + "| 8589934592|                  2022|\n"
            + "|12884901888|                  2023|\n"
            + "+-----------+----------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // type convert: number -> string
    statement = "select version from d0.c0.information;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------------------------+\n"
            + "|        key|d0.c0.information.version|\n"
            + "+-----------+-------------------------+\n"
            + "| 4294967296|                      1.0|\n"
            + "| 8589934592|                      1.0|\n"
            + "|12884901888|                      3.0|\n"
            + "+-----------+-------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // type convert: Double over Long
    statement = "select score from d0.c0.information;";
    expect =
        "ResultSets:\n"
            + "+-----------+-----------------------+\n"
            + "|        key|d0.c0.information.score|\n"
            + "+-----------+-----------------------+\n"
            + "| 4294967296|                    1.0|\n"
            + "| 8589934592|                    2.0|\n"
            + "|12884901888|                    3.1|\n"
            + "+-----------+-----------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // type convert: bson -> json
    statement = "select _id from d0.c0;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------------------------------------------+\n"
            + "|        key|                                  d0.c0._id|\n"
            + "+-----------+-------------------------------------------+\n"
            + "| 4294967296|{\"$\": {\"$oid\": \"652f4577a162014f74419b7f\"}}|\n"
            + "| 8589934592|{\"$\": {\"$oid\": \"652f4577a162014f74419b80\"}}|\n"
            + "|12884901888|{\"$\": {\"$oid\": \"652f4577a162014f74419b81\"}}|\n"
            + "+-----------+-------------------------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testFilter() {
    String statement = "select i from d1.c1 where i < 3;";
    String expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.i|\n"
            + "+-----------+-------+\n"
            + "| 4294967296|      0|\n"
            + "| 8589934592|      1|\n"
            + "|12884901888|      2|\n"
            + "+-----------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select b from d1.c1 where b = true;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.b|\n"
            + "+-----------+-------+\n"
            + "| 4294967296|   true|\n"
            + "|12884901888|   true|\n"
            + "|21474836480|   true|\n"
            + "+-----------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d1.c1 where f > 2;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.f|\n"
            + "+-----------+-------+\n"
            + "|12884901888|    2.1|\n"
            + "|17179869184|    3.1|\n"
            + "|21474836480|    4.1|\n"
            + "+-----------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d1.c1 where f > 2;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.f|\n"
            + "+-----------+-------+\n"
            + "|12884901888|    2.1|\n"
            + "|17179869184|    3.1|\n"
            + "|21474836480|    4.1|\n"
            + "+-----------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select f from d1.c1 where f > 1.0 and f < 2.9;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.f|\n"
            + "+-----------+-------+\n"
            + "| 8589934592|    1.1|\n"
            + "|12884901888|    2.1|\n"
            + "+-----------+-------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select s from d1.c1 where s = '1st';";
    expect =
        "ResultSets:\n"
            + "+----------+-------+\n"
            + "|       key|d1.c1.s|\n"
            + "+----------+-------+\n"
            + "|4294967296|    1st|\n"
            + "+----------+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select s from d1.c1 where s LIKE '.*th';";
    expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.s|\n"
            + "+-----------+-------+\n"
            + "|12884901888|    3th|\n"
            + "|17179869184|    4th|\n"
            + "|21474836480|    5th|\n"
            + "+-----------+-------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement =
        "select * from d1.c1 where _id = '{\"$\": {\"$oid\": \"000000000000000000000003\"}}';";
    expect =
        "ResultSets:\n"
            + "+-----------+-------------------------------------------+-------+-------+-------+-------+\n"
            + "|        key|                                  d1.c1._id|d1.c1.b|d1.c1.f|d1.c1.i|d1.c1.s|\n"
            + "+-----------+-------------------------------------------+-------+-------+-------+-------+\n"
            + "|17179869184|{\"$\": {\"$oid\": \"000000000000000000000003\"}}|  false|    3.1|      3|    4th|\n"
            + "+-----------+-------------------------------------------+-------+-------+-------+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }
}

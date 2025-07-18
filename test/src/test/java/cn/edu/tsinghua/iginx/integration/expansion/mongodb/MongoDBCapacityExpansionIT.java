/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.integration.expansion.mongodb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.mongodb;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBCapacityExpansionIT extends BaseCapacityExpansionIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBCapacityExpansionIT.class);

  public MongoDBCapacityExpansionIT() {
    super(mongodb, (String) null, new MongoDBHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
  }

  @Override
  protected void testShowColumnsInExpansion(boolean before) {
    String statement = "SHOW COLUMNS nt.wf03.*;";
    String expected =
        "Columns:\n"
            + "+--------------------+--------+\n"
            + "|                Path|DataType|\n"
            + "+--------------------+--------+\n"
            + "|         nt.wf03._id| INTEGER|\n"
            + "|nt.wf03.wt01.status2|    LONG|\n"
            + "+--------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                  Path|DataType|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                               b.b._id| INTEGER|\n"
              + "|                                                                                 b.b.b|    LONG|\n"
              + "|                                                                        ln.wf02.status| BOOLEAN|\n"
              + "|                                                                       ln.wf02.version|  BINARY|\n"
              + "|                                                                           nt.wf03._id| INTEGER|\n"
              + "|                                                                  nt.wf03.wt01.status2|    LONG|\n"
              + "|                                                                           nt.wf04._id| INTEGER|\n"
              + "|                                                              nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "|                          zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz._id| INTEGER|\n"
              + "|zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz|    LONG|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "Total line number = 10\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                  Path|DataType|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                               b.b._id| INTEGER|\n"
              + "|                                                                                 b.b.b|    LONG|\n"
              + "|                                                                        ln.wf02.status| BOOLEAN|\n"
              + "|                                                                       ln.wf02.version|  BINARY|\n"
              + "|                                                                           nt.wf03._id| INTEGER|\n"
              + "|                                                                  nt.wf03.wt01.status2|    LONG|\n"
              + "|                                                                           nt.wf04._id| INTEGER|\n"
              + "|                                                              nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "|                                                                        p1.nt.wf03._id| INTEGER|\n"
              + "|                                                               p1.nt.wf03.wt01.status2|    LONG|\n"
              + "|                          zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz._id| INTEGER|\n"
              + "|zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz|    LONG|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "Total line number = 12\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    if (before) {
      statement = "SHOW COLUMNS p1.*;";
      expected =
          "Columns:\n"
              + "+----+--------+\n"
              + "|Path|DataType|\n"
              + "+----+--------+\n"
              + "+----+--------+\n"
              + "Empty set.\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      statement = "SHOW COLUMNS p1.*;";
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|         p1.nt.wf03._id| INTEGER|\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS *.wf03.wt01.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------+--------+\n"
              + "|                Path|DataType|\n"
              + "+--------------------+--------+\n"
              + "|nt.wf03.wt01.status2|    LONG|\n"
              + "+--------------------+--------+\n"
              + "Total line number = 1\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|   nt.wf03.wt01.status2|    LONG|\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }

  @Override
  protected void testShowColumnsRemoveStorageEngine(boolean before) {
    String statement = "SHOW COLUMNS p1.*, p2.*, p3.*;";
    String expected;
    if (before) {
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|             p1.nt.wf03._id| INTEGER|\n"
              + "|    p1.nt.wf03.wt01.status2|    LONG|\n"
              + "|             p2.nt.wf03._id| INTEGER|\n"
              + "|    p2.nt.wf03.wt01.status2|    LONG|\n"
              + "|             p3.nt.wf03._id| INTEGER|\n"
              + "|    p3.nt.wf03.wt01.status2|    LONG|\n"
              + "|             p3.nt.wf04._id| INTEGER|\n"
              + "|p3.nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 8\n";
    } else { // 移除schemaPrefix为p2及p3，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|             p1.nt.wf03._id| INTEGER|\n"
              + "|    p1.nt.wf03.wt01.status2|    LONG|\n"
              + "|             p3.nt.wf04._id| INTEGER|\n"
              + "|p3.nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 4\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testProject();
    testFilter();
    testProjectNonexistent();
  }

  private static void testProject() {
    // wildcard query
    String statement = "select * from d0.c0.objects;";
    String expect =
        "ResultSets:\n"
            + "+----------+------------------------------+-----------------------------+-----------------------------+---------------------+------------------------+------------------------+-------------------------+--------------------------+----------------+--------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+------------------------+\n"
            + "|       key|     d0.c0.objects.bitmap.data|d0.c0.objects.bitmap.origin.0|d0.c0.objects.bitmap.origin.1|d0.c0.objects.classId|d0.c0.objects.classTitle| d0.c0.objects.createdAt|d0.c0.objects.description|d0.c0.objects.geometryType|d0.c0.objects.id|d0.c0.objects.labelerLogin|d0.c0.objects.points.exterior.0.0|d0.c0.objects.points.exterior.0.1|d0.c0.objects.points.exterior.1.0|d0.c0.objects.points.exterior.1.1| d0.c0.objects.updatedAt|\n"
            + "+----------+------------------------------+-----------------------------+-----------------------------+---------------------+------------------------+------------------------+-------------------------+--------------------------+----------------+--------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+------------------------+\n"
            + "|4294967296|eJwBgQd++IlQTkcNChoKAAAADUlIRF|                          535|                           66|              1661571|                  person|2020-08-07T11:09:51.054Z|                         |                    bitmap|       497521359|                    alexxx|                             null|                             null|                             null|                             null|2020-08-07T11:09:51.054Z|\n"
            + "|4294967297|                          null|                         null|                         null|              1661574|                    bike|2020-08-07T11:09:51.054Z|                         |                 rectangle|       497521358|                    alexxx|                                0|                              236|                              582|                              872|2020-08-07T11:09:51.054Z|\n"
            + "+----------+------------------------------+-----------------------------+-----------------------------+---------------------+------------------------+------------------------+-------------------------+--------------------------+----------------+--------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+------------------------+\n"
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

    // unwind array
    statement = "select information.contributor, objects.geometryType from d0.c0;";
    expect =
        "ResultSets:\n"
            + "+-----------+-----------------------------+--------------------------+\n"
            + "|        key|d0.c0.information.contributor|d0.c0.objects.geometryType|\n"
            + "+-----------+-----------------------------+--------------------------+\n"
            + "| 4294967296|                 Label Studio|                    bitmap|\n"
            + "| 4294967297|                         null|                 rectangle|\n"
            + "| 8589934592|                 Label Studio|                      null|\n"
            + "|12884901888|                 Label Studio|                      null|\n"
            + "+-----------+-----------------------------+--------------------------+\n"
            + "Total line number = 4\n";
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
            + "+-----------+------------------------------------+\n"
            + "|        key|                           d0.c0._id|\n"
            + "+-----------+------------------------------------+\n"
            + "| 4294967296|ObjectId(\"652f4577a162014f74419b7f\")|\n"
            + "| 8589934592|ObjectId(\"652f4577a162014f74419b80\")|\n"
            + "|12884901888|ObjectId(\"652f4577a162014f74419b81\")|\n"
            + "+-----------+------------------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    // wildcard match
    statement = "select _id from d0.*;";
    expect =
        "ResultSets:\n"
            + "+-----------+------------------------------------+\n"
            + "|        key|                           d0.c0._id|\n"
            + "+-----------+------------------------------------+\n"
            + "| 4294967296|ObjectId(\"652f4577a162014f74419b7f\")|\n"
            + "| 8589934592|ObjectId(\"652f4577a162014f74419b80\")|\n"
            + "|12884901888|ObjectId(\"652f4577a162014f74419b81\")|\n"
            + "+-----------+------------------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select _id from *.c1;";
    expect =
        "ResultSets:\n"
            + "+-----------+------------------------------------+\n"
            + "|        key|                           d1.c1._id|\n"
            + "+-----------+------------------------------------+\n"
            + "| 4294967296|ObjectId(\"000000000000000000000000\")|\n"
            + "| 8589934592|ObjectId(\"000000000000000000000001\")|\n"
            + "|12884901888|ObjectId(\"000000000000000000000002\")|\n"
            + "|17179869184|ObjectId(\"000000000000000000000003\")|\n"
            + "|21474836480|ObjectId(\"000000000000000000000004\")|\n"
            + "+-----------+------------------------------------+\n"
            + "Total line number = 5\n";
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

    statement = "select i from d1.c1 where key > 0 and key <= 8589934592;";
    expect =
        "ResultSets:\n"
            + "+----------+-------+\n"
            + "|       key|d1.c1.i|\n"
            + "+----------+-------+\n"
            + "|4294967296|      0|\n"
            + "|8589934592|      1|\n"
            + "+----------+-------+\n"
            + "Total line number = 2\n";
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

    statement = "select s from d1.c1 where s LIKE '.*th' and key < 21474836480;";
    expect =
        "ResultSets:\n"
            + "+-----------+-------+\n"
            + "|        key|d1.c1.s|\n"
            + "+-----------+-------+\n"
            + "|12884901888|    3th|\n"
            + "|17179869184|    4th|\n"
            + "+-----------+-------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from d1.c1 where _id = 'ObjectId(\"000000000000000000000003\")';";
    expect =
        "ResultSets:\n"
            + "+-----------+------------------------------------+-------+-------+-------+-------+\n"
            + "|        key|                           d1.c1._id|d1.c1.b|d1.c1.f|d1.c1.i|d1.c1.s|\n"
            + "+-----------+------------------------------------+-------+-------+-------+-------+\n"
            + "|17179869184|ObjectId(\"000000000000000000000003\")|  false|    3.1|      3|    4th|\n"
            + "+-----------+------------------------------------+-------+-------+-------+-------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select contributor, version from d0.c0.information where version = 3.0;";
    expect =
        "ResultSets:\n"
            + "+-----------+-----------------------------+-------------------------+\n"
            + "|        key|d0.c0.information.contributor|d0.c0.information.version|\n"
            + "+-----------+-----------------------------+-------------------------+\n"
            + "|12884901888|                 Label Studio|                      3.0|\n"
            + "+-----------+-----------------------------+-------------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select contributor, version from d0.c0.information where version = 1.0;";
    expect =
        "ResultSets:\n"
            + "+----------+-----------------------------+-------------------------+\n"
            + "|       key|d0.c0.information.contributor|d0.c0.information.version|\n"
            + "+----------+-----------------------------+-------------------------+\n"
            + "|4294967296|                 Label Studio|                      1.0|\n"
            + "|8589934592|                 Label Studio|                      1.0|\n"
            + "+----------+-----------------------------+-------------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement =
        "select contributor, version from d0.c0.information where version = 3.0 or version = 1.0;";
    expect =
        "ResultSets:\n"
            + "+-----------+-----------------------------+-------------------------+\n"
            + "|        key|d0.c0.information.contributor|d0.c0.information.version|\n"
            + "+-----------+-----------------------------+-------------------------+\n"
            + "| 4294967296|                 Label Studio|                      1.0|\n"
            + "| 8589934592|                 Label Studio|                      1.0|\n"
            + "|12884901888|                 Label Studio|                      3.0|\n"
            + "+-----------+-----------------------------+-------------------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  // mongoDB中，数据含有id
  @Override
  public void testShowColumns() {
    String statement = "SHOW COLUMNS mn.*;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|             mn.wf01._id| INTEGER|\n"
            + "|     mn.wf01.wt01.status|    LONG|\n"
            + "|mn.wf01.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS nt.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|             nt.wf03._id| INTEGER|\n"
            + "|    nt.wf03.wt01.status2|    LONG|\n"
            + "|             nt.wf04._id| INTEGER|\n"
            + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS tm.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|             tm.wf05._id| INTEGER|\n"
            + "|     tm.wf05.wt01.status|    LONG|\n"
            + "|tm.wf05.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }

  // no param is allowed to be updated
  @Override
  protected void updateParams(int port) {}

  @Override
  protected void restoreParams(int port) {}

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "mongodb", 30);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "mongodb", 30);
  }

  @Override
  protected void testPathOverlappedDataNotOverlapped() throws SessionException {
    // before
    String statement = "select status from mn.wf01.wt01;";
    String expected =
        "ResultSets:\n"
            + "+----------+-------------------+\n"
            + "|       key|mn.wf01.wt01.status|\n"
            + "+----------+-------------------+\n"
            + "|4294967296|           11111111|\n"
            + "|8589934592|           22222222|\n"
            + "+----------+-------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    String insert =
        "insert into mn.wf01.wt01 (key, status) values (10, 33333333), (100, 44444444);";
    session.executeSql(insert);

    // after
    statement = "select status from mn.wf01.wt01;";
    expected =
        "ResultSets:\n"
            + "+----------+-------------------+\n"
            + "|       key|mn.wf01.wt01.status|\n"
            + "+----------+-------------------+\n"
            + "|        10|           33333333|\n"
            + "|       100|           44444444|\n"
            + "|4294967296|           11111111|\n"
            + "|8589934592|           22222222|\n"
            + "+----------+-------------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  protected void testProjectNonexistent() {
    // field name
    String statement = "select key, area, category_id from d0.c0.annotations0;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select key, area, category_id from d0.c00.annotations;";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select key, area, category_id from d00.c0.annotations0;";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select key, area0, category_id0 from d00.c00.annotations00;";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }
}

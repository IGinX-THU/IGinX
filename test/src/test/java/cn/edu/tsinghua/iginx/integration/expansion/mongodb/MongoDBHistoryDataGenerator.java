package cn.edu.tsinghua.iginx.integration.expansion.mongodb;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.readOnlyPort;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBHistoryDataGenerator.class);

  private static final String LOCAL_IP = "127.0.0.1";

  public MongoDBHistoryDataGenerator() {
    Constant.oriPort = 27017;
    Constant.expPort = 27018;
    readOnlyPort = 27019;
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    try (MongoClient client = connect(port)) {
      for (int i = 0; i < pathList.size(); i++) {
        String[] nodes = pathList.get(i).split("\\.");
        StringJoiner joiner = new StringJoiner(".");
        for (int j = 2; j < nodes.length; j++) {
          joiner.add(nodes[j]);
        }
        String field = joiner.toString();

        List<WriteModel<? extends Document>> operations = new ArrayList<>();
        for (int k = 0; k < valuesList.size(); k++) {
          List<Object> row = valuesList.get(k);
          Object value = row.get(i);
          if (value instanceof byte[]) {
            value = new String((byte[]) value);
          }
          Bson filter = Filters.eq("_id", k);
          Bson update = Updates.set(field, value);
          operations.add(new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true)));
        }

        MongoCollection<Document> collection = client.getDatabase(nodes[0]).getCollection(nodes[1]);
        collection.bulkWrite(operations);
      }
    }
    LOGGER.info("write data to 127.0.0.1:{} success!", port);
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    writeHistoryData(port, pathList, dataTypeList, new ArrayList<>(), valuesList);
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    try (MongoClient client = connect(port)) {
      for (String db : client.listDatabaseNames()) {
        if (!db.equals("admin") && !db.equals("config") && !db.equals("local")) {
          client.getDatabase(db).drop();
        }
      }
    }
    LOGGER.info("clear data on 127.0.0.1:{} success!", port);
  }

  @Override
  public void writeSpecialHistoryData() {
    try (MongoClient client = connect(readOnlyPort)) {
      MongoDatabase database = client.getDatabase("d0");
      MongoCollection<Document> collection = database.getCollection("c0");
      collection.insertOne(Document.parse(JSON_EXAMPLE_0));
      collection.insertOne(Document.parse(JSON_EXAMPLE_1));
      collection.insertOne(Document.parse(JSON_EXAMPLE_2));
    }
    try (MongoClient client = connect(readOnlyPort)) {
      MongoDatabase database = client.getDatabase("d1");
      MongoCollection<Document> collection = database.getCollection("c1");
      for (String json : JSON_FILTER_EXAMPLE) {
        collection.insertOne(Document.parse(json));
      }
    }
  }

  private MongoClient connect(int port) {
    ServerAddress address = new ServerAddress(MongoDBHistoryDataGenerator.LOCAL_IP, port);
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(address)))
            .build();

    return MongoClients.create(settings);
  }

  private static final String JSON_EXAMPLE_0 =
      "{\n"
          + "  \"_id\": {\"$oid\": \"652f4577a162014f74419b7f\"},"
          + "  \"images\": [\n"
          + "    {\n"
          + "      \"width\": 1037,\n"
          + "      \"height\": 501,\n"
          + "      \"id\": 0,\n"
          + "      \"file_name\": \"images/3/ad37161b-P92902000484212110001_-2_crop.jpg\"\n"
          + "    }\n"
          + "  ],\n"
          + "  \"categories\": [\n"
          + "    {\n"
          + "      \"id\": 0,\n"
          + "      \"name\": \"Blur\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"id\": 1,\n"
          + "      \"name\": \"Phone\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"id\": 2,\n"
          + "      \"name\": \"ReflectLight\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"id\": 3,\n"
          + "      \"name\": \"Reflection\"\n"
          + "    }\n"
          + "  ],\n"
          + "  \"annotations\": [\n"
          + "    {\n"
          + "      \"id\": 0,\n"
          + "      \"image_id\": 0,\n"
          + "      \"category_id\": 0,\n"
          + "      \"segmentation\": [\n"
          + "        [\n"
          + "          4.106930693069307,\n"
          + "          65.70491803278689,\n"
          + "          43.122772277227725,\n"
          + "          25.66598360655738,\n"
          + "          988.7435643564356,\n"
          + "          12.319672131147541,\n"
          + "          1029.8128712871287,\n"
          + "          49.278688524590166,\n"
          + "          1028.786138613861,\n"
          + "          428.1086065573771,\n"
          + "          1004.1445544554455,\n"
          + "          459.9344262295082,\n"
          + "          964.1019801980199,\n"
          + "          467.1209016393442,\n"
          + "          529.7940594059406,\n"
          + "          482.5204918032787,\n"
          + "          247.44257425742566,\n"
          + "          487.6536885245902,\n"
          + "          84.1920792079208,\n"
          + "          490.7336065573771,\n"
          + "          26.695049504950497,\n"
          + "          477.38729508196724,\n"
          + "          11.294059405940594,\n"
          + "          444.53483606557376\n"
          + "        ]\n"
          + "      ],\n"
          + "      \"bbox\": [\n"
          + "        4.106930693069307,\n"
          + "        12.319672131147541,\n"
          + "        1025.7059405940595,\n"
          + "        478.4139344262295\n"
          + "      ],\n"
          + "      \"ignore\": 0,\n"
          + "      \"iscrowd\": 0,\n"
          + "      \"area\": 468549.3681311881\n"
          + "    },\n"
          + "    {\n"
          + "      \"id\": 1,\n"
          + "      \"image_id\": 0,\n"
          + "      \"category_id\": 3,\n"
          + "      \"segmentation\": [\n"
          + "        [\n"
          + "          57.4970297029703,\n"
          + "          37.98565573770492,\n"
          + "          135.52871287128715,\n"
          + "          55.43852459016394,\n"
          + "          203.2930693069307,\n"
          + "          58.51844262295083,\n"
          + "          242.30891089108914,\n"
          + "          60.57172131147539,\n"
          + "          291.5920792079209,\n"
          + "          63.65163934426229,\n"
          + "          323.4207920792079,\n"
          + "          70.83811475409836,\n"
          + "          355.2495049504951,\n"
          + "          86.23770491803279,\n"
          + "          370.65049504950497,\n"
          + "          103.69057377049181,\n"
          + "          379.891089108911,\n"
          + "          112.93032786885244,\n"
          + "          405.55940594059405,\n"
          + "          120.11680327868852,\n"
          + "          425.0673267326732,\n"
          + "          120.11680327868852,\n"
          + "          464.08316831683175,\n"
          + "          122.17008196721311,\n"
          + "          489.7514851485148,\n"
          + "          124.22336065573771,\n"
          + "          512.339603960396,\n"
          + "          127.30327868852459,\n"
          + "          520.5534653465346,\n"
          + "          137.56967213114754,\n"
          + "          524.660396039604,\n"
          + "          148.86270491803282,\n"
          + "          527.7405940594059,\n"
          + "          165.28893442622945,\n"
          + "          522.6069306930693,\n"
          + "          190.95491803278688,\n"
          + "          525.6871287128713,\n"
          + "          208.4077868852459,\n"
          + "          529.7940594059406,\n"
          + "          431.1885245901639,\n"
          + "          521.580198019802,\n"
          + "          451.7213114754098,\n"
          + "          504.12574257425734,\n"
          + "          459.9344262295082,\n"
          + "          77.00495049504946,\n"
          + "          474.30737704918033,\n"
          + "          48.25643564356436,\n"
          + "          447.61475409836066,\n"
          + "          40.04257425742574,\n"
          + "          414.76229508196724,\n"
          + "          39.015841584158416,\n"
          + "          54.411885245901644\n"
          + "        ]\n"
          + "      ],\n"
          + "      \"bbox\": [\n"
          + "        39.015841584158416,\n"
          + "        37.98565573770492,\n"
          + "        490.7782178217822,\n"
          + "        436.3217213114754\n"
          + "      ],\n"
          + "      \"ignore\": 0,\n"
          + "      \"iscrowd\": 0,\n"
          + "      \"area\": 188048.61386138643\n"
          + "    },\n"
          + "    {\n"
          + "      \"id\": 2,\n"
          + "      \"image_id\": 0,\n"
          + "      \"category_id\": 3,\n"
          + "      \"segmentation\": [\n"
          + "        [\n"
          + "          693.0445544554455,\n"
          + "          35.932377049180324,\n"
          + "          772.1029702970297,\n"
          + "          26.692622950819672,\n"
          + "          802.9049504950495,\n"
          + "          441.45491803278685,\n"
          + "          794.6910891089109,\n"
          + "          456.8545081967213,\n"
          + "          752.5950495049505,\n"
          + "          459.9344262295082,\n"
          + "          728.980198019802,\n"
          + "          461.98770491803276\n"
          + "        ]\n"
          + "      ],\n"
          + "      \"bbox\": [\n"
          + "        693.0445544554455,\n"
          + "        26.692622950819672,\n"
          + "        109.8603960396041,\n"
          + "        435.2950819672131\n"
          + "      ],\n"
          + "      \"ignore\": 0,\n"
          + "      \"iscrowd\": 0,\n"
          + "      \"area\": 33132.500309405965\n"
          + "    }\n"
          + "  ],\n"
          + "  \"information\": {\n"
          + "    \"year\": 2021,\n"
          + "    \"version\": \"1.0\",\n"
          + "    \"score\": 1,\n"
          + "    \"description\": \"\",\n"
          + "    \"contributor\": \"Label Studio\",\n"
          + "    \"url\": \"\",\n"
          + "    \"date_created\": \"2022-12-12 08:37:26.832616\"\n"
          + "  },\n"
          + "    \"objects\": [\n"
          + "        {\n"
          + "            \"id\": 497521359,\n"
          + "            \"classId\": 1661571,\n"
          + "            \"description\": \"\",\n"
          + "            \"geometryType\": \"bitmap\",\n"
          + "            \"labelerLogin\": \"alexxx\",\n"
          + "            \"createdAt\": \"2020-08-07T11:09:51.054Z\",\n"
          + "            \"updatedAt\": \"2020-08-07T11:09:51.054Z\",\n"
          + "            \"tags\": [],\n"
          + "            \"classTitle\": \"person\",\n"
          + "            \"bitmap\": {\n"
          + "                \"data\": \"eJwBgQd++IlQTkcNChoKAAAADUlIRF\",\n"
          + "                \"origin\": [\n"
          + "                    535,\n"
          + "                    66\n"
          + "                ]\n"
          + "            }\n"
          + "        },\n"
          + "        {\n"
          + "            \"id\": 497521358,\n"
          + "            \"classId\": 1661574,\n"
          + "            \"description\": \"\",\n"
          + "            \"geometryType\": \"rectangle\",\n"
          + "            \"labelerLogin\": \"alexxx\",\n"
          + "            \"createdAt\": \"2020-08-07T11:09:51.054Z\",\n"
          + "            \"updatedAt\": \"2020-08-07T11:09:51.054Z\",\n"
          + "            \"tags\": [],\n"
          + "            \"classTitle\": \"bike\",\n"
          + "            \"points\": {\n"
          + "                \"exterior\": [\n"
          + "                    [\n"
          + "                        0,\n"
          + "                        236\n"
          + "                    ],\n"
          + "                    [\n"
          + "                        582,\n"
          + "                        872\n"
          + "                    ]\n"
          + "                ],\n"
          + "                \"interior\": []\n"
          + "            }\n"
          + "        }\n"
          + "    ]"
          + "}";
  private static final String JSON_EXAMPLE_1 =
      "{\n"
          + "  \"_id\": {\"$oid\": \"652f4577a162014f74419b80\"},"
          + "  \"images\": [\n"
          + "    {\n"
          + "      \"width\": 1037,\n"
          + "      \"height\": 501,\n"
          + "      \"id\": 0,\n"
          + "      \"file_name\": \"images/3/ad37161b-P92902000484212110001_-2_crop.jpg\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"width\": 1038,\n"
          + "      \"height\": 502,\n"
          + "      \"id\": 1,\n"
          + "      \"file_name\": \"images/3/ad37161b-P92902000484212110001_-3_crop.jpg\"\n"
          + "    }\n"
          + "  ],\n"
          + "  \"information\": {\n"
          + "    \"year\": 2022,\n"
          + "    \"version\": \"1.0\",\n"
          + "    \"score\": 2,\n"
          + "    \"description\": \"\",\n"
          + "    \"contributor\": \"Label Studio\",\n"
          + "    \"url\": \"\",\n"
          + "    \"date_created\": \"2022-12-12 08:37:26.832616\"\n"
          + "  }\n"
          + "}";

  private static final String JSON_EXAMPLE_2 =
      "{\n"
          + "  \"_id\": {\"$oid\": \"652f4577a162014f74419b81\"},"
          + "  \"information\": {\n"
          + "    \"year\": \"2023\",\n"
          + "    \"version\": 3.0,\n"
          + "    \"score\": 3.1,\n"
          + "    \"description\": \"\",\n"
          + "    \"contributor\": \"Label Studio\",\n"
          + "    \"url\": null,\n"
          + "    \"date_created\": \"2022-12-12 08:37:26.832616\"\n"
          + "  }\n"
          + "}";

  private static final String[] JSON_FILTER_EXAMPLE =
      new String[] {
        "{\n"
            + "\t\"_id\": {\n"
            + "\t\t\"$oid\": \"000000000000000000000000\"\n"
            + "\t},\n"
            + "\t\"i\": 0,\n"
            + "\t\"b\": true,\n"
            + "\t\"f\": 0.1,\n"
            + "\t\"s\": \"1st\"\n"
            + "}",
        "{\n"
            + "\t\"_id\": {\n"
            + "\t\t\"$oid\": \"000000000000000000000001\"\n"
            + "\t},\n"
            + "\t\"i\": 1,\n"
            + "\t\"b\": false,\n"
            + "\t\"f\": 1.1,\n"
            + "\t\"s\": \"2nd\"\n"
            + "}",
        "{\n"
            + "\t\"_id\": {\n"
            + "\t\t\"$oid\": \"000000000000000000000002\"\n"
            + "\t},\n"
            + "\t\"i\": 2,\n"
            + "\t\"b\": true,\n"
            + "\t\"f\": 2.1,\n"
            + "\t\"s\": \"3th\"\n"
            + "}",
        "{\n"
            + "\t\"_id\": {\n"
            + "\t\t\"$oid\": \"000000000000000000000003\"\n"
            + "\t},\n"
            + "\t\"i\": 3,\n"
            + "\t\"b\": false,\n"
            + "\t\"f\": 3.1,\n"
            + "\t\"s\": \"4th\"\n"
            + "}",
        "{\n"
            + "\t\"_id\": {\n"
            + "\t\t\"$oid\": \"000000000000000000000004\"\n"
            + "\t},\n"
            + "\t\"i\": 4,\n"
            + "\t\"b\": true,\n"
            + "\t\"f\": 4.1,\n"
            + "\t\"s\": \"5th\"\n"
            + "}",
      };
}

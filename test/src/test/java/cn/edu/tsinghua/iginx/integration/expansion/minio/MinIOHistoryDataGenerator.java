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
package cn.edu.tsinghua.iginx.integration.expansion.minio;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.DataViewGenerator;
import cn.edu.tsinghua.iginx.minio.kv.ObjectKvManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinIOHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MinIOHistoryDataGenerator.class);

  static final String LOCAL_IP = "127.0.0.1";

  static final String USERNAME = "minioadmin";

  static final String PASSWORD = "minioadmin";

  /** 历史数据以 ObjectKV 结构写入该桶，扩容测试中以 dummy_bucket 指定 */
  static final String DUMMY_BUCKET = "iginx-dummy";

  /** IGinX 写入数据所在的桶，与 conf 中 data_bucket 的默认值一致 */
  static final String DATA_BUCKET = "iginx-data";

  /** 以文件树方式读取的特殊历史数据所在的桶 */
  static final String FILE_TREE_BUCKET = "test";

  /**
   * 以 FileTree 方式挂载 test 桶的临时只读引擎所在的端口。IGinX 以 ip、端口、类型和前缀区分引擎，扩容测试的三个端口上已注册了无前缀的引擎， 所以另起两个
   * MinIO，避免临时引擎被当作重复注册。
   */
  static final int FILE_TREE_PORT = 9003;

  static final int FILE_TREE_CSV_PORT = 9004;

  public MinIOHistoryDataGenerator() {
    Constant.oriPort = 9000;
    Constant.expPort = 9001;
    Constant.readOnlyPort = 9002;
  }

  static MinioClient newClient(int port) {
    return MinioClient.builder()
        .endpoint(LOCAL_IP, port, false)
        .credentials(USERNAME, PASSWORD)
        .build();
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    List<Long> keyList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      keyList.add((long) i);
    }
    writeHistoryData(port, pathList, dataTypeList, keyList, valuesList);
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    DataView dataView =
        DataViewGenerator.genRowDataView(pathList, dataTypeList, keyList, valuesList);
    MinioClient client = newClient(port);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      ensureBucket(client, DUMMY_BUCKET);
      try (ObjectKvManager manager =
          new ObjectKvManager(client, DUMMY_BUCKET, "", executor, Integer.MAX_VALUE)) {
        manager.insert(dataView);
      }
      LOGGER.info("write history data {} to {}:{} success", pathList, LOCAL_IP, port);
    } catch (Exception e) {
      LOGGER.error("write history data to {}:{} failure", LOCAL_IP, port, e);
      throw new RuntimeException(e);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @Override
  public void clearHistoryData() {
    super.clearHistoryData();
    clearHistoryDataForGivenPort(FILE_TREE_PORT);
    clearHistoryDataForGivenPort(FILE_TREE_CSV_PORT);
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    MinioClient client = newClient(port);
    for (String bucket : new String[] {DUMMY_BUCKET, DATA_BUCKET, FILE_TREE_BUCKET}) {
      try {
        clearBucket(client, bucket);
      } catch (Exception e) {
        LOGGER.error("clear bucket {} on {}:{} failure", bucket, LOCAL_IP, port, e);
      }
    }
    LOGGER.info("clear data on {}:{} success", LOCAL_IP, port);
  }

  @Override
  public void writeSpecialHistoryData() {
    writeFileTree(FILE_TREE_PORT);
    writeFileTree(FILE_TREE_CSV_PORT);
  }

  /** 以 FileTree 结构读取的文件，放在 test 桶中 */
  private void writeFileTree(int port) {
    // test (bucket)
    // ├── csv
    // │   └── lineitem.csv
    // ├── a
    // │   ├── b/c/d/1.txt
    // │   ├── e/2.txt
    // │   ├── f/g/3.txt
    // │   ├── Iris.parquet
    // │   ├── floatTest.parquet
    // │   ├── lineitem.tsv
    // │   └── other
    // │       ├── MT cars.parquet
    // │       └── price.parquet
    // └── txt
    //     └── dir!@#$%^&()[]{};',.=+~ -目录
    //         ├── example!@#$%^&()[]{};',.=+~ -.txt
    //         └── 示例!@#$%^&()[]{};',.=+~ -.TXT
    StringBuilder content1 = new StringBuilder();
    StringBuilder content2 = new StringBuilder();
    StringBuilder content3 = new StringBuilder();
    for (int i = 0; i < 26; i++) {
      content1.append('a' + i);
    }
    for (int i = 0; i < 26; i++) {
      content2.append('A' + i);
    }
    for (int i = 0; i < 26; i++) {
      content3.append(i);
    }

    MinioClient client = newClient(port);
    try {
      ensureBucket(client, FILE_TREE_BUCKET);
      putObject(client, "a/b/c/d/1.txt", content1.toString().getBytes());
      putObject(client, "a/e/2.txt", content2.toString().getBytes());
      putObject(client, "a/f/g/3.txt", content3.toString().getBytes());

      String parquetResourceDir = "dummy/parquet/";
      putResource(client, parquetResourceDir + "Iris.parquet", "a/Iris.parquet");
      putResource(client, parquetResourceDir + "floatTest.parquet", "a/floatTest.parquet");
      putResource(client, parquetResourceDir + "MT cars.parquet", "a/other/MT cars.parquet");
      putResource(client, parquetResourceDir + "price.parquet", "a/other/price.parquet");

      String csvResourceDir = "dummy/csv/";
      putResource(client, csvResourceDir + "lineitem.tsv", "a/lineitem.tsv");
      putResource(client, csvResourceDir + "lineitem.csv", "csv/lineitem.csv");

      String specialName = "!@#$%^&()[]{};',.=+~ -";
      String folderName = "txt/dir" + specialName + "目录/";
      putResource(client, "dummy/txt/example.txt", folderName + "example" + specialName + ".txt");
      putResource(client, "dummy/txt/example.txt", folderName + "示例" + specialName + ".TXT");
      LOGGER.info("write special history data to {}:{} success", LOCAL_IP, port);
    } catch (Exception e) {
      LOGGER.error("write special history data to {}:{} failure", LOCAL_IP, port, e);
      throw new RuntimeException(e);
    }
  }

  private static void putResource(MinioClient client, String resourcePath, String objectName)
      throws Exception {
    try (InputStream is =
        MinIOHistoryDataGenerator.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalStateException("resource " + resourcePath + " not found");
      }
      putObject(client, objectName, IOUtils.toByteArray(is));
    }
  }

  private static void putObject(MinioClient client, String objectName, byte[] content)
      throws Exception {
    client.putObject(
        PutObjectArgs.builder().bucket(FILE_TREE_BUCKET).object(objectName).stream(
                new ByteArrayInputStream(content), content.length, -1)
            .build());
  }

  private static void ensureBucket(MinioClient client, String bucket) throws Exception {
    if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
    }
  }

  private static void clearBucket(MinioClient client, String bucket) throws Exception {
    if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
      return;
    }
    List<DeleteObject> objects = new ArrayList<>();
    for (Result<Item> result :
        client.listObjects(ListObjectsArgs.builder().bucket(bucket).recursive(true).build())) {
      objects.add(new DeleteObject(result.get().objectName()));
    }
    for (int i = 0; i < objects.size(); i += 1000) {
      List<DeleteObject> batch = objects.subList(i, Math.min(objects.size(), i + 1000));
      for (Result<DeleteError> error :
          client.removeObjects(RemoveObjectsArgs.builder().bucket(bucket).objects(batch).build())) {
        DeleteError e = error.get();
        LOGGER.error("delete {} in bucket {} failure: {}", e.objectName(), bucket, e.message());
      }
    }
  }
}

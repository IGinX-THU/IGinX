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
package cn.edu.tsinghua.iginx.s3.strategy;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.descriptor.DriverDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.storage.descriptor.HostPolicy;
import cn.edu.tsinghua.iginx.engine.physical.storage.descriptor.StorageDriverDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils;
import cn.edu.tsinghua.iginx.s3.S3Config;
import cn.edu.tsinghua.iginx.s3.S3Storage;
import cn.edu.tsinghua.iginx.s3.common.S3Client;
import cn.edu.tsinghua.iginx.s3.common.S3Object;
import cn.edu.tsinghua.iginx.s3.kv.ColumnNames;
import cn.edu.tsinghua.iginx.s3.kv.ObjectKvManager;
import cn.edu.tsinghua.iginx.s3.kv.Segment;
import cn.edu.tsinghua.iginx.s3.kv.SegmentName;
import cn.edu.tsinghua.iginx.s3.nio.S3FileSystem;
import cn.edu.tsinghua.iginx.s3.struct.DataTarget;
import cn.edu.tsinghua.iginx.s3.tree.ObjectTreeConfig;
import cn.edu.tsinghua.iginx.s3.tree.ObjectTreeManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import okhttp3.HttpUrl;
import okhttp3.Request;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks the requests IGinX sends to the endpoint, on an in-memory S3 service without network.
 * Aliyun OSS only accepts virtual-hosted-style requests, so every request must put the bucket in
 * the host, and none of CreateBucket and GetBucketLocation (always path-style in the SDK) may be
 * sent.
 */
public class S3RequestUrlTest {

  private static final String OSS_HOST = "oss-cn-hangzhou.aliyuncs.com";
  /** an address of {@link #OSS_HOST} */
  private static final String OSS_IP = "118.31.219.230";

  private static final String OSS_REGION = "oss-cn-hangzhou";
  private static final String BUCKET = "bkt";
  private static final byte[] CSV = "k,v\n1,2\n".getBytes(StandardCharsets.UTF_8);

  private ExecutorService executor;

  @Before
  public void setUp() {
    executor = Executors.newFixedThreadPool(2);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  private static Map<String, String> aliyunParams() {
    Map<String, String> params = new HashMap<>();
    params.put(S3Config.ENGINE, "aliyun");
    params.put(S3Config.REGION, OSS_REGION);
    params.put(S3Config.USERNAME, "ak");
    params.put(S3Config.PASSWORD, "sk");
    params.put(S3Config.DATA_BUCKET, BUCKET);
    return params;
  }

  /** the descriptor declared on the driver class, loaded as StorageManager#getDescriptor does */
  private static StorageDriverDescriptor descriptor() throws Exception {
    return S3Storage.class
        .getAnnotation(DriverDescriptor.class)
        .value()
        .getConstructor()
        .newInstance();
  }

  /**
   * Registers the engine as IGinX does (DefaultMetaManager#resolveStorageEngineFromConf and
   * IginxWorker#addStorageEngines): the address saved in meta and passed to the driver follows the
   * host policy declared by the driver.
   */
  private static S3Strategy register(String host, int port, Map<String, String> params)
      throws Exception {
    String ip = StorageEngineUtils.addressOf(host, descriptor().hostPolicy(params));
    assertNotNull("IGinX rejects host " + host, ip);
    StorageEngineMeta meta = new StorageEngineMeta(-1, ip, port, params, StorageEngineType.s3, -1);
    S3Strategy strategy = S3StrategyFactory.create(S3Config.of(meta));
    strategy.validate();
    return strategy;
  }

  private static S3Client clientOf(S3Strategy strategy, FakeS3 fake) {
    return strategy.createClient(fake.httpClient());
  }

  private static FakeS3 newOss(String host) {
    FakeS3 fake = new FakeS3(host, true);
    fake.createBucket(BUCKET);
    fake.put(BUCKET, "a/b.csv", CSV);
    return fake;
  }

  /** the requests issued by each kind of operation of the driver */
  private static void runAllOperations(S3Strategy strategy, S3Client client) throws Exception {
    strategy.ensureBucket(client, BUCKET);
    List<String> listed = new ArrayList<>();
    for (S3Object object : client.listObjects(BUCKET, "a/", false)) {
      listed.add(object.key());
    }
    assertEquals(Collections.singletonList("a/b.csv"), listed);
    assertEquals(CSV.length, client.statObjectOrNull(BUCKET, "a/b.csv").size());
    try (InputStream in = client.getObject(BUCKET, "a/b.csv", 0, 4)) {
      assertEquals("k,v\n", new String(readAll(in), StandardCharsets.UTF_8));
    }
    client.putObject(BUCKET, "iginx_unit0/x.seg", new byte[] {1, 2, 3}, "application/octet-stream");
    client.removeObjects(BUCKET, Collections.singletonList("iginx_unit0/x.seg"));
  }

  private static List<String> describe(List<Request> requests) {
    return requests.stream().map(r -> r.method() + " " + r.url()).collect(Collectors.toList());
  }

  @Test
  public void testAliyunRequestsAreVirtualHosted() throws Exception {
    FakeS3 fake = newOss(OSS_HOST);
    S3Strategy strategy = register(OSS_HOST, 443, aliyunParams());
    runAllOperations(strategy, clientOf(strategy, fake));

    List<Request> requests = fake.requests();
    assertEquals(
        Arrays.asList(
            "HEAD https://bkt.oss-cn-hangzhou.aliyuncs.com/",
            "GET https://bkt.oss-cn-hangzhou.aliyuncs.com/?list-type=2&max-keys=1000&encoding-type=url&delimiter=%2F&prefix=a%2F",
            "HEAD https://bkt.oss-cn-hangzhou.aliyuncs.com/a/b.csv",
            "GET https://bkt.oss-cn-hangzhou.aliyuncs.com/a/b.csv",
            "PUT https://bkt.oss-cn-hangzhou.aliyuncs.com/iginx_unit0/x.seg",
            "POST https://bkt.oss-cn-hangzhou.aliyuncs.com/?delete="),
        describe(requests));
    assertEquals("bytes=0-3", requests.get(3).header("Range"));
    for (Request request : requests) {
      assertSignedFor(request, OSS_REGION);
    }
  }

  @Test
  public void testMinioRequestsArePathStyle() throws Exception {
    FakeS3 fake = new FakeS3("127.0.0.1", false);
    fake.createBucket(BUCKET);
    fake.put(BUCKET, "a/b.csv", CSV);
    Map<String, String> params = new HashMap<>();
    params.put(S3Config.ENGINE, "minio");
    S3Strategy strategy = register("localhost", 9000, params);
    runAllOperations(strategy, clientOf(strategy, fake));

    // without region, the SDK asks for the location of the bucket first
    assertEquals(
        Arrays.asList(
            "GET http://127.0.0.1:9000/bkt?location=",
            "HEAD http://127.0.0.1:9000/bkt",
            "GET http://127.0.0.1:9000/bkt?list-type=2&max-keys=1000&encoding-type=url&delimiter=%2F&prefix=a%2F",
            "HEAD http://127.0.0.1:9000/bkt/a/b.csv",
            "GET http://127.0.0.1:9000/bkt/a/b.csv",
            "PUT http://127.0.0.1:9000/bkt/iginx_unit0/x.seg",
            "POST http://127.0.0.1:9000/bkt?delete="),
        describe(fake.requests()));
  }

  /** The address policy is a bundled service capability, not a registration option. */
  @Test
  public void testHostPolicy() throws Exception {
    StorageDriverDescriptor descriptor = descriptor();
    assertEquals(HostPolicy.PRESERVE_HOST, descriptor.hostPolicy(aliyunParams()));
    assertEquals(
        HostPolicy.PRESERVE_HOST,
        descriptor.hostPolicy(Collections.singletonMap(S3Config.ENGINE, " AliYun ")));
    Map<String, String> minio = new HashMap<>();
    minio.put(S3Config.ENGINE, "minio");
    assertEquals(HostPolicy.RESOLVE_TO_IP, descriptor.hostPolicy(minio));
    minio.put(S3Config.VIRTUAL_STYLE, "true");
    minio.put("preserve_host", "true");
    minio.put("meta_properties_path", "unused.properties");
    assertEquals(HostPolicy.RESOLVE_TO_IP, descriptor.hostPolicy(minio));
    Map<String, String> aliyun = aliyunParams();
    aliyun.put(S3Config.VIRTUAL_STYLE, "false");
    aliyun.put("preserve_host", "false");
    aliyun.put("host_policy", "RESOLVE_TO_IP");
    assertEquals(HostPolicy.PRESERVE_HOST, descriptor.hostPolicy(aliyun));
    // invalid params fall back to the default, and are reported when the storage is created
    assertEquals(HostPolicy.RESOLVE_TO_IP, descriptor.hostPolicy(Collections.emptyMap()));
    assertEquals(
        HostPolicy.RESOLVE_TO_IP,
        descriptor.hostPolicy(Collections.singletonMap(S3Config.ENGINE, "unknown")));
  }

  /** the domain is kept as is (in lower case) without resolving, so no DNS is needed */
  @Test
  public void testAliyunRegisteredByDomain() throws Exception {
    FakeS3 fake = newOss(OSS_HOST);
    S3Strategy strategy = register(" OSS-CN-HANGZHOU.aliyuncs.com ", 443, aliyunParams());
    assertTrue(clientOf(strategy, fake).bucketExists(BUCKET));
    assertEquals(
        Collections.singletonList("HEAD https://bkt.oss-cn-hangzhou.aliyuncs.com/"),
        describe(fake.requests()));
  }

  /** the bucket would be put before the IP, so aliyun must be registered by domain */
  @Test
  public void testAliyunRegisteredByIpRejected() throws Exception {
    try {
      register(OSS_IP, 443, aliyunParams());
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("domain of the service"));
    }
    assertNull(StorageEngineUtils.addressOf("", descriptor().hostPolicy(aliyunParams())));
  }

  @Test
  public void testAliyunInternalEndpoint() throws Exception {
    String host = "oss-cn-hangzhou-internal.aliyuncs.com";
    FakeS3 fake = newOss(host);
    S3Strategy strategy = register(host, 443, aliyunParams());
    assertTrue(clientOf(strategy, fake).bucketExists(BUCKET));
    assertEquals(
        Collections.singletonList("HEAD https://bkt.oss-cn-hangzhou-internal.aliyuncs.com/"),
        describe(fake.requests()));
  }

  @Test
  public void testAliyunServiceOverridesRejected() throws Exception {
    Map<String, String> params = aliyunParams();
    params.put(S3Config.SECURE, "false");
    try {
      register(OSS_HOST, 80, params);
      fail("user options must not change the service protocol");
    } catch (StorageInitializationException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("secure is not supported"));
    }
  }

  @Test
  public void testAliyunDoesNotCreateBucket() throws Exception {
    FakeS3 fake = new FakeS3(OSS_HOST, true);
    S3Strategy strategy = register(OSS_HOST, 443, aliyunParams());
    try {
      strategy.ensureBucket(clientOf(strategy, fake), BUCKET);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("create it in the OSS console"));
    }
    assertEquals(
        Collections.singletonList("HEAD https://bkt.oss-cn-hangzhou.aliyuncs.com/"),
        describe(fake.requests()));
  }

  /** Reject an incompatible override before a request can be sent to OSS. */
  @Test
  public void testPathStyleRejectedByOss() throws Exception {
    FakeS3 fake = newOss(OSS_HOST);
    Map<String, String> params = aliyunParams();
    params.put(S3Config.VIRTUAL_STYLE, "false");
    try {
      register(OSS_HOST, 443, params);
      fail();
    } catch (StorageInitializationException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("virtual_style is not supported"));
    }
    assertTrue(fake.requests().isEmpty());
  }

  /** ObjectKV and LegacyFilesystem work on OSS, which rejects any path-style request */
  @Test
  public void testDriversOnOss() throws Exception {
    FakeS3 fake = newOss(OSS_HOST);
    String fullName = ColumnNames.toFullName("a.b", Collections.emptyMap());
    SegmentName name = new SegmentName(SegmentName.newId(), DataType.LONG, 1, 2);
    fake.put(
        BUCKET,
        ColumnNames.encode(fullName) + "/" + name.toFileName(),
        new Segment(DataType.LONG, new long[] {1, 2}, new Object[] {10L, 20L}).encode());
    S3Strategy strategy = register(OSS_HOST, 443, aliyunParams());
    S3Client client = clientOf(strategy, fake);

    try (ObjectKvManager manager = new ObjectKvManager(client, BUCKET, "", executor, 16)) {
      Map<Long, Object> rows = new TreeMap<>();
      try (RowStream stream =
          manager.query(
              new DataTarget(new BoolFilter(true), Collections.singletonList("a.b"), null))) {
        assertEquals("a.b", stream.getHeader().getField(0).getName());
        while (stream.hasNext()) {
          Row row = stream.next();
          rows.put(row.getKey(), row.getValue(0));
        }
      }
      Map<Long, Object> expected = new TreeMap<>();
      expected.put(1L, 10L);
      expected.put(2L, 20L);
      assertEquals(expected, rows);
      manager.delete(new DataTarget(null, Collections.singletonList("a.b"), null));
    }
    assertEquals(Collections.singleton("a/b.csv"), fake.keys(BUCKET));

    S3FileSystem fs = new S3FileSystem(client, Collections.emptySet(), 1024, Duration.ofSeconds(1));
    ObjectTreeConfig treeConfig =
        ObjectTreeConfig.of(ConfigFactory.parseMap(Collections.singletonMap("prefix", "a")));
    ObjectTreeManager tree =
        ObjectTreeManager.legacyFilesystem(
            fs.root().resolve(BUCKET).resolve("a"), treeConfig, executor);
    try (RowStream stream =
        tree.query(new DataTarget(new BoolFilter(true), Collections.singletonList("a.*"), null))) {
      Field field = stream.getHeader().getField(0);
      assertEquals("a.b\\.csv", field.getName());
      assertEquals(DataType.BINARY, field.getType());
      assertTrue(stream.hasNext());
      assertArrayEquals(CSV, (byte[]) stream.next().getValue(0));
    }

    for (Request request : fake.requests()) {
      assertTrue(request.url().toString(), request.url().host().equals("bkt." + OSS_HOST));
      assertSignedFor(request, OSS_REGION);
    }
  }

  private static void assertSignedFor(Request request, String region) {
    HttpUrl url = request.url();
    assertNull(url.toString(), url.queryParameter("location"));
    String authorization = request.header("Authorization");
    assertNotNull(url.toString(), authorization);
    assertTrue(authorization, authorization.startsWith("AWS4-HMAC-SHA256 Credential=ak/"));
    assertTrue(authorization, authorization.contains("/" + region + "/s3/aws4_request"));
    assertEquals(url.host(), request.header("Host") == null ? url.host() : request.header("Host"));
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int n;
    while ((n = in.read(buffer)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }
}

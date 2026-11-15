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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import okhttp3.*;
import okio.Buffer;

/**
 * In-memory S3 service installed as an OkHttp interceptor, so requests never reach the network.
 * Every request is recorded. With {@code virtualHostedOnly}, requests that put the bucket in the
 * path are rejected with {@code SecondLevelDomainForbidden}, as Aliyun OSS does.
 */
final class FakeS3 implements Interceptor {

  private static final MediaType XML = MediaType.get("application/xml");
  private static final String LAST_MODIFIED = "Mon, 09 Nov 2026 00:00:00 GMT";
  private static final String LAST_MODIFIED_ISO = "2026-11-09T00:00:00.000Z";
  private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");
  private static final Pattern DELETE_KEY = Pattern.compile("<Key>(.*?)</Key>");

  private final String endpointHost;
  private final boolean virtualHostedOnly;
  private final Map<String, TreeMap<String, byte[]>> buckets = new TreeMap<>();
  private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());

  FakeS3(String endpointHost, boolean virtualHostedOnly) {
    this.endpointHost = endpointHost;
    this.virtualHostedOnly = virtualHostedOnly;
  }

  synchronized void createBucket(String bucket) {
    buckets.computeIfAbsent(bucket, b -> new TreeMap<>());
  }

  synchronized void put(String bucket, String key, byte[] content) {
    buckets.get(bucket).put(key, content);
  }

  synchronized Set<String> keys(String bucket) {
    return new TreeSet<>(buckets.get(bucket).keySet());
  }

  List<Request> requests() {
    synchronized (requests) {
      return new ArrayList<>(requests);
    }
  }

  void clearRequests() {
    requests.clear();
  }

  OkHttpClient httpClient() {
    return new OkHttpClient.Builder().addInterceptor(this).build();
  }

  @Override
  public Response intercept(Chain chain) throws IOException {
    Request request = chain.request();
    requests.add(request);
    HttpUrl url = request.url();
    String bucket;
    List<String> keySegments;
    if (url.host().equals(endpointHost)) {
      List<String> segments = url.pathSegments();
      bucket = segments.get(0).isEmpty() ? null : segments.get(0);
      keySegments = segments.subList(1, segments.size());
      if (bucket != null && virtualHostedOnly) {
        return error(request, 403, "SecondLevelDomainForbidden", bucket, null);
      }
    } else if (url.host().endsWith("." + endpointHost)) {
      bucket = url.host().substring(0, url.host().length() - endpointHost.length() - 1);
      keySegments = url.pathSegments();
    } else {
      throw new IOException("unexpected host " + url.host());
    }
    String key = String.join("/", keySegments);
    synchronized (this) {
      if (bucket == null) {
        return listBuckets(request);
      }
      if (url.queryParameter("location") != null) {
        return ok(request, "<LocationConstraint></LocationConstraint>");
      }
      TreeMap<String, byte[]> objects = buckets.get(bucket);
      if (key.isEmpty()) {
        return bucketRequest(request, bucket, objects);
      }
      if (objects == null) {
        return error(request, 404, "NoSuchBucket", bucket, key);
      }
      return objectRequest(request, bucket, key, objects);
    }
  }

  private Response bucketRequest(
      Request request, String bucket, @Nullable TreeMap<String, byte[]> objects)
      throws IOException {
    switch (request.method()) {
      case "PUT":
        createBucket(bucket);
        return ok(request, "");
      case "HEAD":
        return objects == null
            ? error(request, 404, "NoSuchBucket", bucket, null)
            : ok(request, "");
      default:
        break;
    }
    if (objects == null) {
      return error(request, 404, "NoSuchBucket", bucket, null);
    }
    if (request.method().equals("POST") && request.url().queryParameter("delete") != null) {
      Matcher matcher = DELETE_KEY.matcher(bodyOf(request));
      while (matcher.find()) {
        objects.remove(matcher.group(1));
      }
      return ok(request, "<DeleteResult></DeleteResult>");
    }
    if (request.method().equals("GET")) {
      return listObjects(request, bucket, objects);
    }
    throw new IOException("unexpected request " + request);
  }

  private Response objectRequest(
      Request request, String bucket, String key, TreeMap<String, byte[]> objects)
      throws IOException {
    byte[] content = objects.get(key);
    switch (request.method()) {
      case "PUT":
        objects.put(key, bodyOf(request).getBytes(StandardCharsets.ISO_8859_1));
        return response(request, 200, new byte[0]).header("ETag", "\"etag\"").build();
      case "HEAD":
        if (content == null) {
          return error(request, 404, "NoSuchKey", bucket, key);
        }
        return response(request, 200, new byte[0])
            .header("Content-Length", String.valueOf(content.length))
            .header("Last-Modified", LAST_MODIFIED)
            .header("ETag", "\"etag\"")
            .build();
      case "GET":
        if (content == null) {
          return error(request, 404, "NoSuchKey", bucket, key);
        }
        String range = request.header("Range");
        if (range == null) {
          return response(request, 200, content).build();
        }
        Matcher matcher = RANGE.matcher(range);
        if (!matcher.matches()) {
          throw new IOException("unexpected range " + range);
        }
        int start = Integer.parseInt(matcher.group(1));
        int end = Math.min(content.length - 1, Integer.parseInt(matcher.group(2)));
        return response(request, 206, Arrays.copyOfRange(content, start, end + 1)).build();
      default:
        throw new IOException("unexpected request " + request);
    }
  }

  private Response listBuckets(Request request) {
    StringBuilder xml = new StringBuilder("<ListAllMyBucketsResult><Buckets>");
    for (String bucket : buckets.keySet()) {
      xml.append("<Bucket><Name>")
          .append(bucket)
          .append("</Name><CreationDate>")
          .append(LAST_MODIFIED_ISO)
          .append("</CreationDate></Bucket>");
    }
    return ok(request, xml.append("</Buckets></ListAllMyBucketsResult>").toString());
  }

  private Response listObjects(Request request, String bucket, TreeMap<String, byte[]> objects) {
    HttpUrl url = request.url();
    String prefix = Optional.ofNullable(url.queryParameter("prefix")).orElse("");
    String delimiter = url.queryParameter("delimiter");
    StringBuilder contents = new StringBuilder();
    Set<String> commonPrefixes = new TreeSet<>();
    for (Map.Entry<String, byte[]> entry : objects.tailMap(prefix, true).entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(prefix)) {
        break;
      }
      String rest = key.substring(prefix.length());
      int split = delimiter == null || delimiter.isEmpty() ? -1 : rest.indexOf(delimiter);
      if (split >= 0) {
        commonPrefixes.add(prefix + rest.substring(0, split + delimiter.length()));
        continue;
      }
      contents
          .append("<Contents><Key>")
          .append(escape(key))
          .append("</Key><LastModified>")
          .append(LAST_MODIFIED_ISO)
          .append("</LastModified><ETag>\"etag\"</ETag><Size>")
          .append(entry.getValue().length)
          .append("</Size><StorageClass>STANDARD</StorageClass></Contents>");
    }
    StringBuilder xml =
        new StringBuilder("<ListBucketResult><Name>")
            .append(bucket)
            .append("</Name><Prefix>")
            .append(escape(prefix))
            .append("</Prefix><IsTruncated>false</IsTruncated>")
            .append(contents);
    for (String commonPrefix : commonPrefixes) {
      xml.append("<CommonPrefixes><Prefix>")
          .append(escape(commonPrefix))
          .append("</Prefix></CommonPrefixes>");
    }
    return ok(request, xml.append("</ListBucketResult>").toString());
  }

  private static String bodyOf(Request request) throws IOException {
    if (request.body() == null) {
      return "";
    }
    Buffer buffer = new Buffer();
    request.body().writeTo(buffer);
    return buffer.readString(StandardCharsets.ISO_8859_1);
  }

  private static String escape(String text) {
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  private static Response ok(Request request, String xml) {
    return response(request, 200, xml.getBytes(StandardCharsets.UTF_8)).build();
  }

  private static Response error(
      Request request, int code, String errorCode, String bucket, @Nullable String key) {
    String xml =
        "<Error><Code>"
            + errorCode
            + "</Code><Message>"
            + errorCode
            + "</Message><BucketName>"
            + bucket
            + "</BucketName>"
            + (key == null ? "" : "<Key>" + escape(key) + "</Key>")
            + "</Error>";
    byte[] body =
        request.method().equals("HEAD") ? new byte[0] : xml.getBytes(StandardCharsets.UTF_8);
    return response(request, code, body).build();
  }

  private static Response.Builder response(Request request, int code, byte[] body) {
    return new Response.Builder()
        .request(request)
        .protocol(Protocol.HTTP_1_1)
        .code(code)
        .message(String.valueOf(code))
        .header("Content-Type", "application/xml")
        .body(ResponseBody.create(body, XML));
  }
}

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
package cn.edu.tsinghua.iginx.filesystem.service.rpc;

import cn.edu.tsinghua.iginx.filesystem.thrift.RpcException;
import cn.edu.tsinghua.iginx.filesystem.thrift.Status;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.LoggerFactory;

/** HMAC covers the method and every argument; optional field 127 carries the MAC. */
public final class InternalAuth {
  private InternalAuth() {}

  public static String sign(String method, TBase<?, ?>... arguments) {
    return sign(SecurityConfig.getInstance().get("internal.secret"), method, arguments);
  }

  public static String sign(String secret, String method, TBase<?, ?>... arguments) {
    if (secret == null) return null;
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      value(output, method);
      for (TBase<?, ?> argument : arguments) value(output, argument);
      return Base64.getEncoder().encodeToString(mac.doFinal(bytes.toByteArray()));
    } catch (Exception e) {
      throw new IllegalStateException("Cannot compute internal credential", e);
    }
  }

  public static void verify(String supplied, String method, TBase<?, ?>... arguments)
      throws RpcException {
    SecurityConfig security = SecurityConfig.getInstance();
    if (security.get("internal.secret") == null) return;
    boolean valid =
        supplied != null
            && MessageDigest.isEqual(
                sign(method, arguments).getBytes(StandardCharsets.US_ASCII),
                supplied.getBytes(StandardCharsets.US_ASCII));
    if (!valid) {
      if ("required".equals(security.get("internal.auth"))) {
        throw new RpcException(Status.FileSystemException, "Internal authentication required");
      }
      LoggerFactory.getLogger(InternalAuth.class)
          .warn("Unauthenticated FileSystem RPC accepted in optional mode: {}", method);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void value(DataOutputStream out, Object value) throws IOException {
    if (value == null) {
      out.writeByte(0);
      return;
    }
    if (value instanceof TBase) {
      out.writeByte(1);
      TBase struct = (TBase) value;
      Map<? extends TFieldIdEnum, FieldMetaData> fields =
          FieldMetaData.getStructMetaDataMap(struct.getClass());
      List<TFieldIdEnum> ordered = new ArrayList<>(fields.keySet());
      ordered.sort(Comparator.comparingInt(TFieldIdEnum::getThriftFieldId));
      for (TFieldIdEnum field : ordered) {
        if (field.getThriftFieldId() == 127 || !struct.isSet(field)) continue;
        out.writeShort(field.getThriftFieldId());
        value(out, struct.getFieldValue(field));
      }
      out.writeShort(0);
    } else if (value instanceof Map) {
      out.writeByte(2);
      List<byte[]> entries = new ArrayList<>();
      for (Object item : ((Map) value).entrySet()) {
        Map.Entry entry = (Map.Entry) item;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes);
        value(data, entry.getKey());
        value(data, entry.getValue());
        entries.add(bytes.toByteArray());
      }
      entries.sort(
          (a, b) -> {
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
              int cmp = Integer.compare(a[i] & 255, b[i] & 255);
              if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
          });
      out.writeInt(entries.size());
      for (byte[] bytes : entries) {
        out.writeInt(bytes.length);
        out.write(bytes);
      }
    } else if (value instanceof Collection) {
      out.writeByte(3);
      out.writeInt(((Collection) value).size());
      for (Object item : (Collection) value) value(out, item);
    } else if (value instanceof ByteBuffer || value instanceof byte[]) {
      out.writeByte(4);
      ByteBuffer buffer =
          value instanceof ByteBuffer
              ? ((ByteBuffer) value).duplicate()
              : ByteBuffer.wrap((byte[]) value);
      out.writeInt(buffer.remaining());
      while (buffer.hasRemaining()) out.writeByte(buffer.get());
    } else {
      out.writeByte(5);
      byte[] bytes =
          (value.getClass().getName() + ":" + value.toString()).getBytes(StandardCharsets.UTF_8);
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }
}

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
package cn.edu.tsinghua.iginx.filesystem.security;

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

/**
 * A protocol-level client: checks CONNACK/SUBACK and actual delivery, including anonymous CONNECT.
 */
final class SecurityMqttClient implements AutoCloseable {
  final Socket socket;
  final int code;

  SecurityMqttClient(int port, String user, String password) throws Exception {
    socket = new Socket("127.0.0.1", port);
    socket.setSoTimeout(3000);
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(buffer);
    out.writeUTF("MQTT");
    out.writeByte(4);
    out.writeByte(user == null ? 2 : 0xc2);
    out.writeShort(30);
    out.writeUTF(java.util.UUID.randomUUID().toString());
    if (user != null) {
      out.writeUTF(user);
      out.writeUTF(password);
    }
    send(0x10, buffer.toByteArray());
    code = read(0x20)[1] & 255;
  }

  void send(int type, byte[] bytes) throws IOException {
    OutputStream out = socket.getOutputStream();
    out.write(type);
    int length = bytes.length;
    do {
      int digit = length % 128;
      length /= 128;
      out.write(length == 0 ? digit : digit | 128);
    } while (length > 0);
    out.write(bytes);
    out.flush();
  }

  byte[] read(int expected) throws IOException {
    DataInputStream in = new DataInputStream(socket.getInputStream());
    assertEquals(expected, in.readUnsignedByte());
    int length = 0, multiplier = 1, digit;
    do {
      digit = in.readUnsignedByte();
      length += (digit & 127) * multiplier;
      multiplier *= 128;
    } while ((digit & 128) != 0);
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return bytes;
  }

  int subscribe(String topic) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(buffer);
    out.writeShort(1);
    out.writeUTF(topic);
    out.writeByte(0);
    send(0x82, buffer.toByteArray());
    return read(0x90)[2] & 255;
  }

  void publish(String path, long key) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(buffer);
    out.writeUTF("events");
    out.write(
        ("[{\"path\":\""
                + path
                + "\",\"timestamp\":"
                + key
                + ",\"dataType\":\"long\",\"value\":42}]")
            .getBytes(StandardCharsets.UTF_8));
    send(0x30, buffer.toByteArray());
  }

  String receive() throws Exception {
    byte[] message = read(0x30);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(message));
    assertEquals("events", in.readUTF());
    byte[] body = new byte[in.available()];
    in.readFully(body);
    return new String(body, StandardCharsets.UTF_8);
  }

  void noMessage() throws Exception {
    socket.setSoTimeout(300);
    try {
      receive();
      fail("Subscriber received a publication");
    } catch (SocketTimeoutException expected) {
    } finally {
      socket.setSoTimeout(3000);
    }
  }

  void disconnect() throws Exception {
    send(0xe0, new byte[0]);
  }

  public void close() throws IOException {
    socket.close();
  }
}

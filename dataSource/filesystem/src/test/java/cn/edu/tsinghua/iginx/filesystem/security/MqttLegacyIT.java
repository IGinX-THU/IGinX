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

import static cn.edu.tsinghua.iginx.filesystem.security.SecurityTestEnvironment.*;
import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class MqttLegacyIT {
  @Test
  public void igt63_arbitraryCredentialsPublishSubscribeAndReconnect() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, null);
      node.config.setProperty("enableMqtt", "true");
      node.start();
      try (SecurityMqttClient subscriber =
          new SecurityMqttClient(node.mqtt, "unknown", "arbitrary")) {
        assertEquals(0, subscriber.code);
        assertEquals(0, subscriber.subscribe("events"));
        for (int i = 0; i < 2; i++)
          try (SecurityMqttClient publisher =
              new SecurityMqttClient(node.mqtt, "other", "anything")) {
            assertEquals(0, publisher.code);
            publisher.publish("legacy_mqtt.value", i + 1);
            assertTrue(subscriber.receive().contains("legacy_mqtt.value"));
            publisher.disconnect();
          }
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      long points;
      do {
        points = node.admin.execute("COUNT POINTS;").pointsNum;
        if (points == 2) break;
        Thread.sleep(50);
      } while (System.nanoTime() < deadline);
      assertEquals(2, points);
    }
  }
}

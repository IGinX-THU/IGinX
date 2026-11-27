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

import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class SecurityPortTest {
  @Test
  public void listenersAvoidAutomaticPortsAndPreviouslySelectedPorts() throws Exception {
    Set<Integer> selected = new HashSet<>();
    for (int i = 0; i < 128; i++) {
      int port = SecurityTestEnvironment.port();
      assertTrue(
          "Fixture port overlaps CI ephemeral ranges: " + port, port >= 10000 && port < 30000);
      assertTrue("Fixture port was assigned twice: " + port, selected.add(port));
      try (ServerSocket listener = new ServerSocket(port)) {
        assertEquals(port, listener.getLocalPort());
      }
    }
  }

  @Test
  public void activeListenerRemainsAvailableWhileOtherPortsAreSelected() throws Exception {
    int first = SecurityTestEnvironment.port();
    try (ServerSocket listener = new ServerSocket(first)) {
      for (int i = 0; i < 128; i++) {
        int port = SecurityTestEnvironment.port();
        assertNotEquals(first, port);
        try (ServerSocket other = new ServerSocket(port)) {
          assertEquals(port, other.getLocalPort());
        }
      }
      assertFalse(listener.isClosed());
    }
  }
}

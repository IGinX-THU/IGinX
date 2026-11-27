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

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/** Pausing drops existing connections and rejects new ones, rather than merely stalling reads. */
final class SecurityTcpProxy implements AutoCloseable {
  final ServerSocket listener;
  final int destination;
  final ExecutorService workers = Executors.newCachedThreadPool();
  final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
  volatile boolean paused, closed;

  SecurityTcpProxy(int destination) throws IOException {
    this.destination = destination;
    listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
    workers.submit(
        () -> {
          while (!closed)
            try {
              Socket client = listener.accept();
              if (paused) {
                client.close();
                continue;
              }
              Socket upstream = new Socket("127.0.0.1", destination);
              sockets.add(client);
              sockets.add(upstream);
              workers.submit(() -> copy(client, upstream));
              workers.submit(() -> copy(upstream, client));
            } catch (IOException e) {
              if (!closed) pause();
            }
        });
  }

  void copy(Socket source, Socket target) {
    try {
      byte[] bytes = new byte[8192];
      int n;
      while ((n = source.getInputStream().read(bytes)) != -1)
        target.getOutputStream().write(bytes, 0, n);
    } catch (IOException ignored) {
    } finally {
      for (Socket s : Arrays.asList(source, target)) {
        sockets.remove(s);
        try {
          s.close();
        } catch (IOException ignored) {
        }
      }
    }
  }

  void pause() {
    paused = true;
    for (Socket s : sockets)
      try {
        s.close();
      } catch (IOException ignored) {
      }
  }

  void resume() {
    paused = false;
  }

  public void close() throws Exception {
    closed = true;
    pause();
    listener.close();
    workers.shutdownNow();
    if (!workers.awaitTermination(5, TimeUnit.SECONDS))
      throw new IllegalStateException("Proxy threads did not terminate");
  }
}

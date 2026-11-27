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

import cn.edu.tsinghua.iginx.Iginx;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageTaskThreadPoolExecutor;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Test-only server: pause a real storage task before execution, then exercise revocation. */
public final class RestRevocationNode {
  private static final AtomicReference<Path> barrier = new AtomicReference<>();

  public static void main(String[] args) throws Exception {
    Thread controller =
        new Thread(
            () -> {
              try {
                control(Paths.get(System.getProperty("IGINX_HOME")));
              } catch (Exception error) {
                error.printStackTrace();
              }
            },
            "rest-revocation-test-controller");
    controller.setDaemon(true);
    controller.start();
    Iginx.main(args);
  }

  @SuppressWarnings("unchecked")
  private static void control(Path home) throws Exception {
    Field field = StorageManager.class.getDeclaredField("storageMap");
    field.setAccessible(true);
    Map<Long, Pair<IStorage, ThreadPoolExecutor>> pools =
        (Map<Long, Pair<IStorage, ThreadPoolExecutor>>) field.get(null);
    long ready = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
    while (pools.isEmpty() && System.nanoTime() < ready) Thread.sleep(10);
    if (pools.isEmpty()) throw new IllegalStateException("Storage executor did not initialize");
    for (Map.Entry<Long, Pair<IStorage, ThreadPoolExecutor>> entry : pools.entrySet()) {
      Pair<IStorage, ThreadPoolExecutor> previous = entry.getValue();
      ThreadPoolExecutor gated =
          new StorageTaskThreadPoolExecutor(
              previous.v.getCorePoolSize(),
              Integer.MAX_VALUE,
              60,
              TimeUnit.SECONDS,
              new SynchronousQueue<>()) {
            @Override
            protected void beforeExecute(Thread worker, Runnable task) {
              super.beforeExecute(worker, task);
              Path active = barrier.get();
              if (active == null) return;
              try {
                Files.write(active.resolve("entered"), new byte[0]);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (!Files.exists(active.resolve("release"))) {
                  if (System.nanoTime() >= until)
                    throw new IllegalStateException("Test failed to release query");
                  Thread.sleep(2);
                }
              } catch (Exception error) {
                throw new IllegalStateException(error);
              }
            }
          };
      entry.setValue(new Pair<>(previous.k, gated));
      previous.v.shutdown();
      if (!previous.v.awaitTermination(5, TimeUnit.SECONDS))
        throw new IllegalStateException("Old storage executor did not stop");
    }
    Files.write(home.resolve("rest-revocation-ready"), new byte[0]);
    Path command = home.resolve("rest-revocation-command");
    while (true) {
      if (!Files.exists(command)) {
        Thread.sleep(2);
        continue;
      }
      Path active = home.resolve(new String(Files.readAllBytes(command), StandardCharsets.UTF_8));
      Files.delete(command);
      barrier.set(active);
      Files.write(active.resolve("armed"), new byte[0]);
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (!Files.exists(active.resolve("release")) && System.nanoTime() < until) Thread.sleep(2);
      barrier.set(null);
      Files.write(active.resolve("finished"), new byte[0]);
    }
  }
}

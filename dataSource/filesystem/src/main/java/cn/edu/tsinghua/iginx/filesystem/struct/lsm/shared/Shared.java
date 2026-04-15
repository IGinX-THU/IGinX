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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.FileLsmConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.DBConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class Shared implements Closeable {
  private final DBConfig config;

  private final Semaphore flusherPermits;

  private final Semaphore memTablePermits;

  private final Semaphore scannerPermits;

  private final CachePool cachePool;

  private final BufferAllocator allocator;

  public Shared(
      DBConfig config,
      Semaphore flusherPermits,
      Semaphore memTablePermits,
      Semaphore scannerPermits,
      CachePool cachePool,
      BufferAllocator allocator) {
    this.config = config;
    this.flusherPermits = flusherPermits;
    this.memTablePermits = memTablePermits;
    this.scannerPermits = scannerPermits;
    this.cachePool = cachePool;
    this.allocator = allocator;
  }

  public static Shared of(FileLsmConfig config) {
    SharedConfig sharedConfig = config.getShared();
    Semaphore flusherPermits = new Semaphore(sharedConfig.getWriters(), true);
    Semaphore memTablePermits = new Semaphore(sharedConfig.getMemtableQueue(), true);
    Semaphore scannerPermits = new Semaphore(sharedConfig.getScanners(), true);
    CachePool cachePool = new CachePool(sharedConfig.getCache());
    BufferAllocator allocator = new RootAllocator();
    return new Shared(config.getDb(), flusherPermits, memTablePermits, scannerPermits, cachePool, allocator);
  }

  public DBConfig getConfig() {
    return config;
  }

  public Semaphore getFlusherPermits() {
    return flusherPermits;
  }

  public Semaphore getMemTablePermits() {
    return memTablePermits;
  }

  public Semaphore getScannerPermits() {
    return scannerPermits;
  }

  public CachePool getCachePool() {
    return cachePool;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public void close() throws IOException {
    cachePool.asMap().clear();
    allocator.close();
  }


}

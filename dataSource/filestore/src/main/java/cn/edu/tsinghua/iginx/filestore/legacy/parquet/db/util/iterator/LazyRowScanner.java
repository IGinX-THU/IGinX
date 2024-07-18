/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.legacy.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.filestore.legacy.parquet.util.exception.StorageException;

public class LazyRowScanner<K extends Comparable<K>, F, V> implements Scanner<K, Scanner<F, V>> {

  private final RowScannerFactory<K, F, V> factory;

  public LazyRowScanner(RowScannerFactory<K, F, V> factory) {
    this.factory = factory;
  }

  private Scanner<K, Scanner<F, V>> scanner;

  @Override
  public K key() {
    return scanner.key();
  }

  @Override
  public Scanner<F, V> value() {
    return scanner.value();
  }

  @Override
  public boolean iterate() throws StorageException {
    if (scanner == null) {
      scanner = factory.create();
    }
    return scanner.iterate();
  }

  @Override
  public void close() throws StorageException {
    if (scanner != null) {
      scanner.close();
    }
  }
}

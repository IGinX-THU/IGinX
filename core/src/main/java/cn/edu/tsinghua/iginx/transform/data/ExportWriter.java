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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.utils.Mutex;

public abstract class ExportWriter implements Writer, Exporter {

  private final Mutex mutex = new Mutex();

  public ExportWriter() {}

  @Override
  public void writeBatch(BatchData batchData) {
    write(batchData);

    // call the JobRunner to send next batch of data.
    mutex.unlock();
  }

  public abstract void write(BatchData batchData);

  @Override
  public Mutex getMutex() {
    return mutex;
  }
}

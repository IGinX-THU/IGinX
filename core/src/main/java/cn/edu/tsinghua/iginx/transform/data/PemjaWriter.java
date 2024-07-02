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
import cn.edu.tsinghua.iginx.transform.driver.PemjaWorker;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;

public class PemjaWriter implements Writer {

  private final PemjaWorker worker;

  public PemjaWriter(PemjaWorker worker) {
    this.worker = worker;
  }

  @Override
  public void writeBatch(BatchData batchData) throws WriteBatchException {
    worker.process(batchData);
  }
}

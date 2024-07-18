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
package cn.edu.tsinghua.iginx.filestore;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public class FileStore implements IStorage {
  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void release() throws PhysicalException {
    throw new UnsupportedOperationException();
  }
}

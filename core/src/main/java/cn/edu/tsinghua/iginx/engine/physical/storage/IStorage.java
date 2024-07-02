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
package cn.edu.tsinghua.iginx.engine.physical.storage;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public interface IStorage {
  /** 对非叠加分片查询数据 */
  TaskExecuteResult executeProject(Project project, DataArea dataArea);

  /** 对叠加分片查询数据 */
  TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea /*dummy area*/);

  /** 询问底层是否支持带谓词下推的查询 */
  boolean isSupportProjectWithSelect();

  /** 对非叠加分片带谓词下推的查询 */
  TaskExecuteResult executeProjectWithSelect(Project project, Select select, DataArea dataArea);

  /** 对叠加分片带谓词下推的查询 */
  TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea);

  /** 对非叠加分片删除数据 */
  TaskExecuteResult executeDelete(Delete delete, DataArea dataArea);

  /** 对某一分片插入数据 */
  TaskExecuteResult executeInsert(Insert insert, DataArea dataArea);

  /** 获取所有列信息 */
  List<Column> getColumns() throws PhysicalException;

  /** 获取指定前缀的数据边界 */
  Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix) throws PhysicalException;

  /** 释放底层连接 */
  void release() throws PhysicalException;
}

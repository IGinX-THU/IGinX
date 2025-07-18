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
package cn.edu.tsinghua.iginx.engine.physical.storage;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import io.reactivex.rxjava3.core.Flowable;
import java.util.Set;

public interface IStorage {
  /** 测试数据库连接 */
  boolean testConnection(StorageEngineMeta meta);

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

  /** 询问底层是否支持仅带AGG的查询 */
  default boolean isSupportProjectWithAgg(Operator agg, DataArea dataArea, boolean isDummy) {
    return false;
  }

  default boolean isSupportProjectWithAggSelect(
      Operator agg, Select select, DataArea dataArea, boolean isDummy) {
    return false;
  }

  /** 对非叠加分片带Agg带谓词下推的查询 */
  TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea);

  /** 对叠加分片带Agg带谓词下推的查询 */
  TaskExecuteResult executeProjectDummyWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea);

  /** 对非叠加分片带Agg的查询 */
  TaskExecuteResult executeProjectWithAgg(Project project, Operator agg, DataArea dataArea);

  /** 对叠加分片带Agg的查询 */
  TaskExecuteResult executeProjectDummyWithAgg(Project project, Operator agg, DataArea dataArea);

  /** 对非叠加分片删除数据 */
  TaskExecuteResult executeDelete(Delete delete, DataArea dataArea);

  /** 对某一分片插入数据 */
  TaskExecuteResult executeInsert(Insert insert, DataArea dataArea);

  /** 获取所有列信息 */
  Flowable<Column> getColumns(Set<String> patterns, TagFilter tagFilter) throws PhysicalException;

  /** 获取指定前缀的数据边界 */
  Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix) throws PhysicalException;

  /** 释放底层连接 */
  void release() throws PhysicalException;
}

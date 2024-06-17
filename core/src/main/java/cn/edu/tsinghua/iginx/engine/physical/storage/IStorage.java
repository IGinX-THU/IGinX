/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;
import java.util.Set;

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
  List<Column> getColumns(Set<String> patterns, TagFilter tagFilter) throws PhysicalException;

  /** 获取指定前缀的数据边界 */
  Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix) throws PhysicalException;

  /** 释放底层连接 */
  void release() throws PhysicalException;
}

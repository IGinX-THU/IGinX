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
package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.UnaryRowMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStreams;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MigrationPhysicalExecutor {

  private static final MigrationPhysicalExecutor INSTANCE = new MigrationPhysicalExecutor();

  public static MigrationPhysicalExecutor getInstance() {
    return INSTANCE;
  }

  public RowStream execute(
      RequestContext ctx, Migration migration, StoragePhysicalTaskExecutor storageTaskExecutor)
      throws PhysicalException {
    FragmentMeta toMigrateFragment = migration.getFragmentMeta();
    StorageUnitMeta targetStorageUnitMeta = migration.getTargetStorageUnitMeta();
    KeyInterval keyInterval = toMigrateFragment.getKeyInterval();
    List<String> paths = migration.getPaths();

    // 查询分区数据
    List<Operator> projectOperators = new ArrayList<>();
    Project project = new Project(new FragmentSource(toMigrateFragment), paths, null);
    projectOperators.add(project);
    StoragePhysicalTask projectPhysicalTask = new StoragePhysicalTask(projectOperators, ctx);

    List<Filter> selectTimeFilters = new ArrayList<>();
    selectTimeFilters.add(new KeyFilter(Op.GE, keyInterval.getStartKey()));
    selectTimeFilters.add(new KeyFilter(Op.L, keyInterval.getEndKey()));
    Select selectOperator =
        new Select(new OperatorSource(project), new AndFilter(selectTimeFilters), null);
    MemoryPhysicalTask<RowStream> selectPhysicalTask =
        new UnaryRowMemoryPhysicalTask(projectPhysicalTask, selectOperator, ctx);
    projectPhysicalTask.setFollowerTask(selectPhysicalTask);

    storageTaskExecutor.commit(projectPhysicalTask);

    try (TaskResult<RowStream> projectResult = selectPhysicalTask.getResult().get();
        RowStream selectRowStream = projectResult.unwrap()) {
      List<String> selectResultPaths = new ArrayList<>();
      List<DataType> selectResultTypes = new ArrayList<>();
      selectRowStream
          .getHeader()
          .getFields()
          .forEach(
              field -> {
                selectResultPaths.add(field.getName());
                selectResultTypes.add(field.getType());
              });

      List<Long> timestampList = new ArrayList<>();
      List<ByteBuffer> valuesList = new ArrayList<>();
      List<Bitmap> bitmapList = new ArrayList<>();
      List<ByteBuffer> bitmapBufferList = new ArrayList<>();

      boolean hasTimestamp = selectRowStream.getHeader().hasKey();
      while (selectRowStream.hasNext()) {
        Row row = selectRowStream.next();
        Object[] rowValues = row.getValues();
        valuesList.add(ByteUtils.getRowByteBuffer(rowValues, selectResultTypes));
        Bitmap bitmap = new Bitmap(rowValues.length);
        for (int i = 0; i < rowValues.length; i++) {
          if (rowValues[i] != null) {
            bitmap.mark(i);
          }
        }
        bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
        bitmapList.add(bitmap);
        if (hasTimestamp) {
          timestampList.add(row.getKey());
        }

        // 按行批量插入数据
        if (timestampList.size()
            == ConfigDescriptor.getInstance().getConfig().getMigrationBatchSize()) {
          insertDataByBatch(
              ctx,
              timestampList,
              valuesList,
              bitmapList,
              bitmapBufferList,
              toMigrateFragment,
              selectResultPaths,
              selectResultTypes,
              targetStorageUnitMeta.getId(),
              storageTaskExecutor);
          timestampList.clear();
          valuesList.clear();
          bitmapList.clear();
          bitmapBufferList.clear();
        }
      }
      insertDataByBatch(
          ctx,
          timestampList,
          valuesList,
          bitmapList,
          bitmapBufferList,
          toMigrateFragment,
          selectResultPaths,
          selectResultTypes,
          targetStorageUnitMeta.getId(),
          storageTaskExecutor);
      return RowStreams.empty();
    } catch (InterruptedException | ExecutionException e) {
      throw new PhysicalException(e);
    }
  }

  private void insertDataByBatch(
      RequestContext ctx,
      List<Long> timestampList,
      List<ByteBuffer> valuesList,
      List<Bitmap> bitmapList,
      List<ByteBuffer> bitmapBufferList,
      FragmentMeta toMigrateFragment,
      List<String> selectResultPaths,
      List<DataType> selectResultTypes,
      String storageUnitId,
      StoragePhysicalTaskExecutor storageTaskExecutor)
      throws PhysicalException {
    // 按行批量插入数据
    RawData rowData =
        new RawData(
            selectResultPaths,
            Collections.emptyList(),
            timestampList,
            ByteUtils.getRowValuesByDataType(valuesList, selectResultTypes, bitmapBufferList),
            selectResultTypes,
            bitmapList,
            RawDataType.NonAlignedRow);
    RowDataView rowDataView =
        new RowDataView(rowData, 0, selectResultPaths.size(), 0, timestampList.size());
    List<Operator> insertOperators = new ArrayList<>();
    insertOperators.add(new Insert(new FragmentSource(toMigrateFragment), rowDataView));
    StoragePhysicalTask insertPhysicalTask = new StoragePhysicalTask(insertOperators, ctx);
    storageTaskExecutor.commitWithTargetStorageUnitId(insertPhysicalTask, storageUnitId);
    try {
      insertPhysicalTask.getResult().get().close();
    } catch (InterruptedException | ExecutionException e) {
      throw new PhysicalException(e);
    }
  }
}

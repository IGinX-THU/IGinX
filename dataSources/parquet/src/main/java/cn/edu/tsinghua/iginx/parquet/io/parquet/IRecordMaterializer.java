/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.io.parquet;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

class IRecordMaterializer extends RecordMaterializer<IRecord> {
  private final IGroupConverter root;

  private IRecord currentRecord = null;

  public IRecordMaterializer(MessageType schema) {
    this.root =
        new IGroupConverter(
            schema,
            record -> {
              currentRecord = record;
            });
  }

  @Override
  public void skipCurrentRecord() {}

  @Override
  public IRecord getCurrentRecord() {
    return currentRecord;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}

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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.combineMultipleColumns;

public class RowTransformLazyStream extends UnaryLazyStream {

    private final RowTransform rowTransform;

    private final List<Pair<RowMappingFunction, Map<String, Value>>> functionAndParamslist;

    private Row nextRow;

    private Header header;

    public RowTransformLazyStream(RowTransform rowTransform, RowStream stream) {
        super(stream);
        this.rowTransform = rowTransform;
        this.functionAndParamslist = new ArrayList<>();
        rowTransform.getFunctionCallList().forEach(functionCall -> {
            this.functionAndParamslist.add(new Pair<>((RowMappingFunction) functionCall.getFunction(), functionCall.getParams()));
        });
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            if (nextRow == null) {
                nextRow = calculateNext();
            }
            header = nextRow == null ? Header.EMPTY_HEADER : nextRow.getHeader();
        }
        return header;
    }

    private Row calculateNext() throws PhysicalException {
        while(stream.hasNext()) {
            List<Row> columnList = new ArrayList<>();
            functionAndParamslist.forEach(pair -> {
                RowMappingFunction function = pair.k;
                Map<String, Value> params = pair.v;
                Row column = null;
                try {
                    // 分别计算每个表达式得到相应的结果
                    column = function.transform(stream.next(), params);
                } catch (Exception e) {
                    try {
                        throw new PhysicalTaskExecuteFailureException("encounter error when execute row mapping function " + function.getIdentifier() + ".", e);
                    } catch (PhysicalTaskExecuteFailureException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                if (column != null) {
                    columnList.add(column);
                }
            });
            // 如果计算结果都不为空，将计算结果合并成一行
            if (columnList.size() == functionAndParamslist.size()) {
                return combineMultipleColumns(columnList);
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (nextRow == null) {
            nextRow = calculateNext();
        }
        return nextRow != null;
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }
}

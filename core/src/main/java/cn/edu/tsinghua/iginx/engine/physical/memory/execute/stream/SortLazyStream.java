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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import java.util.ArrayList;
import java.util.List;

public class SortLazyStream extends UnaryLazyStream {

    private final Sort sort;

    private final boolean asc;

    private final List<Row> rows;

    private boolean hasSorted = false;

    private int cur = 0;

    public SortLazyStream(Sort sort, RowStream stream) {
        super(stream);
        this.sort = sort;
        this.asc = sort.getSortType() == Sort.SortType.ASC;
        this.rows = new ArrayList<>();
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return stream.getHeader();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (!hasSorted) {
            while (stream.hasNext()) {
                rows.add(stream.next());
            }
            RowUtils.sortRows(rows, asc, sort.getSortByCols());
            hasSorted = true;
        }
        return cur < rows.size();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return rows.get(cur++);
    }
}

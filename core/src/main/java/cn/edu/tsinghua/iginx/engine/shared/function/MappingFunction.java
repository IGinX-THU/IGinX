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
package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public interface MappingFunction extends Function {

  /**
   * 此处的输入表从原来的RowStream改为Table,因为逻辑层重构后，一个输入表需要作为多个函数的输入，而RowStream只能被消费一次，因此改为使用可以多次消费的Table。
   * 注意如果函数使用next()来遍历Table,则需要在函数执行完毕后调用Table.reset()来重置Table的指针。
   */
  RowStream transform(Table table, FunctionParams params) throws Exception;
}

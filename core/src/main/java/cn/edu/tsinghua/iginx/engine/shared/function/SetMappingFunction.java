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
package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

public interface SetMappingFunction extends Function {

  /**
   * 此处的输入表从原来的RowStream改为Table,因为逻辑层重构后，一个输入表需要作为多个函数的输入，而RowStream只能被消费一次，因此改为使用可以多次消费的Table。
   * 注意如果函数使用next()来遍历Table,则需要在函数执行完毕后调用Table.reset()来重置Table的指针。
   */
  Row transform(Table table, FunctionParams params) throws Exception;
}

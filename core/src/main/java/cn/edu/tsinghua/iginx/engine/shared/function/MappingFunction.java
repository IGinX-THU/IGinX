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

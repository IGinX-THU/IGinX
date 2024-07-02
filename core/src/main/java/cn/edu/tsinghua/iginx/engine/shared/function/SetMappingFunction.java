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

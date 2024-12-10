package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.function.udf;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class UDTFFunction extends UDFFunction {

  private static final FunctionManager functionManager = FunctionManager.getInstance();

  private FunctionParams params;

  protected UDTFFunction(String identifier, FunctionParams params) {
    super(functionManager.getFunction(identifier));
    this.params = params;
  }

  /**
   * @param allocator
   * @param input
   * @return
   */
  @Override
  protected FieldVector call(BufferAllocator allocator, VectorSchemaRoot input) {
    // TODO:
    // 1. 将input、params传给python，input使用arrow的地址传输，params按照以前的旧方法直接传输
    // 2. python拿到input数据之后，如何调用用户写的eval函数？
    //    a. udtf: 逐行调用（是不是可以用pd.df的apply？或者直接用arrow的向量化计算? arrow支持吗）
    //    b. udaf & udsf: 转化为df，直接给用户
    // 3. 这里的call只能返回一列，或者说expression就返回一列（能不能直接用generator构造返回多列的？）
    //    所以让python将多列结果拼成一个 struct vector，这个复杂结构向量会在ProjectExecutor.computeImpl里被展开。
    // 4. python使用地址传输结果向量

    return null;
  }
}

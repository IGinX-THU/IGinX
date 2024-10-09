package cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowMapper extends PipelineExecutor{

  private static final Logger LOGGER = LoggerFactory.getLogger(RowMapper.class);

  private UnaryOperator operator;


  @Override
  public String getDescription() {
    return "RowMapper";
  }

  /**
   * @throws PhysicalException
   */
  @Override
  public void close() throws PhysicalException {

  }

  /**
   * @param inputSchema
   * @return
   * @throws PhysicalException
   */
  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws PhysicalException {
    return null;
  }

  /**
   * @param batch
   * @return
   * @throws PhysicalException
   */
  @Override
  protected Batch internalCompute(Batch batch) throws PhysicalException {
    return null;
  }
}

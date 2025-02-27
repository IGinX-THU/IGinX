package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStreamReader implements Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchStreamReader.class);

  private final BatchStream batchStream;

  public BatchStreamReader(BatchStream batchStream) {
    this.batchStream = batchStream;
  }

  @Override
  public boolean hasNextBatch() {
    try {
      return batchStream.hasNext();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to examine whether there is more data, because ", e);
      return false;
    }
  }

  @Override
  public BatchData loadNextBatch() {
    try (Batch batch = batchStream.getNext();
        VectorSchemaRoot root = batch.getData();
        ArrowReader reader = new ArrowReader(root, batch.getRowCount())) {
      // 这里ArrowReader的batchSize，和执行时batchStream的batchSize理论上应当相同，都是execute sql过程
      return reader.loadNextBatch();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to load next batch of data, because ", e);
      return null;
    }
  }

  @Override
  public void close() {
    try {
      batchStream.close();
    } catch (PhysicalException e) {
      LOGGER.error("Fail to close batch stream.", e);
    }
  }
}

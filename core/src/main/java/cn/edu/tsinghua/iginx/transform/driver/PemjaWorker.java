package cn.edu.tsinghua.iginx.transform.driver;

import static cn.edu.tsinghua.iginx.transform.utils.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.transform.utils.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.BatchData;
import cn.edu.tsinghua.iginx.transform.data.PemjaReader;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

public class PemjaWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PemjaWorker.class);

  private final String identifier;

  private final PythonInterpreter interpreter;

  private final Writer writer;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public PemjaWorker(String identifier, PythonInterpreter interpreter, Writer writer) {
    this.identifier = identifier;
    this.interpreter = interpreter;
    this.writer = writer;
  }

  public void process(BatchData batchData) {
    List<List<Object>> data = new ArrayList<>();

    List<Object> headerRow = new ArrayList<>();
    Header header = batchData.getHeader();
    if (header.hasKey()) {
      headerRow.add(GlobalConstant.KEY_NAME);
    }
    header.getFields().forEach(field -> headerRow.add(field.getFullName()));
    data.add(headerRow);

    batchData
        .getRowList()
        .forEach(
            row -> {
              if (row.getHeader().hasKey()) {
                List<Object> rowData = new ArrayList<>();
                rowData.add(row.getKey());
                rowData.addAll(Arrays.asList(row.getValues()));
                data.add(rowData);
              } else {
                data.add(Arrays.asList(row.getValues()));
              }
            });

    List<Object> res = (List<Object>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
    PemjaReader reader = new PemjaReader(res, config.getBatchSize());

    try {
      while (reader.hasNextBatch()) {
        BatchData nextBatchData = reader.loadNextBatch();
        writer.writeBatch(nextBatchData);
      }
    } catch (WriteBatchException e) {
      LOGGER.error("PemjaWorker identifier={} fail to writer data.", identifier, e);
    }
  }

  public String getIdentifier() {
    return identifier;
  }

  public PythonInterpreter getInterpreter() {
    return interpreter;
  }

  public Writer getWriter() {
    return writer;
  }
}

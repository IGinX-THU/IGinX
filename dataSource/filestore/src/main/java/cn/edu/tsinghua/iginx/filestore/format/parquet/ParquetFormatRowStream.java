package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreRowStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.WillCloseWhenClosed;
import shaded.iginx.org.apache.parquet.schema.MessageType;

public class ParquetFormatRowStream extends FileStoreRowStream {

  private final IParquetReader reader;
  private final Header header;
  private Row nextRow;

  public ParquetFormatRowStream(
      @WillCloseWhenClosed IParquetReader reader, Function<String, String> nameMapper)
      throws IOException {
    this.reader = reader;
    this.header = new Header(Field.KEY, toFields(reader.getSchema(), nameMapper));
    this.nextRow = fetchNext();
  }

  private static List<Field> toFields(MessageType schema, Function<String, String> nameMapper) {
    List<Field> fields = ProjectUtils.toFields(schema);
    List<Field> result = new ArrayList<>();
    for (Field field : fields) {
      String rawName = field.getName();
      String fullName = nameMapper.apply(rawName);
      result.add(new Field(fullName, field.getType()));
    }
    return result;
  }

  private Row fetchNext() throws IOException {
    IRecord record = reader.read();
    if (record == null) {
      return null;
    }
    long key = reader.getCurrentRowIndex();
    return ProjectUtils.toRow(header, key, record);
  }

  @Override
  public Header getHeader() throws FileStoreException {
    return header;
  }

  @Override
  public void close() throws FileStoreException {
    try {
      reader.close();
    } catch (IOException e) {
      throw new FileStoreException(e);
    }
  }

  @Override
  public boolean hasNext() throws FileStoreException {
    return nextRow != null;
  }

  @Override
  public Row next() throws FileStoreException {
    if (nextRow == null) {
      throw new FileStoreException("No more rows");
    }
    Row row = nextRow;
    try {
      nextRow = fetchNext();
    } catch (IOException e) {
      throw new FileStoreException(e);
    }
    return row;
  }
}

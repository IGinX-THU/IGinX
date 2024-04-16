package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.reader.IntReader;

@NotThreadSafe
public class IndexedFieldReader extends DelegateFieldReader {

  private final IntReader indexReader;

  public IndexedFieldReader(FieldReader reader, IntReader indexReader) {
    super(reader);
    this.indexReader = Preconditions.checkNotNull(indexReader);
    reset();
  }

  @Override
  public boolean isSet() {
    return indexReader.isSet() && super.isSet();
  }

  @Override
  public void reset() {
    setPosition(0);
  }

  @Override
  public int getPosition() {
    return indexReader.getPosition();
  }

  @Override
  public void setPosition(int index) {
    indexReader.setPosition(index);
    if (indexReader.isSet()) {
      super.setPosition(indexReader.readInteger());
    }
  }
}

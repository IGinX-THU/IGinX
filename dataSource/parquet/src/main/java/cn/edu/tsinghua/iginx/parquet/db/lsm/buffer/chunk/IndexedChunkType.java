package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

public enum IndexedChunkType {
  SKIP_LIST(SkipListChunk::new),
  NONE(NoIndexChunk::new);

  private final IndexedChunk.Factory factory;

  IndexedChunkType(IndexedChunk.Factory factory) {
    this.factory = factory;
  }

  public IndexedChunk.Factory factory() {
    return factory;
  }
}

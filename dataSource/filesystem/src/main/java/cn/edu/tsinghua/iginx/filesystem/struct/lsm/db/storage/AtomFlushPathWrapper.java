package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage;

import org.apache.commons.io.file.PathUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Wraps a target path (file or directory) for atomic write operations.
 *
 * <p>Usage pattern:
 *
 * <pre>{@code
 * try (AtomFlushPathWrapper wrapper = new AtomFlushPathWrapper(targetPath)) {
 *   // write data to wrapper.getTmpPath()
 *   ...
 *   wrapper.commit(); // atomically rename tmp -> target on success
 * }
 * // If commit() was never called (e.g. exception thrown), close() cleans up the tmp path.
 * }</pre>
 */
public class AtomFlushPathWrapper implements Closeable {

  private final Path path;
  private final Path tmpPath;
  private final boolean overwrite;
  private boolean committed = false;

  public AtomFlushPathWrapper(Path path,boolean overwrite) throws IOException {
    this.path = Objects.requireNonNull(path);
    this.tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
    this.overwrite = overwrite;
  }

  public Path getTmpPath() {
    return tmpPath;
  }

  /**
   * Atomically moves the tmp path to the target path. Must be called after all data has been
   * successfully written to {@link #getTmpPath()}.
   */
  public void commit() throws IOException {
    List<CopyOption> options = new ArrayList<>();
    if(overwrite) {
      options.add(StandardCopyOption.REPLACE_EXISTING);
    }
    Files.move(tmpPath, path, options.toArray(new CopyOption[0]));
    committed = true;
  }

  /**
   * If {@link #commit()} was not called (write failed or was interrupted), deletes the tmp path to
   * avoid leaving a partial/corrupt file or directory behind. Handles both files and directories.
   */
  @Override
  public void close() throws IOException {
    if (!committed) {
      PathUtils.delete(tmpPath);
    }
  }
}


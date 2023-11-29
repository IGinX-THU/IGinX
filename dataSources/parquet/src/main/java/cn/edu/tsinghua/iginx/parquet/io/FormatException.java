package cn.edu.tsinghua.iginx.parquet.io;

import java.io.IOException;

public class FormatException extends IOException {
        public FormatException(String message) {
            super(message);
        }

        public FormatException(Throwable cause) {
            super(cause);
        }

        public FormatException(String message, Throwable cause) {
            super(message, cause);
        }
}

package cn.edu.tsinghua.iginx.rest.exception;

public class RESTIllegalArgumentException extends RESTException {

        public RESTIllegalArgumentException() {
            super();
        }

        public RESTIllegalArgumentException(String message) {
            super(message);
        }

        public RESTIllegalArgumentException(String message, Throwable cause) {
            super(message, cause);
        }

        public RESTIllegalArgumentException(Throwable cause) {
            super(cause);
        }

        public RESTIllegalArgumentException(
                String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
}

/** IGinX异常处理类。 */
package cn.edu.tsinghua.iginx.exception;
/**
 * IGinX中core模块下所有自定义异常的基类。
 *
 * <p>异常层次结构：
 *
 * <pre>
 * Exception
 * └─IginXException
 *     ├─EngineException           // engine目录下异常基类
 *         ├─PhysicalException
 *         └─LogicalException
 *     ├─MetaStorageException      // meta目录下异常基类
 *     └─SQLParserException        // sql目录下异常基类
 * </pre>
 */
class IginXException extends Exception {}

/** session模块下所有自定义异常的基类。 */
class SessionException extends Exception {}

/** client模块下所有自定义异常的基类。 */
class ClientException extends Exception {}

/**
 * dataSources模块下，各个数据库自定义异常的基类，所有这些异常都继承自PhysicalException。
 *
 * <p>具体异常：
 *
 * <ul>
 *   <li>FilesystemException: 文件系统模块下的基类。
 *   <li>PostgreSQLException: PostgreSQL模块下的基类。
 *   <li>IoTDBException: IoTDB模块下的基类。
 * </ul>
 */
class PhysicalException extends Exception {}

/** filesystem模块下所有自定义异常的基类。 */
class FilesystemException extends PhysicalException {}

/** IoTDB12模块下所有自定义异常的基类。 */
class IoTDBException extends PhysicalException {}

/** PostgreSQL模块下所有自定义异常的基类。 */
class PostgreSQLException extends PhysicalException {}

/**
 * IGinX异常处理最佳实践：
 *
 * <ol>
 *   <li>使用 try-with-resource 自动关闭资源。
 *   <li>捕获异常后使用描述性语言记录错误信息，如果是调用外部服务最好包括入参和出参：
 *       <pre>{@code logger.error("说明信息，异常信息：{}", e.getMessage(), e);}</pre>
 *   <li>不要同时记录和抛出异常，因为异常会被打印多次。正确的处理方式要么抛出异常要么记录异常，如果抛出异常，不要原封不动地抛出，可以自定义异常抛出：
 *       <pre>{@code throw new MyException("my exception", e);}</pre>
 *   <li>自定义异常不要丢弃原有异常，应该将原始异常传入自定义异常中。
 *   <li>自定义异常尽量不要使用检查异常。
 *   <li>尽可能晚的捕获异常，如非必要，建议所有的异常都不要在下层捕获，而应该由最上层捕获并统一处理这些异常。
 *   <li>为了避免重复输出异常日志，建议所有的异常日志都统一交由最上层输出。即使下层捕获到了某个异常，如非特殊情况，也不要将异常信息输出，应该交给最上层统一输出日志。
 * </ol>
 */

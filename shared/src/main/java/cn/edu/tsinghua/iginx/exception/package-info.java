/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * IGinX异常处理规范。
 *
 * <p><b>core模块</b>
 *
 * <p>IginxException是IGinX中在core模块下所有自定义异常的基类。
 *
 * <blockquote>
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
 *
 * </blockquote>
 *
 * <p><b>session模块</b>
 *
 * <p>SessionException是session模块下所有自定义异常的基类。
 *
 * <blockquote>
 *
 * <pre>
 * Exception
 * └─SessionException
 * </pre>
 *
 * </blockquote>
 *
 * <p><b>client模块</b>
 *
 * <p>ClientException是client模块下所有自定义异常的基类。
 *
 * <blockquote>
 *
 * <pre>
 * Exception
 * └─ClientException
 * </pre>
 *
 * </blockquote>
 *
 * <p><b>dataSources模块</b>
 *
 * <p>dataSources下各个数据库自定义异常，但是每个数据库的异常都继承PhysicalException。 每个对接层不能直接用 PhysicalException
 * ，每个对接层模块需要自己定义该模块异常。比如，PostgreSQLException，需要继承PhysicalException。
 *
 * <blockquote>
 *
 * <pre>
 * Exception
 * └─PhysicalException
 *     ├─FilesystemException
 *     ├─PostgreSQLException
 *     └─IoTDBException
 * </pre>
 *
 * </blockquote>
 *
 * <p><b>Filesystem模块</b>
 *
 * <p>FilesystemException是filesystem模块下所有自定义异常的基类。
 *
 * <blockquote>
 *
 * <pre>
 * PhysicalException
 * └─FilesystemException
 * </pre>
 *
 * </blockquote>
 *
 * <p><b>IoTDB12模块</b>
 *
 * <p>IoTDBException是IoTDB12模块下所有自定义异常的基类。
 *
 * <blockquote>
 *
 * <pre>
 * PhysicalException
 * └─IoTDBException
 * </pre>
 *
 * </blockquote>
 *
 * <p><b>PostgreSQL模块</b>
 *
 * <p>PostgreSQLException是PostgreSQL模块下所有自定义异常的基类。
 *
 * <blockquote>
 *
 * <pre>
 * PhysicalException
 * └─PostgreSQLException
 * </pre>
 *
 * </blockquote>
 *
 * <p><b>异常处理最佳实践</b>
 *
 * <ol>
 *   <li>使用 try-with-resource 自动关闭资源。
 *   <li>捕获异常后使用描述性语言记录错误信息，如果是调用外部服务最好包括入参和出参：
 *       <blockquote>
 *       {@code logger.error("说明信息，异常信息：{}", e.getMessage(), e);}
 *       </blockquote>
 *   <li>不要同时记录和抛出异常，因为异常会被打印多次。正确的处理方式要么抛出异常要么记录异常，如果抛出异常，不要原封不动地抛出，可以自定义异常抛出：
 *       <blockquote>
 *       {@code throw new MyException("my exception", e);}
 *       </blockquote>
 *   <li>自定义异常不要丢弃原有异常，应该将原始异常传入自定义异常中。
 *   <li>自定义异常尽量不要使用检查异常。
 *   <li>尽可能晚的捕获异常，如非必要，建议所有的异常都不要在下层捕获，而应该由最上层捕获并统一处理这些异常。
 *   <li>为了避免重复输出异常日志，建议所有的异常日志都统一交由最上层输出。即使下层捕获到了某个异常，如非特殊情况，也不要将异常信息输出，应该交给最上层统一输出日志。
 * </ol>
 */
package cn.edu.tsinghua.iginx.exception;

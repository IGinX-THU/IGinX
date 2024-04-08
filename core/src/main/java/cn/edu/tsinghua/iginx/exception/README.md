## IGinX异常规范
### core模块
`IginxException`是IGinX中在`core`模块下所有自定义异常的基类。
```text
Exception
└─IginXException
    ├─EngineException           // engine目录下异常基类
        ├─PhysicalException
        └─LogicalException
    ├─MetaStorageException      // meta目录下异常基类
    └─SQLParserException        // sql目录下异常基类
```

### session模块
`SessionException`是session模块下所有自定义异常的基类。
```text
Exception
└─SessionException
```

### client模块
`ClientException`是client模块下所有自定义异常的基类。
```text
Exception
└─ClientException
```

### dataSources模块
`dataSources`下各个数据库自定义异常，但是每个数据库的异常都继承`PhysicalException`。  
每个对接层不能直接用 PhysicalException ，每个对接层模块需要自己定义该模块异常。比如，PostgreSQLException，需要继承PhysicalException。  
```text
Exception
└─PhysicalException
    ├─FilesystemException
    ├─PostgreSQLException
    └─IoTDBException
```
##### Filesystem模块
`FilesystemException`是filesystem模块下所有自定义异常的基类。
```text
PhysicalException
└─FilesystemException
```
##### IoTDB12模块
`IoTDBException`是IoTDB12模块下所有自定义异常的基类。
```text
PhysicalException
└─IoTDBException
```
##### PostgreSQL模块
`PostgreSQLException`是PostgreSQL模块下所有自定义异常的基类。
```text
PhysicalException
└─PostgreSQLException
```

## 异常处理最佳实践
1、使用 try-with-resource 关闭资源。  
2、捕获异常后使用描述性语言记录错误信息，如果是调用外部服务最好是包括入参和出参。
```text
logger.error("说明信息，异常信息：{}", e.getMessage(), e)
```
3、不要同时记录和抛出异常，因为异常会打印多次，正确的处理方式要么抛出异常要么记录异常，如果抛出异常，不要原封不动的抛出，可以自定义异常抛出。   
4、自定义异常不要丢弃原有异常，应该将原始异常传入自定义异常中。  
```text
throw MyException("my exception", e);
```
5、自定义异常尽量不要使用检查异常。   
6、尽可能晚的捕获异常，如非必要，建议所有的异常都不要在下层捕获，而应该由最上层捕获并统一处理这些异常。   
7、为了避免重复输出异常日志，建议所有的异常日志都统一交由最上层输出。就算下层捕获到了某个异常，如非特殊情况，也不要将异常信息输出，应该交给最上层统一输出日志。

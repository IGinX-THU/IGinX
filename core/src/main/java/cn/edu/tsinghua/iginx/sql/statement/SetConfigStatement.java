package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.ExecutionException;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetConfigStatement extends SystemStatement {

  private static final Logger logger = LoggerFactory.getLogger(SetConfigStatement.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final String configName;

  private final String configValue;

  public SetConfigStatement(String configName, String configValue) {
    this.statementType = StatementType.SET_CONFIG;
    this.configName = configName;
    this.configValue = configValue;
  }

  @Override
  public void execute(RequestContext ctx) throws ExecutionException {
    Class<Config> clazz = Config.class;
    try {
      Field field = clazz.getDeclaredField(configName);
      field.setAccessible(true);
      Object oldVal = field.get(config);
      Object newVal = transformConfigValueStrToSameType(oldVal, configValue);
      field.set(config, newVal);
      ctx.setResult(new Result(RpcUtils.SUCCESS));
    } catch (NoSuchFieldException e) {
      String errMsg = String.format("no such field, field=%s", configName);
      logger.error(errMsg);
      throw new ExecutionException(errMsg);
    } catch (IllegalAccessException e) {
      String errMsg = String.format("set %s=%s error", configName, configValue);
      logger.error(errMsg);
      throw new ExecutionException(errMsg);
    }
  }

  private static Object transformConfigValueStrToSameType(Object oldVal, String newVal) {
    if (oldVal instanceof Boolean) {
      return Boolean.valueOf(newVal);
    } else if (oldVal instanceof Integer) {
      return Integer.valueOf(newVal);
    } else if (oldVal instanceof Long) {
      return Long.valueOf(newVal);
    } else if (oldVal instanceof Float) {
      return Float.valueOf(newVal);
    } else if (oldVal instanceof Double) {
      return Double.valueOf(newVal);
    } else if (oldVal instanceof byte[]) {
      return newVal.getBytes();
    } else {
      return newVal;
    }
  }
}

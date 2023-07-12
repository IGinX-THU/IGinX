package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowConfigStatement extends SystemStatement {

  private static final Logger logger = LoggerFactory.getLogger(ShowConfigStatement.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final String configName;

  public ShowConfigStatement(String configName) {
    this.statementType = StatementType.SHOW_CONFIG;
    this.configName = configName;
  }

  @Override
  public void execute(RequestContext ctx) throws ExecutionException {
    Class<Config> clazz = Config.class;
    try {
      Field field = clazz.getDeclaredField(configName);
      field.setAccessible(true);
      Object configValue = field.get(config);

      Result result = new Result(RpcUtils.SUCCESS);
      result.setConfigValue(String.valueOf(configValue));
      ctx.setResult(result);
    } catch (NoSuchFieldException e) {
      String errMsg = String.format("no such field, field=%s", configName);
      logger.error(errMsg);
      throw new ExecutionException(errMsg);
    } catch (IllegalAccessException e) {
      String errMsg = String.format("get config %s error", configName);
      logger.error(errMsg);
      throw new ExecutionException(errMsg);
    }
  }
}

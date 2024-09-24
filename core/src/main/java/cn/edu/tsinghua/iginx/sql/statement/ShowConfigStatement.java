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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowConfigStatement extends SystemStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowConfigStatement.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final String configName;

  public ShowConfigStatement(String configName) {
    this.statementType = StatementType.SHOW_CONFIG;
    this.configName = configName;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    Class<Config> clazz = Config.class;
    try {
      Map<String, String> configs = new HashMap<>();
      if (configName.equals("*")) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
          field.setAccessible(true);
          Object configValue = field.get(config);
          configs.put(field.getName(), String.valueOf(configValue));
        }
      } else {
        Field field = clazz.getDeclaredField(configName);
        field.setAccessible(true);
        Object configValue = field.get(config);
        configs.put(configName, String.valueOf(configValue));
      }

      Result result = new Result(RpcUtils.SUCCESS);
      result.setConfigs(configs);
      ctx.setResult(result);
    } catch (NoSuchFieldException e) {
      String errMsg = String.format("no such field, field=%s", configName);
      LOGGER.error(errMsg);
      throw new StatementExecutionException(errMsg);
    } catch (IllegalAccessException e) {
      String errMsg = String.format("get config %s error", configName);
      LOGGER.error(errMsg);
      throw new StatementExecutionException(errMsg);
    }
  }
}

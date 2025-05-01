/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.storage.execute.pushdown.strategy;

import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.List;

public class PushDownStrategyFactory {
  private static PushDownType determinePushDownType(
      List<Operator> operators, IStorage storage, DataArea dataArea, boolean isDummyStorageUnit) {
    System.out.println("determinePushDownType: " + operators);
    if (operators.size() >= 2) {
      OperatorType secondOpType = operators.get(1).getType();
      System.out.println("secondOpType: " + secondOpType);
      if (storage.isSupportProjectWithSelect() && secondOpType == OperatorType.Select) {
        return PushDownType.SelectPushDown;
      }
      if ((secondOpType == OperatorType.GroupBy || secondOpType == OperatorType.SetTransform)
          && storage.isSupportProjectWithAgg(operators.get(1), dataArea, isDummyStorageUnit)) {
        return PushDownType.AggPushDown;
      }
    }
    if (operators.size() >= 3) {
      OperatorType thirdOpType = operators.get(2).getType();
      System.out.println("thirdOpType: " + thirdOpType);
      if (operators.get(1).getType() == OperatorType.Select
          && (thirdOpType == OperatorType.GroupBy || thirdOpType == OperatorType.SetTransform)
          && storage.isSupportProjectWithAggSelect(
              operators.get(2), (Select) operators.get(1), dataArea, isDummyStorageUnit)) {
        return PushDownType.AggSelectPushDown;
      }
    }
    return PushDownType.NoPushDown;
  }

  public static PushDownStrategy getStrategy(
      List<Operator> operators, IStorage storage, DataArea dataArea, boolean isDummyStorageUnit) {
    PushDownType pushDownType =
        determinePushDownType(operators, storage, dataArea, isDummyStorageUnit);
    switch (pushDownType) {
      case SelectPushDown:
        return new SelectPushDownStrategy();
      case AggPushDown:
        return new AggPushDownStrategy();
      case AggSelectPushDown:
        return new AggSelectPushDownStrategy();
      default:
        return new NoPushDownStrategy();
    }
  }
}

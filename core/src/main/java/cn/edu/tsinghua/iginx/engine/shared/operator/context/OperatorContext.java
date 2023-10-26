package cn.edu.tsinghua.iginx.engine.shared.operator.context;

import cn.edu.tsinghua.iginx.engine.shared.ContextWarningMsgType;

public class OperatorContext {
  ContextWarningMsgType msgType;
  private String warningMsg;

  public OperatorContext() {}

  public OperatorContext(String warningMsg) {
    this.warningMsg = warningMsg;
  }

  public String getWarningMsg() {
    return warningMsg;
  }

  public void setWarningMsg(String warningMsg) {
    this.warningMsg = warningMsg;
  }

  public void setWarningType(ContextWarningMsgType warningType) {
    this.msgType = warningType;
  }
}

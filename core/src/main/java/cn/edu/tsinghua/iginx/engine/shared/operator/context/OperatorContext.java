package cn.edu.tsinghua.iginx.engine.shared.operator.context;

public class OperatorContext {
  ContextMsgType msgType;
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

  public void setWarningType(ContextMsgType warningType) {
    this.msgType = warningType;
  }
}

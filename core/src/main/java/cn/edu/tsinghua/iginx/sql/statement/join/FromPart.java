package cn.edu.tsinghua.iginx.sql.statement.join;

import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;

public class FromPart {

	private String path;
	private SelectStatement subStatement;
	private final boolean isSubStatement;
	private JoinCondition joinCondition;
	private final boolean isJoinPart;
	
	public FromPart(String path){
		this.path = path;
		this.isSubStatement = false;
		this.isJoinPart = false;
	}
	
	public FromPart(String path, JoinCondition joinCondition){
		this.path = path;
		this.isSubStatement = false;
		this.joinCondition = joinCondition;
		this.isJoinPart = true;
	}
	
	public FromPart(SelectStatement subStatement) {
		this.subStatement = subStatement;
		this.isSubStatement = true;
		this.isJoinPart = false;
	}
	
	public FromPart(SelectStatement subStatement, JoinCondition joinCondition) {
		this.path = subStatement.getGlobalAlias();
		this.subStatement = subStatement;
		this.isSubStatement = true;
		this.joinCondition = joinCondition;
		this.isJoinPart = true;
	}
	
	public String getPath() {
		return path;
	}
	
	public SelectStatement getSubStatement() {
		return subStatement;
	}
	
	public boolean isSubStatement() {
		return isSubStatement;
	}
	
	public JoinCondition getJoinCondition() {
		return joinCondition;
	}
	
	public boolean isJoinPart() {
		return isJoinPart;
	}
	
}

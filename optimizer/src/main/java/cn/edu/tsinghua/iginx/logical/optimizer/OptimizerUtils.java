package cn.edu.tsinghua.iginx.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptimizerUtils {
    public static boolean validateAggPushDown(Operator operator){
        if(operator == null){
            return false;
        }
        if(operator.getType() != OperatorType.GroupBy && operator.getType() != OperatorType.SetTransform){
            return false;
        }

        List<FunctionCall> functionCallList = new ArrayList<>();
        List< Expression > expressions = new ArrayList<>();

        if(operator.getType() == OperatorType.GroupBy){
            GroupBy groupBy = (GroupBy) operator;
            functionCallList = groupBy.getFunctionCallList();
            expressions.addAll(groupBy.getGroupByExpressions());
        }else{
            // SetTransform
            SetTransform setTransform = (SetTransform) operator;
            functionCallList = setTransform.getFunctionCallList();
        }

        for(FunctionCall fc: functionCallList){
            if(!Arrays.asList("AVG", "MAX", "MIN", "SUM", "COUNT").contains(fc.getFunction().getIdentifier().toUpperCase())){
                return false;
            }
            expressions.addAll(fc.getParams().getExpressions());
        }

        for(Expression expression: expressions){
            final boolean[] isValid = {true};
            expression.accept(new ExpressionVisitor() {
                @Override
                public void visit(BaseExpression expression) {

                }

                @Override
                public void visit(BinaryExpression expression) {

                }

                @Override
                public void visit(BracketExpression expression) {

                }

                @Override
                public void visit(ConstantExpression expression) {

                }

                @Override
                public void visit(FromValueExpression expression) {
                    isValid[0] = false;
                }

                @Override
                public void visit(FuncExpression expression) {

                }

                @Override
                public void visit(MultipleExpression expression) {

                }

                @Override
                public void visit(UnaryExpression expression) {

                }

                @Override
                public void visit(CaseWhenExpression expression) {
                    isValid[0] = false;
                }

                @Override
                public void visit(KeyExpression expression) {
                    isValid[0] = false;
                }

                @Override
                public void visit(SequenceExpression expression) {
                    isValid[0] = false;
                }
            });
            if(!isValid[0]){
                return false;
            }
        }



        return true;
    }



}

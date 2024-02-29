package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

import java.util.List;
import java.util.Map;


public class FragmentEliminationRule extends Rule{
    /*
        该规则是根据Project的Pattern来判断是否需要Fragment，与列裁剪规则有关，
        列裁剪规则裁剪了Project-Fragment中不需要的列，可能导致该Fragment不再需要
     */

    private static final class InstanceHolder {
        private static final FragmentEliminationRule instance = new FragmentEliminationRule();
    }

    public static FragmentEliminationRule getInstance() {
        return InstanceHolder.instance;
    }

    protected FragmentEliminationRule() {
        /*
         * we want to match the topology like:
         *          Project
         *              |
         *          Fragment
         */
        // Fragment的检测在matches中进行
        super("FragmentEliminationRule", operand(Project.class));
    }


    @Override
    public boolean matches(RuleCall call) {
        Project project = (Project) call.getMatchedRoot();
        return project.getSource() instanceof FragmentSource;
    }

    @Override
    public void onMatch(RuleCall call) {
        Project project = (Project) call.getMatchedRoot();
        FragmentSource fragmentSource = (FragmentSource) project.getSource();

        boolean needElimination = false;
        List<String> patterns = project.getPatterns();
        if(patterns.isEmpty()){
            needElimination = true;
        }

        ColumnsInterval columnsInterval = fragmentSource.getFragment().getColumnsInterval();
        for(String pattern: patterns){
            if(!columnsInterval.isContain(pattern)){
                needElimination = true;
                break;
            }
        }

        if(needElimination){
            eliminateFragment(call);
        }
    }

    /**
     * 从查询树中删除Fragment
     * 向上寻找到Binary节点（如Join、Union等），删除这一侧的分支
     * @param call RuleCall上下文
     */
    private void eliminateFragment(RuleCall call){
        Map<Operator, Operator> parentIndexMap = call.getParentIndexMap();
        Operator curOp = parentIndexMap.get(call.getMatchedRoot());
        Operator lastOp = call.getMatchedRoot();
        while(!OperatorType.isBinaryOperator(curOp.getType())){
            lastOp = curOp;
            curOp = parentIndexMap.get(curOp);
        }

        BinaryOperator binaryOperator = (BinaryOperator) curOp;
        Source savedSource;
        if(((OperatorSource)binaryOperator.getSourceA()).getOperator() == lastOp){
            savedSource = binaryOperator.getSourceB();}
        else{
            savedSource = binaryOperator.getSourceA();
        }

        Operator parent = parentIndexMap.get(curOp);

        if(parent != null){
            if(OperatorType.isUnaryOperator(parent.getType())){
                ((UnaryOperator)parent).setSource(savedSource);
            }
            else if(OperatorType.isBinaryOperator(parent.getType())){
                if(((BinaryOperator)parent).getSourceA() == curOp){
                    ((BinaryOperator)parent).setSourceA(savedSource);
                }
                else{
                    ((BinaryOperator)parent).setSourceB(savedSource);
                }
            }
        }
    }
}

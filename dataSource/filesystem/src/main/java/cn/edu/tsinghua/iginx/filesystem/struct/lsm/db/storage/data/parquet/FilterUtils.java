package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.ConstantExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.arrow.util.Preconditions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.*;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

public class FilterUtils {

    private FilterUtils() {
    }

    @Nullable
    public static Predicate toPaimonPredicate(Filter filter, Header header, RowType projectedSchema, List<Filter> unsupportedFilters) {
        List<Filter> filters = new ArrayList<>();
        if(filter instanceof  AndFilter){
            filters.addAll(((AndFilter) filter).getChildren());
        } else {
            filters.add(filter);
        }
        List<Predicate> predicates = new ArrayList<>();
        for (Filter f : filters) {
            try{
                Predicate predicate = toPaimonPredicate(f, header, projectedSchema);
                predicates.add(predicate);
            } catch (UnsupportedOperationException e) {
                 unsupportedFilters.add(f);
            }
        }
        if(predicates.isEmpty()) {
            return null;
        }
        return PredicateBuilder.and(predicates);
    }

    public static Predicate toPaimonPredicate(Filter filter, Header header, RowType projectedSchema) {
        switch (filter.getType()) {
            case Key:
                return toPaimonPredicate((KeyFilter) filter, projectedSchema);
            case Value:
                return toPaimonPredicate((ValueFilter) filter, header, projectedSchema);
            case And:
                return toPaimonPredicate((AndFilter) filter, header, projectedSchema);
            case Or:
                return toPaimonPredicate((OrFilter) filter, header, projectedSchema);
            case In:
                return toPaimonPredicate((InFilter) filter, header, projectedSchema);
            case Expr:
                return toPaimonPredicate(toValueFilter((ExprFilter) filter), header, projectedSchema);
            case Bool:
            case Path:
            case Not:
            default:
                throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
        }
    }

    private static ValueFilter toValueFilter(ExprFilter filter) {
        Expression exprA = filter.getExpressionA();
        Expression exprB = filter.getExpressionB();
        BaseExpression baseExpr;
        ConstantExpression constantExpr;
        if(exprA instanceof ConstantExpression && exprB instanceof BaseExpression) {
            baseExpr = (BaseExpression) exprB;
            constantExpr = (ConstantExpression) exprA;
        } else if(exprA instanceof BaseExpression && exprB instanceof ConstantExpression) {
            baseExpr = (BaseExpression) exprA;
            constantExpr = (ConstantExpression) exprB;
        } else {
            throw new UnsupportedOperationException("Unsupported expression types in ExprFilter: " + exprA.getClass() + " and " + exprB.getClass());
        }
        return new ValueFilter(baseExpr.getPathName(), filter.getOp(), new Value(constantExpr.getValue()));
    }

    private static Predicate toPaimonPredicate(KeyFilter filter, RowType projectedSchema) {
        PredicateBuilder builder = new PredicateBuilder(projectedSchema);
        switch (filter.getOp()) {
            case GE:
            case GE_AND:
                return builder.greaterOrEqual(0, filter.getValue());
            case G:
            case G_AND:
                return builder.greaterThan(0, filter.getValue());
            case LE:
            case LE_AND:
                return builder.lessOrEqual(0, filter.getValue());
            case L:
            case L_AND:
                return builder.lessThan(0, filter.getValue());
            case E:
            case E_AND:
                return builder.equal(0, filter.getValue());
            case NE:
            case NE_AND:
                return builder.notEqual(0, filter.getValue());
            case LIKE:
            case LIKE_AND:
            case NOT_LIKE:
            case NOT_LIKE_AND:
            default:
                throw new UnsupportedOperationException("Unsupported filter operator: " + filter.getOp());
        }
    }

    @Nullable
    private static Predicate toPaimonPredicate(InFilter filter, Header header, RowType projectedSchema) {
        PredicateBuilder builder = new PredicateBuilder(projectedSchema);

        int index = getIndex(filter.getPath(), header);
        List<Object> values = new ArrayList<>();
        for (Value value : filter.getValues()) {
            values.add(getJavaObject(header, index, value));
        }
        DataField field = projectedSchema.getFields().get(index);
        switch (filter.getInOp()) {
            case IN_AND:
            case IN_OR:
                if(values.size() == 1) {
                    return builder.equal(index, values.get(0));
                }
                return new LeafPredicate(In.INSTANCE, field.type(), index, field.name(), values);
            case NOT_IN_AND:
            case NOT_IN_OR:
                if(values.size() == 1) {
                    return builder.notEqual(index, values.get(0));
                }
                return new LeafPredicate(NotIn.INSTANCE, field.type(), index, field.name(), values);
            default:
                throw new UnsupportedOperationException("Unsupported InOp: " + filter.getInOp());
        }
    }

    @Nullable
    private static Predicate toPaimonPredicate(ValueFilter filter, Header header, RowType projectedSchema) {
        PredicateBuilder builder = new PredicateBuilder(projectedSchema);

        int index = getIndex(filter.getPath(), header);
        Object value = getJavaObject(header, index, filter.getValue());
        switch (filter.getOp()) {
            case GE:
            case GE_AND:
                return builder.greaterOrEqual(index, value);
            case G:
            case G_AND:
                return builder.greaterThan(index, value);
            case LE:
            case LE_AND:
                return builder.lessOrEqual(index, value);
            case L:
            case L_AND:
                return builder.lessThan(index, value);
            case E:
            case E_AND:
                return builder.equal(index, value);
            case NE:
            case NE_AND:
                return builder.notEqual(index, value);
            case LIKE:
            case LIKE_AND:
                String pattern = new String(((BinaryString)value).toBytes());
                LikeOptimizer.Classified classified = LikeOptimizer.classify(pattern);
                BinaryString literal = BinaryString.fromBytes(classified.getLiteral().getBytes());
                switch (classified.getKind()) {
                    case EXACT:
                        return builder.equal(index, literal);
                    case STARTS_WITH:
                        return builder.startsWith(index, literal);
                    case ENDS_WITH:
                        return builder.endsWith(index, literal);
                    case CONTAINS:
                        return builder.contains(index, literal);
                    default:
                        throw new UnsupportedOperationException("Unsupported Like pattern: " + classified.getKind());
                }
            case NOT_LIKE:
            case NOT_LIKE_AND:
            default:
                throw new UnsupportedOperationException("Unsupported Op: " + filter.getOp());
        }
    }

    private static Predicate toPaimonPredicate(AndFilter filter, Header header, RowType projectedSchema) {
        List<Predicate> predicates = new ArrayList<>();
        Preconditions.checkArgument(!filter.getChildren().isEmpty(), "AndFilter must have at least one child");
        for (Filter child : filter.getChildren()) {
            Predicate childPredicate = toPaimonPredicate(child, header, projectedSchema);
            predicates.add(childPredicate);
        }
        return PredicateBuilder.and(predicates);
    }

    private static Predicate toPaimonPredicate(OrFilter filter, Header header, RowType projectedSchema) {
        List<Predicate> predicates = new ArrayList<>();
        Preconditions.checkArgument(!filter.getChildren().isEmpty(), "OrFilter must have at least one child");
        for (Filter child : filter.getChildren()) {
            Predicate childPredicate = toPaimonPredicate(child, header, projectedSchema);
            predicates.add(childPredicate);
        }
        return PredicateBuilder.or(predicates);
    }

    private static int getIndex(String pattern, Header header) {
        List<Integer> indices = header.patternIndexOf(pattern);
        if (indices.isEmpty()) {
            throw new UnsupportedOperationException("Path " + pattern + " not found in header");
        }
        if (indices.size() > 1) {
            throw new UnsupportedOperationException("Multiple paths match pattern " + pattern + ": " + indices);
        }
        return indices.get(0) + 1;
    }

    private static Object getJavaObject(Header header, int index, Value value) {
        DataType type = header.getField(index - 1).getType();
        Object result = value.getValue();
        if(result == null) {
            throw new UnsupportedOperationException("Value is null");
        }
        switch (type) {
            case INTEGER:
                if(result instanceof Integer) {
                    return result;
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to int");
            case LONG:
                if(result instanceof Long || result instanceof Integer) {
                    return ((Number) result).longValue();
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to long");
            case FLOAT:
                if(result instanceof Float || result instanceof Integer || result instanceof Long) {
                    return ((Number) result).floatValue();
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to float");
            case DOUBLE:
                if(result instanceof Number) {
                    return ((Number) result).doubleValue();
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to double");
            case BINARY:
                if (result instanceof byte[]) {
                    return BinaryString.fromBytes((byte[]) result);
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to byte[]");
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + type);
        }
    }

    public static Map<String, org.apache.paimon.types.DataType> extractFields(Predicate predicate) {
        Map<String, org.apache.paimon.types.DataType> fields = new HashMap<>();
        predicate.visit(new PredicateVisitor<Void>(){

            @Override
            public Void visit(LeafPredicate predicate) {
                DataField field = new DataField(predicate.index(),predicate.fieldName(), predicate.type());
                if(field.id() != 0) {
                    fields.put(field.name(), field.type());
                }
                return null;
            }

            @Override
            public Void visit(CompoundPredicate predicate) {
                for(Predicate child : predicate.children()) {
                    child.visit(this);
                }
                return null;
            }
        });
        return fields;
    }

    @Nullable
    public static java.util.function.Predicate<InternalRow> optimizeUnsupportedFilter(Filter filter, Header header, InternalRow.FieldGetter[] fieldGetters){
        try{
            switch (filter.getType()) {
                case Value:
                    return optimizeUnsupportedFilter((ValueFilter) filter, header);
                case Path:
                    return optimizeUnsupportedFilter((PathFilter) filter, header, fieldGetters);
                case Key:
                case And:
                case Or:
                case In:
                case Expr:
                case Bool:
                case Not:
                default:
                    return null;
            }
        }catch (UnsupportedOperationException e) {
            return null;
        }
    }

    @Nullable
    private static java.util.function.Predicate<InternalRow> optimizeUnsupportedFilter(ValueFilter filter, Header header) {
        int index = getIndex(filter.getPath(), header);
        Object value = getJavaObject(header, index, filter.getValue());
        switch (filter.getOp()) {
            case LIKE:
            case LIKE_AND:
                return optimizeLike(index, (BinaryString) value);
            case NOT_LIKE:
            case NOT_LIKE_AND:
                return optimizeLike(index, (BinaryString) value).negate();
            default:
                return null;
        }
    }

    private static java.util.function.Predicate<InternalRow> optimizeLike(int index, BinaryString pattern) {
        String patternStr = new String(pattern.toBytes());
        Pattern regexPattern = Pattern.compile(patternStr);
        return row -> {
            BinaryString value = row.getString(index);
            if (value == null) {
                return false;
            }
            String valueStr = new String(value.toBytes());
            return regexPattern.matcher(valueStr).matches();
        };
    }

    private static java.util.function.Predicate<InternalRow> optimizeUnsupportedFilter(PathFilter filter, Header header, InternalRow.FieldGetter[] fieldGetters) {
        int indexA = getIndex(filter.getPathA(), header);
        int indexB = getIndex(filter.getPathB(), header);

        Function<InternalRow,Integer> cmp = (internalRow) -> {
            Comparable valueA = (Comparable) fieldGetters[indexA].getFieldOrNull(internalRow);
            Comparable valueB = (Comparable) fieldGetters[indexB].getFieldOrNull(internalRow);
            if (valueA == null || valueB == null) {
                return null;
            }
            return valueA.compareTo(valueB);
        };


        switch (filter.getOp()) {
            case GE:
            case GE_AND:
                return row -> {
                    Integer cmpResult = cmp.apply(row);
                    return cmpResult != null && cmpResult >= 0;
                };
            case G:
            case G_AND:
                return row -> {
                    Integer cmpResult = cmp.apply(row);
                    return cmpResult != null && cmpResult > 0;
                };
            case LE:
            case LE_AND:
                return row -> {
                    Integer cmpResult = cmp.apply(row);
                    return cmpResult != null && cmpResult <= 0;
                };
            case L:
            case L_AND:
                return row -> {
                    Integer cmpResult = cmp.apply(row);
                    return cmpResult != null && cmpResult < 0;
                };
            case E:
            case E_AND:
                return row -> {
                    Integer cmpResult = cmp.apply(row);
                    return cmpResult != null && cmpResult == 0;
                };
            case NE:
            case NE_AND:
                return row -> {
                    Integer cmpResult = cmp.apply(row);
                    return cmpResult != null && cmpResult != 0;
                };
            case LIKE:
            case LIKE_AND:
            case NOT_LIKE:
            case NOT_LIKE_AND:
            default:
                throw new UnsupportedOperationException("Unsupported filter operator: " + filter.getOp());
        }
    }
}

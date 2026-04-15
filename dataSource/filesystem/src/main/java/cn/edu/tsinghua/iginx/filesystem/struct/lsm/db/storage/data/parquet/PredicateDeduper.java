package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import com.google.common.collect.*;
import org.apache.arrow.util.Preconditions;
import org.apache.paimon.predicate.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;


public class PredicateDeduper {

    @Nullable
    public static  Predicate simplify(@Nullable Predicate predicate) {
        if (predicate == null) return null;

        // 1. 获取所有的 And 节点
        List<Predicate> andParts = PredicateBuilder.splitAnd(predicate);
        if (andParts.size() > 1) {
            // AND 语境：丢弃更弱的（被别人包含的）
            List<Predicate> simplified = andParts.stream().map(PredicateDeduper::simplify).filter(Objects::nonNull).distinct().collect(Collectors.toList());
            Preconditions.checkState(!simplified.isEmpty(), "After simplification, AND parts should not be empty");
            List<Predicate> deduped = dedup(simplified, false);
//            List<Predicate> optimized = optimizeRanges(deduped, false);
//            if(optimized.isEmpty()) {
//                return null;
//            }
            return PredicateBuilder.and(deduped);
        }

        // 2. 获取所有的 Or 节点
        List<Predicate> orParts = PredicateBuilder.splitOr(predicate);
        if (orParts.size() > 1) {
            // OR 语境：丢弃更强的（包含别人的）
            List<Predicate> simplified = orParts.stream().map(PredicateDeduper::simplify).filter(Objects::nonNull).distinct().collect(Collectors.toList());
            Preconditions.checkState(!simplified.isEmpty(), "After simplification, OR parts should not be empty");
            List<Predicate> deduped = dedup(simplified, true);
//            List<Predicate> optimized = optimizeRanges(deduped, true);
//            if(optimized.isEmpty()) {
//                return null;
//            }
            return PredicateBuilder.or(deduped);
        }

        // 3. 既不是 And 也不是 Or，说明是 Leaf
        return predicate;
    }

    private static List<Predicate> dedup(List<Predicate> children, boolean isOr) {
        Preconditions.checkArgument(!children.isEmpty(), "Children list should not be empty for deduplication");
        // 先递归简化子项，确保内部是干净的
        List<Predicate> result = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            Predicate candidate = children.get(i);
            boolean isRedundant = false;
            for (int j = 0; j < children.size(); j++) {
                if (i == j) continue;
                Predicate other = children.get(j);
                if(isOr) {
                    // OR 语境下，如果 candidate 强于 other，则 candidate 是冗余的
                    if(isStronger(candidate, other)){
                        isRedundant = true;
                        break;
                    }
                } else {
                    // AND 语境下，如果 other 强于 candidate，则 candidate 是冗余的
                    if(isStronger(other, candidate)){
                        isRedundant = true;
                        break;
                    }
                }
            }
            if (!isRedundant) result.add(candidate);
        }
        return result;
    }

    private static boolean isStronger(Predicate p1, Predicate p2) {
        if (p1.equals(p2)) return true;

        // 1. A => (A || B)
        List<Predicate> p2Ors = PredicateBuilder.splitOr(p2);
        if (p2Ors.size() > 1) {
            return p2Ors.stream().anyMatch(child -> isStronger(p1, child));
        }

        // 2. (A && B) => A
        List<Predicate> p1Ands = PredicateBuilder.splitAnd(p1);
        if (p1Ands.size() > 1) {
            return p1Ands.stream().anyMatch(child -> isStronger(child, p2));
        }

        // 3. 数值蕴含
        if (p1 instanceof LeafPredicate && p2 instanceof LeafPredicate) {
            return isLeafStronger((LeafPredicate) p1, (LeafPredicate) p2);
        }

        return false;
    }

    private static boolean isLeafStronger(LeafPredicate l1, LeafPredicate l2) {
        if (l1.index() != l2.index()) return false;

        LeafFunction f1 = l1.function();
        LeafFunction f2 = l2.function();

        Comparable v1 = (Comparable) l1.literals().get(0);
        Comparable v2 = (Comparable) l2.literals().get(0);

        // 处理 Greater 系列
        boolean isG1 = f1 instanceof GreaterThan || f1 instanceof GreaterOrEqual;
        boolean isG2 = f2 instanceof GreaterThan || f2 instanceof GreaterOrEqual;
        if (isG1 && isG2) {
            int cmp = v1.compareTo(v2);
            if (cmp > 0) return true; // q > 10 强于 q > 7
            if (cmp == 0) {
                // 值相等时，不带等号的更强：(q > 10) => (q >= 10)
                return (f1 instanceof GreaterThan) || (f2 instanceof GreaterOrEqual);
            }
            return false;
        }

        // 处理 Less 系列
        boolean isL1 = f1 instanceof LessThan || f1 instanceof LessOrEqual;
        boolean isL2 = f2 instanceof LessThan || f2 instanceof LessOrEqual;
        if (isL1 && isL2) {
            int cmp = v1.compareTo(v2);
            if (cmp < 0) return true; // q < 7 强于 q < 10
            if (cmp == 0) {
                return (f1 instanceof LessThan) || (f2 instanceof LessOrEqual);
            }
            return false;
        }

        return Objects.equals(f1, f2) && Objects.equals(l1.literals(), l2.literals());
    }

    private static List<Predicate> optimizeRanges(List<Predicate> parts, boolean isOr) {
        Map<Integer, List<LeafPredicate>> groups = parts.stream()
                .filter(p -> p instanceof LeafPredicate)
                .map(p -> (LeafPredicate) p)
                .collect(Collectors.groupingBy(LeafPredicate::index));

        if (groups.isEmpty()) return parts;

        List<Predicate> result = new ArrayList<>();
        for(Predicate p : parts) {
            if (p instanceof LeafPredicate) continue;
            result.add(p);
        }

        for (Map.Entry<Integer, List<LeafPredicate>> entry : groups.entrySet()) {
            RangeSet<Comparable<?>>  rangeSet = TreeRangeSet.create();
            if (isOr) {
                for (LeafPredicate lp : entry.getValue()) {
                    RangeSet<Comparable<?>> rs = toRange(lp);
                    rangeSet.addAll(rs);
                }
                if(rangeSet.encloses(Range.all())){
                    return Collections.emptyList(); // 恒为真
                }

            } else {
                rangeSet.add(Range.all());
                for (LeafPredicate lp : entry.getValue()) {
                    RangeSet<Comparable<?>> rs = toRange(lp);
                    for(Range<Comparable<?>> r : rs.complement().asRanges()) {
                        rangeSet.remove(r);
                    }
                }
                if (rangeSet.isEmpty()) {
                    return Collections.emptyList(); // 恒为假
                }
            }

            // 将合并后的 Range 转回 Predicate (此处简化处理，实际需根据 rangeSet.asRanges() 构造)
            result.addAll(fromRangeSet(entry.getValue().get(0).fieldRef(), rangeSet));

        }
        return result;
    }

    private static RangeSet<Comparable<?>> toRange(LeafPredicate lp) {
        if(lp.function() instanceof In) {
            TreeRangeSet<Comparable<?>> set = TreeRangeSet.create();
            for(Object literal : lp.literals()) {
                set.add(Range.singleton((Comparable<?>) literal));
            }
            return set;
        }
        if(lp.function() instanceof NotIn) {
            TreeRangeSet<Comparable<?>> set = TreeRangeSet.create();
            set.add(Range.all());
            for(Object literal : lp.literals()) {
                set.remove(Range.singleton((Comparable<?>) literal));
            }
            return set;
        }
        Comparable<?> val = (Comparable<?>) lp.literals().get(0);
        if (lp.function() instanceof GreaterOrEqual) return ImmutableRangeSet.of(Range.atLeast(val));
        if (lp.function() instanceof GreaterThan) return ImmutableRangeSet.of(Range.greaterThan(val));
        if (lp.function() instanceof LessOrEqual) return ImmutableRangeSet.of(Range.atMost(val));
        if (lp.function() instanceof LessThan) return ImmutableRangeSet.of(Range.lessThan(val));
        if (lp.function() instanceof Equal) return ImmutableRangeSet.of(Range.singleton(val));
        if (lp.function() instanceof NotEqual) {
            RangeSet<Comparable<?>> set = ImmutableRangeSet.of(Range.singleton(val));
            return set.complement();
        }
        throw new UnsupportedOperationException("Unsupported function for range conversion: " + lp.function());
    }

    private static List<Predicate> fromRangeSet(FieldRef origin, RangeSet<Comparable<?>> rangeSet) {
        Preconditions.checkState(!rangeSet.isEmpty(), "RangeSet should not be empty here");
        Preconditions.checkState(!rangeSet.encloses(Range.all()), "RangeSet should not enclose all here");

        List<Predicate> result = new ArrayList<>();

        List<Object> equals = new ArrayList<>();
        for(Range<Comparable<?>> range : rangeSet.asRanges()) {
            if(range.hasLowerBound() && range.hasUpperBound() && range.lowerEndpoint().equals(range.upperEndpoint()) && range.lowerBoundType() == BoundType.CLOSED && range.upperBoundType() == BoundType.CLOSED) {
                equals.add(range.lowerEndpoint());
                continue;
            }
            if(range.hasLowerBound()){
                if(range.lowerBoundType() == BoundType.CLOSED) {
                    result.add(new LeafPredicate(GreaterOrEqual.INSTANCE, origin.type(), origin.index(), origin.name(), Collections.singletonList(range.lowerEndpoint())));
                }else{
                    result.add(new LeafPredicate(GreaterThan.INSTANCE, origin.type(), origin.index(), origin.name(), Collections.singletonList(range.lowerEndpoint())));
                }
            }
            if(range.hasUpperBound()){
                if(range.upperBoundType() == BoundType.CLOSED) {
                    result.add(new LeafPredicate(LessOrEqual.INSTANCE, origin.type(), origin.index(), origin.name(), Collections.singletonList(range.upperEndpoint())));
                }else{
                    result.add(new LeafPredicate(LessThan.INSTANCE, origin.type(), origin.index(), origin.name(), Collections.singletonList(range.upperEndpoint())));
                }
            }
        }

        if(equals.size() == 1) {
            result.add(new LeafPredicate(Equal.INSTANCE, origin.type(), origin.index(), origin.name(), equals));
        } else if(equals.size() > 1) {
            result.add(new LeafPredicate(In.INSTANCE, origin.type(), origin.index(), origin.name(), equals));
        }

        return result;
    }
}
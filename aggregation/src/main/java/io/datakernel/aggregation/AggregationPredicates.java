package io.datakernel.aggregation;

import com.google.common.base.Joiner;
import io.datakernel.aggregation.AggregationPredicate.FieldAccessor;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.PredicateDef;

import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.*;

public class AggregationPredicates {
	private AggregationPredicates() {
	}

	private static class PredicateSimplifierKey<L extends AggregationPredicate, R extends AggregationPredicate> {
		private final Class<L> leftType;
		private final Class<R> rightType;

		private PredicateSimplifierKey(Class<L> leftType, Class<R> rightType) {
			this.leftType = leftType;
			this.rightType = rightType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateSimplifierKey that = (PredicateSimplifierKey) o;

			if (!leftType.equals(that.leftType)) return false;
			return rightType.equals(that.rightType);

		}

		@Override
		public int hashCode() {
			int result = leftType.hashCode();
			result = 31 * result + rightType.hashCode();
			return result;
		}
	}

	private interface PredicateSimplifier<L extends AggregationPredicate, R extends AggregationPredicate> {
		AggregationPredicate simplifyAnd(L left, R right);
	}

	private final static Map<PredicateSimplifierKey<?, ?>, PredicateSimplifier<?, ?>> simplifiers = new HashMap<>();

	private static <L extends AggregationPredicate, R extends AggregationPredicate> void register(Class<L> leftType, Class<R> rightType, final PredicateSimplifier<L, R> operation) {
		PredicateSimplifierKey keyLeftRight = new PredicateSimplifierKey<>(leftType, rightType);
		checkState(!simplifiers.containsKey(keyLeftRight));
		simplifiers.put(keyLeftRight, operation);
		if (!rightType.equals(leftType)) {
			PredicateSimplifierKey keyRightLeft = new PredicateSimplifierKey<>(rightType, leftType);
			checkState(!simplifiers.containsKey(keyRightLeft));
			simplifiers.put(keyRightLeft, new PredicateSimplifier<R, L>() {
				@Override
				public AggregationPredicate simplifyAnd(R right, L left) {
					return operation.simplifyAnd(left, right);
				}
			});
		}
	}

	static {
		PredicateSimplifier simplifierAlwaysFalse = new PredicateSimplifier<PredicateAlwaysFalse, AggregationPredicate>() {
			@Override
			public AggregationPredicate simplifyAnd(PredicateAlwaysFalse left, AggregationPredicate right) {
				return alwaysFalse();
			}
		};
		register(PredicateAlwaysFalse.class, PredicateAlwaysFalse.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateAlwaysTrue.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateNot.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateEq.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateBetween.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateRegexp.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateAnd.class, simplifierAlwaysFalse);
		register(PredicateAlwaysFalse.class, PredicateOr.class, simplifierAlwaysFalse);

		PredicateSimplifier simplifierAlwaysTrue = new PredicateSimplifier<PredicateAlwaysTrue, AggregationPredicate>() {
			@Override
			public AggregationPredicate simplifyAnd(PredicateAlwaysTrue left, AggregationPredicate right) {
				return right;
			}
		};
		register(PredicateAlwaysTrue.class, PredicateAlwaysTrue.class, simplifierAlwaysTrue);
		register(PredicateAlwaysTrue.class, PredicateNot.class, simplifierAlwaysTrue);
		register(PredicateAlwaysTrue.class, PredicateEq.class, simplifierAlwaysTrue);
		register(PredicateAlwaysTrue.class, PredicateBetween.class, simplifierAlwaysTrue);
		register(PredicateAlwaysTrue.class, PredicateRegexp.class, simplifierAlwaysTrue);
		register(PredicateAlwaysTrue.class, PredicateAnd.class, simplifierAlwaysTrue);
		register(PredicateAlwaysTrue.class, PredicateOr.class, simplifierAlwaysTrue);

		PredicateSimplifier simplifierNot = new PredicateSimplifier<PredicateNot, AggregationPredicate>() {
			@Override
			public AggregationPredicate simplifyAnd(PredicateNot left, AggregationPredicate right) {
				if (left.predicate.equals(right))
					return alwaysFalse();
				return null;
			}
		};
		register(PredicateNot.class, PredicateNot.class, simplifierNot);
		register(PredicateNot.class, PredicateEq.class, simplifierNot);
		register(PredicateNot.class, PredicateBetween.class, simplifierNot);
		register(PredicateNot.class, PredicateRegexp.class, simplifierNot);
		register(PredicateNot.class, PredicateAnd.class, simplifierNot);
		register(PredicateNot.class, PredicateOr.class, simplifierNot);

		register(PredicateEq.class, PredicateEq.class, new PredicateSimplifier<PredicateEq, PredicateEq>() {
			@Override
			public AggregationPredicate simplifyAnd(PredicateEq left, PredicateEq right) {
				if (!left.key.equals(right.key))
					return null;
				return alwaysFalse();
			}
		});
		register(PredicateEq.class, PredicateBetween.class, new PredicateSimplifier<PredicateEq, PredicateBetween>() {
			@Override
			public AggregationPredicate simplifyAnd(PredicateEq left, PredicateBetween right) {
				if (!left.key.equals(right.key))
					return null;
				if (right.from.compareTo(left.value) <= 0 && right.to.compareTo(left.value) >= 0)
					return right;
				return alwaysFalse();
			}
		});
		register(PredicateBetween.class, PredicateBetween.class, new PredicateSimplifier<PredicateBetween, PredicateBetween>() {
			@Override
			public AggregationPredicate simplifyAnd(PredicateBetween left, PredicateBetween right) {
				if (!left.key.equals(right.key))
					return null;
				Comparable from = left.from.compareTo(right.from) >= 0 ? left.from : right.from;
				Comparable to = left.to.compareTo(right.to) <= 0 ? left.to : right.to;
				return between(left.key, from, to).simplify();
			}
		});
	}

	@SuppressWarnings("unchecked")
	private static AggregationPredicate simplifyAnd(AggregationPredicate left, AggregationPredicate right) {
		if (left.equals(right))
			return left;
		PredicateSimplifierKey key = new PredicateSimplifierKey(left.getClass(), right.getClass());
		PredicateSimplifier<AggregationPredicate, AggregationPredicate> simplifier = (PredicateSimplifier<AggregationPredicate, AggregationPredicate>) simplifiers.get(key);
		if (simplifier == null)
			return null;
		return simplifier.simplifyAnd(left, right);
	}

	public static final class PredicateAlwaysFalse implements AggregationPredicate {
		private static final PredicateAlwaysFalse instance = new PredicateAlwaysFalse();

		private PredicateAlwaysFalse() {
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return emptySet();
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return emptyMap();
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			return Expressions.alwaysFalse();
		}

		@Override
		public String toString() {
			return "FALSE";
		}
	}

	public static final class PredicateAlwaysTrue implements AggregationPredicate {
		private static final PredicateAlwaysTrue instance = new PredicateAlwaysTrue();

		private PredicateAlwaysTrue() {
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return emptySet();
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return emptyMap();
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			return Expressions.alwaysTrue();
		}

		@Override
		public String toString() {
			return "TRUE";
		}
	}

	public static final class PredicateNot implements AggregationPredicate {
		private final AggregationPredicate predicate;

		private PredicateNot(AggregationPredicate predicate) {
			this.predicate = predicate;
		}

		public AggregationPredicate getPredicate() {
			return predicate;
		}

		@Override
		public AggregationPredicate simplify() {
			if (predicate instanceof PredicateNot)
				return ((PredicateNot) predicate).predicate.simplify();
			return not(predicate.simplify());
		}

		@Override
		public Set<String> getDimensions() {
			return predicate.getDimensions();
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return emptyMap();
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			return Expressions.not(predicate.createPredicateDef(record, fields));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateNot that = (PredicateNot) o;

			return predicate.equals(that.predicate);

		}

		@Override
		public int hashCode() {
			return predicate.hashCode();
		}

		@Override
		public String toString() {
			return "NOT " + predicate;
		}
	}

	public static final class PredicateEq implements AggregationPredicate {
		final String key;
		final Object value;

		private PredicateEq(String key, Object value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return singleton(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return singletonMap(key, value);
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			return cmpEq(field(record, key.replace('.', '$')), value(fields.toInternalValue(key, value)));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateEq that = (PredicateEq) o;

			if (!key.equals(that.key)) return false;
			return value.equals(that.value);

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + value.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return key + '=' + value;
		}
	}

	public static final class PredicateRegexp implements AggregationPredicate {
		final String key;
		final String regexp;

		private PredicateRegexp(String key, String regexp) {
			this.key = key;
			this.regexp = regexp;
		}

		public String getKey() {
			return key;
		}

		public String getRegexp() {
			return regexp;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return singleton(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return emptyMap();
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			Pattern pattern = Pattern.compile(regexp);
			return cmpNe(value(false),
					call(call(value(pattern), "matcher", cast(field(record, key.replace('.', '$')), CharSequence.class)), "matches"));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateRegexp that = (PredicateRegexp) o;

			if (!key.equals(that.key)) return false;
			return regexp.equals(that.regexp);

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + regexp.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return key + " " + regexp;
		}
	}

	public static final class PredicateBetween implements AggregationPredicate {
		final String key;
		final Comparable from;
		final Comparable to;

		PredicateBetween(String key, Comparable from, Comparable to) {
			this.key = key;
			this.from = from;
			this.to = to;
		}

		public String getKey() {
			return key;
		}

		public Comparable getFrom() {
			return from;
		}

		public Comparable getTo() {
			return to;
		}

		@Override
		public AggregationPredicate simplify() {
			return (from.compareTo(to) > 0) ? alwaysFalse() : (from.equals(to) ? AggregationPredicates.eq(key, from) : this);
		}

		@Override
		public Set<String> getDimensions() {
			return singleton(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return emptyMap();
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			return Expressions.and(
					cmpGe(field(record, key.replace('.', '$')), value(fields.toInternalValue(key, from))),
					cmpLe(field(record, key.replace('.', '$')), value(fields.toInternalValue(key, to))));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateBetween that = (PredicateBetween) o;

			if (!key.equals(that.key)) return false;
			if (!from.equals(that.from)) return false;
			return to.equals(that.to);

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + from.hashCode();
			result = 31 * result + to.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "" + key + " BETWEEN " + from + " AND " + to;
		}
	}

	public static final class PredicateAnd implements AggregationPredicate {
		static final Joiner JOINER = Joiner.on(" AND ");
		final List<AggregationPredicate> predicates;

		private PredicateAnd(List<AggregationPredicate> predicates) {
			this.predicates = predicates;
		}

		public List<AggregationPredicate> getPredicates() {
			return predicates;
		}

		@Override
		public AggregationPredicate simplify() {
			Set<AggregationPredicate> simplifiedPredicates = newLinkedHashSet();
			for (AggregationPredicate predicate : predicates) {
				AggregationPredicate simplified = predicate.simplify();
				if (simplified instanceof PredicateAnd) {
					simplifiedPredicates.addAll(((PredicateAnd) simplified).predicates);
				} else {
					simplifiedPredicates.add(simplified);
				}
			}
			boolean simplified;
			do {
				simplified = false;
				HashSet<AggregationPredicate> newPredicates = new HashSet<>();
				L:
				for (AggregationPredicate newPredicate : simplifiedPredicates) {
					for (AggregationPredicate simplifiedPredicate : newPredicates) {
						AggregationPredicate maybeSimplified = simplifyAnd(newPredicate, simplifiedPredicate);
						if (maybeSimplified != null) {
							newPredicates.remove(simplifiedPredicate);
							newPredicates.add(maybeSimplified);
							simplified = true;
							continue L;
						}
					}
					newPredicates.add(newPredicate);
				}
				simplifiedPredicates = newPredicates;
			} while (simplified);

			return simplifiedPredicates.isEmpty() ? alwaysTrue() : simplifiedPredicates.size() == 1 ? simplifiedPredicates.iterator().next() : and(newArrayList(simplifiedPredicates));
		}

		@Override
		public Set<String> getDimensions() {
			Set<String> result = new HashSet<>();
			for (AggregationPredicate predicate : predicates) {
				result.addAll(predicate.getDimensions());
			}
			return result;
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			Map<String, Object> result = new HashMap<>();
			for (AggregationPredicate predicate : predicates) {
				result.putAll(predicate.getFullySpecifiedDimensions());
			}
			return result;
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			List<PredicateDef> predicateDefs = new ArrayList<>();
			for (AggregationPredicate predicate : predicates) {
				predicateDefs.add(predicate.createPredicateDef(record, fields));
			}
			return Expressions.and(predicateDefs);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateAnd that = (PredicateAnd) o;

			return new HashSet<>(predicates).equals(new HashSet<>(that.predicates));

		}

		@Override
		public int hashCode() {
			return new HashSet<>(predicates).hashCode();
		}

		@Override
		public String toString() {
			return "(" + JOINER.join(predicates) + ")";
		}
	}

	public static final class PredicateOr implements AggregationPredicate {
		static final Joiner JOINER = Joiner.on(" OR ");
		final List<AggregationPredicate> predicates;

		PredicateOr(List<AggregationPredicate> predicates) {
			this.predicates = predicates;
		}

		public List<AggregationPredicate> getPredicates() {
			return predicates;
		}

		@Override
		public AggregationPredicate simplify() {
			Set<AggregationPredicate> simplifiedPredicates = newLinkedHashSet();
			for (AggregationPredicate predicate : predicates) {
				AggregationPredicate simplified = predicate.simplify();
				if (simplified instanceof PredicateOr) {
					simplifiedPredicates.addAll(((PredicateOr) simplified).predicates);
				} else {
					simplifiedPredicates.add(simplified);
				}
			}
			return simplifiedPredicates.isEmpty() ? alwaysTrue() : simplifiedPredicates.size() == 1 ? simplifiedPredicates.iterator().next() : or(newArrayList(simplifiedPredicates));
		}

		@Override
		public Set<String> getDimensions() {
			Set<String> result = new HashSet<>();
			for (AggregationPredicate predicate : predicates) {
				result.addAll(predicate.getDimensions());
			}
			return result;
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return emptyMap();
		}

		@Override
		public PredicateDef createPredicateDef(Expression record, FieldAccessor fields) {
			List<PredicateDef> predicateDefs = new ArrayList<>();
			for (AggregationPredicate predicate : predicates) {
				predicateDefs.add(predicate.createPredicateDef(record, fields));
			}
			return Expressions.or(predicateDefs);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateOr that = (PredicateOr) o;

			return new HashSet<>(predicates).equals(new HashSet<>(that.predicates));

		}

		@Override
		public int hashCode() {
			return new HashSet<>(predicates).hashCode();
		}

		@Override
		public String toString() {
			return "(" + JOINER.join(predicates) + ")";
		}
	}

	public static AggregationPredicate alwaysTrue() {
		return PredicateAlwaysTrue.instance;
	}

	public static AggregationPredicate alwaysFalse() {
		return PredicateAlwaysFalse.instance;
	}

	public static AggregationPredicate not(AggregationPredicate predicate) {
		return new PredicateNot(predicate);
	}

	public static AggregationPredicate and(List<AggregationPredicate> predicates) {
		return new PredicateAnd(predicates);
	}

	public static AggregationPredicate and(AggregationPredicate... predicates) {
		return and(Arrays.asList(predicates));
	}

	public static AggregationPredicate or(List<AggregationPredicate> predicates) {
		return new PredicateOr(predicates);
	}

	public static AggregationPredicate or(AggregationPredicate... predicates) {
		return or(Arrays.asList(predicates));
	}

	public static AggregationPredicate eq(String key, Object value) {
		return new PredicateEq(key, value);
	}

	public static AggregationPredicate regexp(String key, String pattern) {
		return new PredicateRegexp(key, pattern);
	}

	public static AggregationPredicate between(String key, Comparable from, Comparable to) {
		return new PredicateBetween(key, from, to);
	}

	public static final class RangeScan {
		private final PrimaryKey from;
		private final PrimaryKey to;

		private RangeScan(PrimaryKey from, PrimaryKey to) {
			this.from = from;
			this.to = to;
		}

		public static RangeScan noScan() {
			return new RangeScan(null, null);
		}

		public static RangeScan fullScan() {
			return new RangeScan(PrimaryKey.ofArray(), PrimaryKey.ofArray());
		}

		public static RangeScan rangeScan(PrimaryKey from, PrimaryKey to) {
			return new RangeScan(from, to);
		}

		public boolean isNoScan() {
			return from == null;
		}

		public boolean isFullScan() {
			return from.size() == 0;
		}

		public boolean isRangeScan() {
			return !isNoScan() && !isFullScan();
		}

		public PrimaryKey getFrom() {
			checkState(!isNoScan());
			return from;
		}

		public PrimaryKey getTo() {
			checkState(!isNoScan());
			return to;
		}
	}

	public static RangeScan toRangeScan(AggregationPredicate predicate, List<String> primaryKey, FieldAccessor converters) {
		predicate = predicate.simplify();
		if (predicate == alwaysFalse())
			return RangeScan.noScan();
		List<AggregationPredicate> conjunctions = new ArrayList<>();
		if (predicate instanceof PredicateAnd) {
			conjunctions.addAll(((PredicateAnd) predicate).predicates);
		} else {
			conjunctions.add(predicate);
		}

		List<Object> from = new ArrayList<>();
		List<Object> to = new ArrayList<>();

		L:
		for (String key : primaryKey) {
			for (int j = 0; j < conjunctions.size(); j++) {
				AggregationPredicate conjunction = conjunctions.get(j);
				if (conjunction instanceof PredicateEq && ((PredicateEq) conjunction).key.equals(key)) {
					conjunctions.remove(j);
					PredicateEq eq = (PredicateEq) conjunction;
					from.add(converters.toInternalValue(eq.key, eq.value));
					to.add(converters.toInternalValue(eq.key, eq.value));
					continue L;
				}
				if (conjunction instanceof PredicateBetween && ((PredicateBetween) conjunction).key.equals(key)) {
					conjunctions.remove(j);
					PredicateBetween between = (PredicateBetween) conjunction;
					from.add(converters.toInternalValue(between.key, between.from));
					to.add(converters.toInternalValue(between.key, between.to));
					break L;
				}
			}
			break;
		}

		return RangeScan.rangeScan(PrimaryKey.ofList(from), PrimaryKey.ofList(to));
	}

}

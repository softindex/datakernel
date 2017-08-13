package io.datakernel.ot;

import io.datakernel.annotation.Nullable;

import java.util.*;

public final class OTSystemImpl<OP> implements OTSystem<OP> {
	public interface TransformFunction<OP, L extends OP, R extends OP> {
		DiffPair<? extends OP> transform(L left, R right);
	}

	public interface SquashFunction<OP, OP1 extends OP, OP2 extends OP> {
		@Nullable
		OP trySquash(OP1 op1, OP2 op2);
	}

	public interface InvertFunction<OP> {
		List<? extends OP> invert(OP op);
	}

	public interface EmptyPredicate<OP> {
		@Nullable
		boolean isEmpty(OP op);
	}

	private static class KeyPair<O> {
		public final Class<? extends O> left;
		public final Class<? extends O> right;

		public KeyPair(Class<? extends O> left, Class<? extends O> right) {
			this.left = left;
			this.right = right;
		}

		@SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "RedundantIfStatement"})
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			KeyPair<?> key = (KeyPair<?>) o;
			if (!left.equals(key.left)) return false;
			if (!right.equals(key.right)) return false;
			return true;
		}

		@Override
		public int hashCode() {
			int result = left.hashCode();
			result = 31 * result + right.hashCode();
			return result;
		}
	}

	private final Map<KeyPair<OP>, TransformFunction<OP, ?, ?>> transformers = new HashMap<>();
	private final Map<KeyPair<OP>, SquashFunction<OP, ?, ?>> squashers = new HashMap<>();
	private final Map<Class<OP>, InvertFunction<OP>> inverters = new HashMap<>();
	private final Map<Class<OP>, EmptyPredicate<OP>> emptyPredicates = new HashMap<>();

	private OTSystemImpl() {
	}

	public static <O> OTSystemImpl<O> create() {
		return new OTSystemImpl<>();
	}

	@SuppressWarnings("unchecked")
	public <L extends OP, R extends OP> OTSystemImpl<OP> withTransformFunction(Class<? super L> leftType, Class<? super R> rightType,
	                                                                           final TransformFunction<OP, L, R> transformer) {
		this.transformers.put(new KeyPair(leftType, rightType), transformer);
		if (leftType != rightType) {
			this.transformers.put(new KeyPair(rightType, leftType), new TransformFunction<OP, R, L>() {
				@Override
				public DiffPair<? extends OP> transform(R left, L right) {
					DiffPair<? extends OP> transformed = transformer.transform(right, left);
					return DiffPair.of(transformed.right, transformed.left);
				}
			});
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public <O1 extends OP, O2 extends OP> OTSystemImpl<OP> withSquashFunction(Class<? super O1> opType1, Class<? super O2> opType2,
	                                                                          SquashFunction<OP, O1, O2> squashFunction) {
		this.squashers.put(new KeyPair(opType1, opType2), squashFunction);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <O extends OP> OTSystemImpl<OP> withInvertFunction(Class<? super O> opType, InvertFunction<O> inverter) {
		this.inverters.put((Class<OP>) opType, (InvertFunction<OP>) inverter);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <O extends OP> OTSystemImpl<OP> withEmptyPredicate(Class<? super O> opType, EmptyPredicate<O> emptyChecker) {
		this.emptyPredicates.put((Class<OP>) opType, (EmptyPredicate<OP>) emptyChecker);
		return this;
	}

	@SuppressWarnings({"unchecked", "SuspiciousMethodCalls", "SimplifiableIfStatement"})
	@Override
	public boolean isEmpty(OP op) {
		if (emptyPredicates.isEmpty())
			return false;
		EmptyPredicate<OP> emptyChecker = emptyPredicates.get(op.getClass());
		if (emptyChecker == null)
			return false;
		return emptyChecker.isEmpty(op);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DiffPair<OP> transform(DiffPair<? extends OP> arg) {
		if (arg.left.isEmpty() && arg.right.isEmpty())
			return DiffPair.empty();
		if (arg.left.isEmpty()) {
			return DiffPair.left(arg.right);
		}
		if (arg.right.isEmpty()) {
			return DiffPair.right(arg.left);
		}
		if (arg.left.size() == 1) {
			OP left = arg.left.get(0);
			OP right = arg.right.get(0);
			KeyPair key = new KeyPair(left.getClass(), right.getClass());
			TransformFunction transformer = transformers.get(key);
			DiffPair transformed1 = transformer.transform(left, right);
			DiffPair transformed2 = this.transform(DiffPair.of(transformed1.right, arg.right.subList(1, arg.right.size())));
			ArrayList<Object> transformedLeft = new ArrayList<>(transformed1.left.size() + transformed2.left.size());
			transformedLeft.addAll(transformed1.left);
			transformedLeft.addAll(transformed2.left);
			return DiffPair.of(transformedLeft, transformed2.right);
		}

		DiffPair<OP> transform1 = this.transform(DiffPair.of(arg.left.subList(0, 1), arg.right));
		DiffPair<OP> transform2 = this.transform(DiffPair.of(arg.left.subList(1, arg.left.size()), transform1.left));
		ArrayList<OP> transformedRight = new ArrayList<>(transform1.right.size() + transform2.right.size());
		transformedRight.addAll(transform1.right);
		transformedRight.addAll(transform2.right);
		return DiffPair.of(transform2.left, transformedRight);
	}

	@SuppressWarnings({"MismatchedReadAndWriteOfArray", "unchecked"})
	@Override
	public List<OP>[] transform(List<? extends OP>[] inputs) {
		List<OP>[] result = new List[inputs.length];
		for (int i = 0; i < inputs.length; i++) {
			result[i] = new ArrayList<>();
		}
		if (inputs.length <= 1)
			return result;

		List<OP> left = (List<OP>) inputs[0];
		List<OP>[] tail = new List[inputs.length - 1];
		for (int i = 0; i < tail.length; i++) {
			tail[i] = (List<OP>) inputs[i + 1];
		}
		List<OP>[] transformedTail = this.transform(tail);
		List<OP> right = new ArrayList<>(inputs[1].size() + transformedTail[0].size());
		right.addAll(inputs[1]);
		right.addAll(transformedTail[0]);
		DiffPair<OP> transformed = this.transform(DiffPair.of(left, right));
		result[0].addAll(transformed.left);
		for (int i = 0; i < transformedTail.length; i++) {
			result[i + 1].addAll(transformedTail[i]);
			result[i + 1].addAll(transformed.right);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<OP> squash(List<? extends OP> ops) {
		if (squashers.isEmpty())
			return (List) ops;
		List<OP> result = new ArrayList<>();
		Iterator<OP> it = ((List) ops).iterator();
		if (!it.hasNext())
			return Collections.emptyList();
		OP cur = it.next();
		while (it.hasNext()) {
			OP next = it.next();
			KeyPair key = new KeyPair(cur.getClass(), next.getClass());
			SquashFunction squashFunction = squashers.get(key);
			OP squashed = squashFunction == null ? null : (OP) squashFunction.trySquash(cur, next);
			if (squashed != null) {
				cur = squashed;
			} else {
				if (!isEmpty(cur)) {
					result.add(cur);
				}
				cur = next;
			}
		}
		if (!isEmpty(cur)) {
			result.add(cur);
		}
		return result;
	}

	@SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
	@Override
	public List<OP> invert(List<? extends OP> ops) {
		int size = ops.size();
		List<OP> result = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			OP op = ops.get(size - i - 1);
			InvertFunction<OP> inverter = inverters.get(op.getClass());
			List<? extends OP> inverted = inverter.invert(op);
			result.addAll(inverted);
		}
		return result;
	}
}

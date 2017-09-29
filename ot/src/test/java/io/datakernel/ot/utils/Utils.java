package io.datakernel.ot.utils;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import static io.datakernel.ot.TransformResult.*;
import static io.datakernel.util.Preconditions.check;
import static java.util.Arrays.asList;

public class Utils {
	private Utils() {

	}

	public static TestAdd add(int delta) {
		return new TestAdd(delta);
	}

	public static TestSet set(int prev, int next) {
		return new TestSet(prev, next);
	}

	@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
	public static OTSystem<TestOp> createTestOp() {
		return OTSystemImpl.<TestOp>create()
				.withTransformFunction(TestAdd.class, TestAdd.class, (left, right) -> of(add(right.getDelta()), add(left.getDelta())))
				.withTransformFunction(TestAdd.class, TestSet.class, (left, right) -> left(set(right.getPrev() + left.getDelta(), right.getNext())))
				.withTransformFunction(TestSet.class, TestSet.class, (left, right) -> {
					check(left.getPrev() == right.getPrev());
					if (left.getNext() > right.getNext()) return left(set(left.getNext(), right.getNext()));
					if (left.getNext() < right.getNext()) return right(set(right.getNext(), left.getNext()));
					return empty();
				})
				.withSquashFunction(TestAdd.class, TestAdd.class, (op1, op2) -> add(op1.getDelta() + op2.getDelta()))
				.withSquashFunction(TestSet.class, TestSet.class, (op1, op2) -> set(op1.getPrev(), op2.getNext()))
				.withSquashFunction(TestAdd.class, TestSet.class, (op1, op2) -> set(op1.inverse().apply(op2.getPrev()), op2.getNext()))
				.withEmptyPredicate(TestAdd.class, add -> add.getDelta() == 0)
				.withEmptyPredicate(TestSet.class, set -> set.getPrev() == set.getNext())
				.withInvertFunction(TestAdd.class, op -> asList(op.inverse()))
				.withInvertFunction(TestSet.class, op -> asList(set(op.getNext(), op.getPrev())));
	}
}

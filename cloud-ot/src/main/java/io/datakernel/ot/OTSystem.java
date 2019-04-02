package io.datakernel.ot;

import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.util.Tuple2;
import io.datakernel.util.TupleConstructor2;
import io.datakernel.util.TupleConstructor3;
import io.datakernel.util.TupleConstructor4;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public interface OTSystem<D> {

	TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws OTTransformException;

	default TransformResult<D> transform(D leftDiff, D rightDiff) throws OTTransformException {
		return transform(singletonList(leftDiff), singletonList(rightDiff));
	}

	List<D> squash(List<? extends D> ops);

	boolean isEmpty(D op);

	<O extends D> List<D> invert(List<O> ops);

	static <D, D1, D2> OTSystem<D> mergeOtSystems(TupleConstructor2<List<D1>, List<D2>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2) {
		return new OTSystem<D>() {
			@Override
			public TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws OTTransformException {
				List<D1> leftDiffs1 = collect(leftDiffs, getter1);
				List<D2> leftDiffs2 = collect(leftDiffs, getter2);
				List<D1> rightDiffs1 = collect(rightDiffs, getter1);
				List<D2> rightDiffs2 = collect(rightDiffs, getter2);

				TransformResult<D1> transform1 = otSystem1.transform(leftDiffs1, rightDiffs1);
				TransformResult<D2> transform2 = otSystem2.transform(leftDiffs2, rightDiffs2);

				D left = constructor.create(transform1.left, transform2.left);
				D right = constructor.create(transform1.right, transform2.right);
				return TransformResult.of(left, right);
			}

			@Override
			public List<D> squash(List<? extends D> ops) {
				List<D1> squashed1 = otSystem1.squash(collect(ops, getter1));
				List<D2> squashed2 = otSystem2.squash(collect(ops, getter2));

				return singletonList(constructor.create(squashed1, squashed2));
			}

			@Override
			public boolean isEmpty(D op) {
				for (D1 operation : getter1.apply(op)) {
					if (!otSystem1.isEmpty(operation)) {
						return false;
					}
				}
				for (D2 operation : getter2.apply(op)) {
					if (!otSystem2.isEmpty(operation)) {
						return false;
					}
				}
				return true;
			}

			@Override
			public <O extends D> List<D> invert(List<O> ops) {
				List<D1> inverted1 = otSystem1.invert(collect(ops, getter1));
				List<D2> inverted2 = otSystem2.invert(collect(ops, getter2));

				return singletonList(constructor.create(inverted1, inverted2));
			}

			private <OP> List<OP> collect(List<? extends D> ops, Function<D, List<OP>> getter) {
				return ops.stream()
						.flatMap(op -> getter.apply(op).stream())
						.collect(toList());
			}
		};
	}

	static <D, D1, D2, D3> OTSystem<D> mergeOtSystems(TupleConstructor3<List<D1>, List<D2>, List<D3>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2,
			Function<D, List<D3>> getter3, OTSystem<D3> otSystem3) {

		OTSystem<Tuple2<List<D1>, List<D2>>> premerged = mergeOtSystems(Tuple2::new,
				Tuple2::getValue1, otSystem1,
				Tuple2::getValue2, otSystem2);

		return OTSystem.mergeOtSystems((tuples, list) -> {
					Tuple2<List<D1>, List<D2>> tuple = tuples.get(0);
					List<D2> ops2 = tuple.getValue2();
					return constructor.create(tuple.getValue1(), ops2, list);
				},
				d -> singletonList(new Tuple2<>(getter1.apply(d), getter2.apply(d))), premerged,
				getter3, otSystem3);
	}

	static <D, D1, D2, D3, D4> OTSystem<D> mergeOtSystems(TupleConstructor4<List<D1>, List<D2>, List<D3>, List<D4>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2,
			Function<D, List<D3>> getter3, OTSystem<D3> otSystem3,
			Function<D, List<D4>> getter4, OTSystem<D4> otSystem4) {

		OTSystem<Tuple2<List<D1>, List<D2>>> premerged1 = mergeOtSystems(Tuple2::new,
				Tuple2::getValue1, otSystem1,
				Tuple2::getValue2, otSystem2);

		OTSystem<Tuple2<List<D3>, List<D4>>> premerged2 = mergeOtSystems(Tuple2::new,
				Tuple2::getValue1, otSystem3,
				Tuple2::getValue2, otSystem4);

		return OTSystem.mergeOtSystems((tuples1, tuples2) -> {
					Tuple2<List<D1>, List<D2>> tuple1 = tuples1.get(0);
					Tuple2<List<D3>, List<D4>> tuple2 = tuples2.get(0);
					return constructor.create(tuple1.getValue1(), tuple1.getValue2(), tuple2.getValue1(), tuple2.getValue2());
				},
				d -> singletonList(new Tuple2<>(getter1.apply(d), getter2.apply(d))), premerged1,
				d -> singletonList(new Tuple2<>(getter3.apply(d), getter4.apply(d))), premerged2);
	}
}

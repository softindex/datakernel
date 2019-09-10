package io.global.ot.map;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Objects;

import static io.global.ot.map.SetValue.set;
import static java.util.Collections.singletonList;

public final class SetValueOTSystem {
	private SetValueOTSystem() {
		throw new AssertionError();
	}

	public static <V> OTSystem<SetValue<V>> create(@Nullable Comparator<V> comparator) {
		return OTSystemImpl.<SetValue<V>>create()
				.withEmptyPredicate(SetValue.class, SetValue::isEmpty)
				.withInvertFunction(SetValue.class, op -> singletonList(op.invert()))
				.withTransformFunction(SetValue.class, SetValue.class, (left, right) -> {
					if (!Objects.equals(left.getPrev(), right.getPrev())) {
						throw new OTTransformException("Previous values should be equal");
					}

					V leftNextValue = left.getNext();
					V rightNextValue = right.getNext();

					if (leftNextValue == null) {
						return TransformResult.left(set(null, rightNextValue));
					}
					if (rightNextValue == null) {
						return TransformResult.right(set(null, leftNextValue));
					}
					if (leftNextValue.equals(rightNextValue)) {
						return TransformResult.empty();
					}
					int compare = comparator == null ? -1 : comparator.compare(leftNextValue, rightNextValue);
					if (compare > 0) {
						return TransformResult.right(set(rightNextValue, leftNextValue));
					} else {
						return TransformResult.left(set(leftNextValue, rightNextValue));
					}
				})
				.withSquashFunction(SetValue.class, SetValue.class, (first, second) -> set(first.getPrev(), second.getNext()));
	}
}

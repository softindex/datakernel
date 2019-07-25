package io.global.ot.map;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.util.CollectorsEx;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.intersection;
import static io.datakernel.util.CollectionUtils.transformMapValues;
import static io.global.ot.map.SetValue.set;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public final class MapOTSystem {
	private MapOTSystem() {
		throw new AssertionError();
	}

	public static <K, V> OTSystem<MapOperation<K, V>> createOTSystem(Comparator<V> comparator) {
		return OTSystemImpl.<MapOperation<K, V>>create()
				.withInvertFunction(MapOperation.class, op ->
						singletonList(MapOperation.of(
								transformMapValues(op.getOperations(), SetValue::invert)))
				)
				.withEmptyPredicate(MapOperation.class, op ->
						op.getOperations().values().stream()
								.allMatch(SetValue::isEmpty)
				)
				.withTransformFunction(MapOperation.class, MapOperation.class, (left, right) -> {
					Map<K, SetValue<V>> leftOps = filterEmpty(left.getOperations());
					Map<K, SetValue<V>> rightOps = filterEmpty(right.getOperations());

					if (leftOps.equals(rightOps)) return TransformResult.empty();

					Set<K> intersection = intersection(leftOps.keySet(), rightOps.keySet());
					if (intersection.isEmpty()) return TransformResult.of(right, left);

					Map<K, SetValue<V>> rightTransformed = leftOps.entrySet().stream()
							.filter(entry -> !intersection.contains(entry.getKey()))
							.collect(toMap(Entry::getKey, Entry::getValue));
					Map<K, SetValue<V>> leftTransformed = rightOps.entrySet().stream()
							.filter(entry -> !intersection.contains(entry.getKey()))
							.collect(toMap(Entry::getKey, Entry::getValue));

					for (K key : intersection) {
						SetValue<V> leftSetOp = leftOps.get(key);
						SetValue<V> rightSetOp = rightOps.get(key);

						if (leftSetOp.equals(rightSetOp)) break;

						V leftNextValue = leftSetOp.getNext();
						V rightNextValue = rightSetOp.getNext();

						if (leftNextValue == null) {
							leftTransformed.put(key, rightSetOp);
							break;
						}
						if (rightNextValue == null) {
							rightTransformed.put(key, leftSetOp);
							break;
						}
						int compare = comparator.compare(leftNextValue, rightNextValue);
						if (compare > 0) {
							rightTransformed.put(key, set(rightNextValue, leftNextValue));
						} else if (compare < 0) {
							leftTransformed.put(key, set(leftNextValue, rightNextValue));
						}
					}
					return TransformResult.of(MapOperation.of(leftTransformed), MapOperation.of(rightTransformed));
				})
				.withSquashFunction(MapOperation.class, MapOperation.class, (op1, op2) ->
						MapOperation.of(
								filterEmpty(Stream.concat(
										op1.getOperations().entrySet().stream(),
										op2.getOperations().entrySet().stream())
										.collect(toMap(Entry::getKey, Entry::getValue,
												(first, second) -> set(first.getPrev(), second.getNext())))))
				);
	}

	private static <K, V> Map<K, SetValue<V>> filterEmpty(Map<K, SetValue<V>> map) {
		return map.entrySet().stream()
				.filter(entry -> !entry.getValue().isEmpty())
				.collect(CollectorsEx.toMap());
	}

}

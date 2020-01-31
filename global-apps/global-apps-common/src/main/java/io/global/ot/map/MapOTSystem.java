package io.global.ot.map;

import io.datakernel.common.CollectorsEx;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static io.datakernel.common.collection.CollectionUtils.intersection;
import static io.datakernel.common.collection.CollectionUtils.transformMapValues;
import static io.global.ot.map.SetValue.set;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public final class MapOTSystem {
	private MapOTSystem() {
		throw new AssertionError();
	}

	public static <K, V> OTSystem<MapOperation<K, V>> create() {
		return MapOTSystem.create(null);
	}

	public static <K, V> OTSystem<MapOperation<K, V>> create(@Nullable Comparator<V> comparator) {
		OTSystem<SetValue<V>> setValueOTSystem = SetValueOTSystem.create(comparator);
		return OTSystemImpl.<MapOperation<K, V>>create()
				.withInvertFunction(MapOperation.class, op ->
						singletonList(MapOperation.of(transformMapValues(op.getOperations(), SetValue::invert))))

				.withEmptyPredicate(MapOperation.class, op ->
						op.getOperations().values().stream().allMatch(setValueOTSystem::isEmpty))
				.withTransformFunction(MapOperation.class, MapOperation.class, (left, right) -> {
					Map<K, SetValue<V>> leftOps = nonEmpty(left.getOperations());
					Map<K, SetValue<V>> rightOps = nonEmpty(right.getOperations());

					if (leftOps.equals(rightOps)) return TransformResult.empty();

					Set<K> intersection = intersection(leftOps.keySet(), rightOps.keySet());
					if (intersection.isEmpty()) {
						return TransformResult.of(right, left);
					}

					Map<K, SetValue<V>> rightTransformed = leftOps.entrySet().stream()
							.filter(entry -> !intersection.contains(entry.getKey()))
							.collect(toMap(Entry::getKey, Entry::getValue));

					Map<K, SetValue<V>> leftTransformed = rightOps.entrySet().stream()
							.filter(entry -> !intersection.contains(entry.getKey()))
							.collect(toMap(Entry::getKey, Entry::getValue));

					for (K key : intersection) {
						TransformResult<SetValue<V>> subResult = setValueOTSystem.transform(leftOps.get(key), rightOps.get(key));

						subResult.left.forEach(setValue -> leftTransformed.put(key, setValue));
						subResult.right.forEach(setValue -> rightTransformed.put(key, setValue));
					}
					return TransformResult.of(MapOperation.of(leftTransformed), MapOperation.of(rightTransformed));
				})
				.withSquashFunction(MapOperation.class, MapOperation.class, (op1, op2) ->
						MapOperation.of(
								nonEmpty(Stream.concat(
										op1.getOperations().entrySet().stream(),
										op2.getOperations().entrySet().stream())
										.collect(toMap(Entry::getKey, Entry::getValue,
												(first, second) -> set(first.getPrev(), second.getNext()))))));
	}

	private static <K, V> Map<K, SetValue<V>> nonEmpty(Map<K, SetValue<V>> map) {
		return map.entrySet().stream()
				.filter(entry -> !entry.getValue().isEmpty())
				.collect(CollectorsEx.toMap());
	}
}

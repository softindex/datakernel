package io.global.ot.dictionary;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.util.CollectorsEx;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.intersection;
import static io.datakernel.util.CollectionUtils.transformMapValues;
import static io.global.ot.dictionary.SetOperation.set;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public final class DictionaryOTSystem {
	private DictionaryOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<DictionaryOperation> createOTSystem() {
		return OTSystemImpl.<DictionaryOperation>create()
				.withInvertFunction(DictionaryOperation.class, op ->
						singletonList(DictionaryOperation.of(transformMapValues(op.getOperations(), SetOperation::invert))))
				.withEmptyPredicate(DictionaryOperation.class,
						op -> op.getOperations().values().stream()
								.allMatch(SetOperation::isEmpty))
				.withTransformFunction(DictionaryOperation.class, DictionaryOperation.class, (left, right) -> {
					Map<String, SetOperation> leftOps = filterEmpty(left.getOperations());
					Map<String, SetOperation> rightOps = filterEmpty(right.getOperations());

					if (leftOps.equals(rightOps)) return TransformResult.empty();

					Set<String> intersection = intersection(leftOps.keySet(), rightOps.keySet());
					if (intersection.isEmpty()) return TransformResult.of(right, left);

					Map<String, SetOperation> rightTransformed = leftOps.entrySet().stream()
							.filter(entry -> !intersection.contains(entry.getKey()))
							.collect(toMap(Entry::getKey, Entry::getValue));
					Map<String, SetOperation> leftTransformed = rightOps.entrySet().stream()
							.filter(entry -> !intersection.contains(entry.getKey()))
							.collect(toMap(Entry::getKey, Entry::getValue));

					for (String key : intersection) {
						SetOperation leftSetOp = leftOps.get(key);
						SetOperation rightSetOp = rightOps.get(key);

						if (leftSetOp.equals(rightSetOp)) break;

						String leftNextValue = leftSetOp.getNext();
						String rightNextValue = rightSetOp.getNext();

						if (leftNextValue == null) {
							leftTransformed.put(key, rightSetOp);
							break;
						}
						if (rightNextValue == null) {
							rightTransformed.put(key, leftSetOp);
							break;
						}
						int compare = leftNextValue.compareTo(rightNextValue);
						if (compare > 0) {
							rightTransformed.put(key, set(rightNextValue, leftNextValue));
						} else if (compare < 0) {
							leftTransformed.put(key, set(leftNextValue, rightNextValue));
						}
					}
					return TransformResult.of(DictionaryOperation.of(leftTransformed), DictionaryOperation.of(rightTransformed));
				})
				.withSquashFunction(DictionaryOperation.class, DictionaryOperation.class,
						(op1, op2) -> DictionaryOperation.of(
								filterEmpty(Stream.concat(
										op1.getOperations().entrySet().stream(),
										op2.getOperations().entrySet().stream())
										.collect(toMap(Entry::getKey, Entry::getValue,
												(first, second) -> set(first.getPrev(), second.getNext()))))
						));
	}

	private static Map<String, SetOperation> filterEmpty(Map<String, SetOperation> map) {
		return map.entrySet().stream()
				.filter(entry -> !entry.getValue().isEmpty())
				.collect(CollectorsEx.toMap());
	}

}

/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot.utils;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredInput;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.common.exception.parse.ParseException;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.system.OTSystem;
import io.datakernel.ot.system.OTSystemImpl;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.collection.CollectionUtils.difference;
import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.ot.TransformResult.*;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class Utils {

	private static final Object INVALID_KEY = new Object();

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
					checkArgument(left.getPrev() == right.getPrev(), "Previous values of left and right set operation should be equal");
					if (left.getNext() > right.getNext()) return left(set(left.getNext(), right.getNext()));
					if (left.getNext() < right.getNext()) return right(set(right.getNext(), left.getNext()));
					return empty();
				})
				.withSquashFunction(TestAdd.class, TestAdd.class, (op1, op2) -> add(op1.getDelta() + op2.getDelta()))
				.withSquashFunction(TestSet.class, TestSet.class, (op1, op2) -> set(op1.getPrev(), op2.getNext()))
				.withSquashFunction(TestAdd.class, TestSet.class, (op1, op2) -> set(op1.inverse().apply(op2.getPrev()), op2.getNext()))
				.withSquashFunction(TestSet.class, TestAdd.class, (op1, op2) -> set(op1.getPrev(), op1.getNext() + op2.getDelta()))
				.withEmptyPredicate(TestAdd.class, add -> add.getDelta() == 0)
				.withEmptyPredicate(TestSet.class, set -> set.getPrev() == set.getNext())
				.withInvertFunction(TestAdd.class, op -> asList(op.inverse()))
				.withInvertFunction(TestSet.class, op -> asList(set(op.getNext(), op.getPrev())));
	}

	public static StructuredCodec<TestOp> OP_CODEC = new StructuredCodec<TestOp>() {
		@Override
		public void encode(StructuredOutput out, TestOp testOp) {
			out.writeObject(() -> {
				if (testOp instanceof TestAdd) {
					out.writeKey("add");
					out.writeInt(((TestAdd) testOp).getDelta());
				} else {
					out.writeKey("set");
					out.writeTuple(() -> {
						TestSet testSet = (TestSet) testOp;
						out.writeInt(testSet.getPrev());
						out.writeInt(testSet.getNext());
					});
				}
			});
		}

		@Override
		public TestOp decode(StructuredInput in) throws ParseException {
			return in.readObject($ -> {
				String key = in.readKey();
				switch (key) {
					case "add":
						return new TestAdd(in.readInt());
					case "set":
						return in.readTuple($2 -> {
							int prev = in.readInt();
							int next = in.readInt();
							return new TestSet(prev, next);
						});
					default:
						throw new ParseException("Invalid TestOp key " + key);
				}
			});
		}
	};

	public static <K> long calcLevels(K commitId, Map<K, Long> levels, Function<K, Collection<K>> getParents) {
		if (!levels.containsKey(commitId)) {
			levels.put(commitId, 1L + getParents.apply(commitId).stream()
					.mapToLong(parentId -> calcLevels(parentId, levels, getParents))
					.max()
					.orElse(0L));
		}
		return levels.get(commitId);
	}

	public static <D> Consumer<OTGraphBuilder<Long, D>> asLong(Consumer<OTGraphBuilder<Integer, D>> intGraphConsumer) {
		return longGraphBuilder ->
				intGraphConsumer.accept((parent, child, diffs) ->
						longGraphBuilder.add((long) parent, (long) child, diffs));
	}

	public static <K, D> List<OTCommit<K, D>> commits(Consumer<OTGraphBuilder<K, D>> graphBuilder) {
		return commits(graphBuilder, true, 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> List<OTCommit<K, D>> commits(Consumer<OTGraphBuilder<K, D>> graphBuilder, boolean withRoots, long initialLevel) {
		Map<K, Map<K, List<D>>> graph = new HashMap<>();
		graphBuilder.accept((parent, child, diffs) ->
				graph.computeIfAbsent(child, $ -> new HashMap<>()).computeIfAbsent(parent, $ -> new ArrayList<>()).addAll(diffs));
		Set<K> heads = difference(
				graph.keySet(),
				graph.values()
						.stream()
						.flatMap(parents -> parents.keySet().stream())
						.collect(toSet()));
		Set<K> roots = difference(
				graph.values()
						.stream()
						.flatMap(parents -> parents.keySet().stream())
						.collect(toSet()),
				graph.keySet());
		HashMap<K, Long> levels = new HashMap<>();
		for (K head : heads) {
			calcLevels(head, levels, id -> graph.getOrDefault(id, emptyMap()).keySet());
		}
		if (withRoots) {
			if (roots.size() == 1) {
				graph.put(first(roots), emptyMap()); // true root
			} else {
				roots.forEach(root -> graph.put(root, singletonMap((K) INVALID_KEY, emptyList()))); // intermediate node
			}
		}
		return graph.entrySet()
				.stream()
				.map(entry -> OTCommit.of(0, entry.getKey(), entry.getValue().keySet(), entry.getValue()::get, id -> initialLevel + levels.getOrDefault(id, 0L) - 1L)
						.withTimestamp(initialLevel - 1L + levels.get(entry.getKey())))
				.collect(toList());
	}

}

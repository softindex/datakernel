package io.datakernel.ot.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.ot.TransformResult.*;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.Preconditions.check;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class Utils {

	private static final Object INVALID_KEY = new Object();

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
					check(left.getPrev() == right.getPrev(), "Previous values of left and right set operation should be equal");
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

	public static TypeAdapter<TestOp> OP_ADAPTER = new TypeAdapter<TestOp>() {
		@Override
		public void write(JsonWriter jsonWriter, TestOp testOp) throws IOException {
			jsonWriter.beginObject();
			if (testOp instanceof TestAdd) {
				jsonWriter.name("add");
				jsonWriter.value(((TestAdd) testOp).getDelta());
			} else {
				jsonWriter.name("set");
				TestSet testSet = (TestSet) testOp;
				jsonWriter.beginArray();
				jsonWriter.value(testSet.getPrev());
				jsonWriter.value(testSet.getNext());
				jsonWriter.endArray();
			}
			jsonWriter.endObject();
		}

		@Override
		public TestOp read(JsonReader jsonReader) throws IOException {
			jsonReader.beginObject();
			TestOp testOp;
			String name = jsonReader.nextName();
			if (name.equals("add")) {
				testOp = new TestAdd(jsonReader.nextInt());
			} else {
				jsonReader.beginArray();
				int prev = jsonReader.nextInt();
				int next = jsonReader.nextInt();
				jsonReader.endArray();
				testOp = new TestSet(prev, next);
			}
			jsonReader.endObject();
			return testOp;
		}
	};

	public static <K> long calcLevels(K commitId, Map<K, Long> levels, Function<K, Collection<K>> getParents) {
		return levels.computeIfAbsent(commitId,
				k -> 1L + getParents.apply(k).stream()
						.mapToLong(parentId -> calcLevels(parentId, levels, getParents))
						.max()
						.orElse(0L));
	}

	public static <D> Consumer<OTGraphBuilder<Long, D>> asLong(Consumer<OTGraphBuilder<Integer, D>> intGraphConsumer) {
		return longGraphBuilder ->
				intGraphConsumer.accept((parent, child, diffs) ->
						longGraphBuilder.add((long) parent, (long) child, diffs));
	}

	public static <K, D> List<OTCommit<K, D>> commits(Consumer<OTGraphBuilder<K, D>> graphBuilder) {
		return commits(graphBuilder, true, 1L);
	}

	public static <K, D> List<OTCommit<K, D>> commits(Consumer<OTGraphBuilder<K, D>> graphBuilder, boolean withRoots, long initialLevel) {
		Map<K, Map<K, List<D>>> graph = new HashMap<>();
		graphBuilder.accept((parent, child, diffs) -> graph
				.computeIfAbsent(child, $ -> new HashMap<>())
				.computeIfAbsent(parent, $ -> new ArrayList<>())
				.addAll(diffs));
		Set<K> heads = difference(
				graph.keySet(),
				graph.values().stream()
						.flatMap(t -> t.keySet().stream())
						.collect(toSet()));
		Set<K> roots = difference(
				graph.values().stream()
						.flatMap(t -> t.keySet().stream())
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
				//noinspection unchecked
				roots.forEach(root -> graph.put(root, singletonMap((K) INVALID_KEY, null))); // intermediate node
			}
		}
		return graph.entrySet().stream()
				.map(entry -> OTCommit.of(entry.getKey(), entry.getValue(), initialLevel - 1L + levels.get(entry.getKey()))
						.withTimestamp(initialLevel - 1L + levels.get(entry.getKey())))
				.collect(toList());
	}

}

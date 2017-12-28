package io.datakernel.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncFunction;
import io.datakernel.async.Stages;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteSql;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;

public class GraphUtils {

	public static <T> TypeAdapter<T> skipReadAdapter(Supplier<T> supplier) {
		return new TypeAdapter<T>() {
			@Override
			public void write(JsonWriter jsonWriter, T t) throws IOException {
				throw new UnsupportedOperationException("write");
			}

			@Override
			public T read(JsonReader jsonReader) throws IOException {
				while (jsonReader.hasNext()) jsonReader.skipValue();
				return supplier.get();
			}
		};
	}

	private static <K> CompletionStage<Void> visitNodes(AsyncFunction<K, Set<K>> parentLoader,
	                                                    Predicate<K> filter, Set<K> heads) {
		return Stages.runSequence(heads.stream()
				.filter(filter)
				.<AsyncCallable<Void>>map(k -> () -> parentLoader.apply(k)
						.thenComposeAsync(parentIds -> visitNodes(parentLoader, filter, parentIds)))
				.iterator());
	}

	public static <K, T> CompletionStage<Void> visitNodes(AsyncFunction<K, T> parentLoader,
	                                                      AsyncFunction<T, ?> processor,
	                                                      Function<T, Set<K>> parents,
	                                                      Predicate<K> filter, Set<K> heads) {
		final AsyncFunction<K, Set<K>> function = input -> parentLoader.apply(input)
				.thenComposeAsync(t -> processor.apply(t)
						.thenApplyAsync($ -> parents.apply(t)));
		return GraphUtils.visitNodes(function, filter, heads);
	}

	public static <K> CompletionStage<Void> visitNodes(AsyncFunction<K, Set<K>> parentLoader,
	                                                   AsyncFunction<K, ?> processor,
	                                                   Predicate<K> filter, Set<K> heads) {
		final AsyncFunction<K, Set<K>> function = input -> processor.apply(input)
				.thenComposeAsync($ -> parentLoader.apply(input));
		return GraphUtils.visitNodes(function, filter, heads);
	}

	public static <K> Predicate<K> visitOnceFilter() {
		final Set<K> visited = new HashSet<>();
		return visited::add;
	}

	public static <D> AsyncFunction<Integer, OTCommit<Integer, D>> sqlLoaderWithoutCheckpointAndDiffs(OTRemoteSql<D> otRemoteSql) {
		return headId -> otRemoteSql.loadDiff(headId).thenApply(diffs -> {
			final Map<Integer, List<D>> parents = diffs.stream().collect(toMap(OTRemoteSql.Diff::getParentId, diff -> emptyList()));
			return OTCommit.of(headId, parents);
		});
	}

	public static <K, D> AsyncFunction<OTCommit<K, D>, Void> dotIdProcessor(StringBuilder sb) {
		return commit -> {
			final K commitId = commit.getId();
			for (K parentId : commit.getParents().keySet()) {
				sb.append(String.format("%s -> %s%n", parentId, commitId));
			}
			return Stages.of(null);
		};
	}

	public static <D> CompletionStage<Void> appendMerges(OTRemoteSql<D> otRemoteSql, StringBuilder sb) {
		return otRemoteSql.loadMerges(false).thenAccept(lists -> {
			for (OTRemoteSql.Merge merge : lists) {
				final List<Integer> parentIds = merge.getParentIds();
				final String str = parentIds.stream().map(Object::toString).collect(Collectors.joining(" ", "\"", "\""));
				sb.append(String.format("%s [fillcolor=green, style=\"filled\"]%n", str));
				sb.append(String.format("{rank=same; %s; %s}%n", parentIds.get(parentIds.size() - 1), str));
			}
		});
	}

	public static <D> AsyncFunction<OTCommit<Integer, D>, Void> dotSqlProcessor(OTRemoteSql<D> otRemoteSql, StringBuilder sb) {
		return commit -> {
			final Integer commitId = commit.getId();
			return otRemoteSql.loadRevision(commitId).thenAccept(revision -> {
				final String createdBy = revision.getCreatedBy();
				final Timestamp timestamp = Timestamp.valueOf(revision.getTimestamp());
				sb.append(String.format("%d [label=\"%d\\n%s\\n%s\"]%n", commitId, commitId, createdBy, timestamp));
				for (Integer parentId : commit.getParents().keySet()) {
					sb.append(String.format("%d -> %d%n", parentId, commitId));
				}
			});
		};
	}
}

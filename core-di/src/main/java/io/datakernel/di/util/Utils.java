package io.datakernel.di.util;

import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.core.*;

import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.*;

public final class Utils {
	private Utils() {}

	private static final BiConsumer<Map<Object, Set<Object>>, Map<Object, Set<Object>>> MULTIMAP_MERGER =
			(into, from) -> from.forEach((k, v) -> into.computeIfAbsent(k, $ -> new HashSet<>()).addAll(v));

	@SuppressWarnings("unchecked")
	public static <K, V> BiConsumer<Map<K, Set<V>>, Map<K, Set<V>>> multimapMerger() {
		return (BiConsumer<Map<K, Set<V>>, Map<K, Set<V>>>) (BiConsumer) MULTIMAP_MERGER;
	}

	public static <T> T[] next(T[] items, T item) {
		T[] next = Arrays.copyOf(items, items.length + 1);
		next[items.length] = item;
		return next;
	}

	public static String getScopeDisplayString(Scope[] scope) {
		return scope.length != 0 ? Arrays.stream(scope).map(Scope::getDisplayString).collect(joining("->", "()->", "")) : "()";
	}

	public static void mergeMultibinders(Map<Key<?>, Multibinder<?>> into, Map<Key<?>, Multibinder<?>> from) {
		from.forEach((k, v) -> into.merge(k, v, (oldResolver, newResolver) -> {
			if (!oldResolver.equals(newResolver)) {
				throw new DIException("More than one multibinder per key");
			}
			return oldResolver;
		}));
	}

	public static <K, V> void combineMultimap(Map<K, Set<V>> accumulator, Map<K, Set<V>> multimap) {
		multimap.forEach((key, set) -> accumulator.computeIfAbsent(key, $ -> new HashSet<>()).addAll(set));
	}

	public static <T> Set<T> union(Set<T> first, Set<T> second) {
		Set<T> result = new HashSet<>((first.size() + second.size()) * 4 / 3 + 1);
		result.addAll(first);
		result.addAll(second);
		return result;
	}

	public static <K, V> Map<K, V> override(Map<K, V> into, Map<K, V> from) {
		Map<K, V> result = new HashMap<>((from.size() + into.size()) * 4 / 3 + 1);
		result.putAll(from);
		result.putAll(into);
		return result;
	}

	public static <T, K, V> Collector<T, ?, Map<K, Set<V>>> toMultimap(Function<? super T, ? extends K> keyMapper,
			Function<? super T, ? extends V> valueMapper) {
		return toMap(keyMapper, t -> singleton(valueMapper.apply(t)), Utils::union);
	}

	public static <K, V> Map<K, Set<V>> toMultimap(Map<K, V> map) {
		return map.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> singleton(entry.getValue())));
	}

	public static <K, V, V1> Map<K, Set<V1>> transformMultimapValues(Map<K, Set<V>> multimap, BiFunction<? super K, ? super V, ? extends V1> fn) {
		return transformMultimap(multimap, Function.identity(), fn);
	}

	public static <K, V, K1, V1> Map<K1, Set<V1>> transformMultimap(Map<K, Set<V>> multimap, Function<? super K, ? extends K1> fnKey, BiFunction<? super K, ? super V, ? extends V1> fnValue) {
		return multimap.entrySet()
				.stream()
				.collect(toMap(
						entry -> fnKey.apply(entry.getKey()),
						entry -> entry.getValue()
								.stream()
								.map(v -> fnValue.apply(entry.getKey(), v))
								.collect(toSet())));
	}

	public static <K, V> Map<K, V> squash(Map<K, Set<V>> multimap, BiFunction<K, Set<V>, V> squasher) {
		return multimap.entrySet().stream()
				.collect(toMap(Entry::getKey, e -> squasher.apply(e.getKey(), e.getValue())));
	}

	public static void checkArgument(boolean condition, String message) {
		if (!condition) {
			throw new IllegalArgumentException(message);
		}
	}

	public static void checkState(boolean condition, String message) {
		if (!condition) {
			throw new IllegalStateException(message);
		}
	}

	public static boolean isKeySet(Key<?> key) {
		if (Types.getRawType(key.getType()) != Set.class) {
			return false;
		}
		Name name = key.getName();
		if (name == null || !name.isMarkedBy(KeySetAnnotation.class)) {
			return false;
		}
		Key<?> param = key.getTypeParameter(0);
		if (Types.getRawType(param.getType()) != Key.class) {
			return false;
		}
		Type subparam = param.getTypeParameter(0).getType();
		if (!(subparam instanceof WildcardType)) {
			return false;
		}
		WildcardType wildcard = (WildcardType) subparam;
		if (wildcard.getLowerBounds().length != 0) {
			return false;
		}
		if (wildcard.getUpperBounds().length != 1) {
			return false;
		}
		return wildcard.getUpperBounds()[0] == Object.class;
	}

	public static String getLocation(Binding<?> binding) {
		LocationInfo location = binding.getLocation();
		return "at " + (location != null ? location.toString() : "<unknown binding location>");
	}

	/**
	 * A shortcut for printing the result of {@link #makeGraphVizGraph} into the standard output.
	 */
	public static void printGraphVizGraph(Trie<Scope, Map<Key<?>, Binding<?>>> trie) {
//		System.out.println("https://somegraphvizurl/#" + URLEncoder.encode(makeGraphVizGraph(trie), "utf-8").replaceAll("\\+", "%20"));
		System.out.println(makeGraphVizGraph(trie));
	}

	/**
	 * Makes a GraphViz graph representation of the binding graph.
	 * Scopes are grouped nicely into subgraph boxes and dependencies are properly drawn from lower to upper scopes.
	 */
	public static String makeGraphVizGraph(Trie<Scope, Map<Key<?>, Binding<?>>> trie) {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n	rankdir=BT;\n");
		Set<ScopedValue<Key<?>>> known = new HashSet<>();
		writeNodes(UNSCOPED, trie, known, "", new int[]{0}, sb);
		writeEdges(UNSCOPED, trie, known, sb);
		sb.append("}\n");
		return sb.toString();
	}

	private static void writeNodes(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> trie, Set<ScopedValue<Key<?>>> known, String indent, int[] scopeCount, StringBuilder sb) {
		if (scope != UNSCOPED) {
			sb.append('\n').append(indent)
					.append("subgraph cluster_").append(scopeCount[0]++).append(" {\n")
					.append(indent).append("\tlabel=\"").append(scope[scope.length - 1].getDisplayString().replace("\"", "\\\"")).append("\"\n");
		}

		for (Entry<Scope, Trie<Scope, Map<Key<?>, Binding<?>>>> entry : trie.getChildren().entrySet()) {
			writeNodes(next(scope, entry.getKey()), entry.getValue(), known, indent + '\t', scopeCount, sb);
		}

		Set<Key<?>> leafs = new HashSet<>();

		for (Entry<Key<?>, Binding<?>> entry : trie.get().entrySet()) {
			Key<?> key = entry.getKey();
			if (entry.getValue().getDependencies().size() == 0) {
				leafs.add(key);
			}
			known.add(ScopedValue.of(scope, key));
			sb.append(indent)
					.append('\t')
					.append('"').append(getScopeId(scope)).append(key.toString().replace("\"", "\\\"")).append('"')
					.append(" [label=\"").append(key.getDisplayString().replace("\"", "\\\"")).append("\"];\n");
		}

		if (!leafs.isEmpty()) {
			sb.append(leafs.stream()
					.map(key -> '"' + getScopeId(scope) + key.toString().replace("\"", "\\\"") + '"')
					.collect(joining(" ", '\n' + indent + "\t{ rank=same; ", " }\n")));
			if (scope == UNSCOPED) {
				sb.append('\n');
			}
		}

		if (scope != UNSCOPED) {
			sb.append(indent).append("}\n\n");
		}
	}

	private static void writeEdges(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> trie, Set<ScopedValue<Key<?>>> known, StringBuilder sb) {
		String scopePath = getScopeId(scope);

		for (Entry<Key<?>, Binding<?>> entry : trie.get().entrySet()) {
			String key = "\"" + scopePath + entry.getKey().toString().replace("\"", "\\\"") + "\"";
			for (Dependency dependency : entry.getValue().getDependencies()) {
				Key<?> depKey = dependency.getKey();

				Scope[] depScope = scope;
				while (!known.contains(ScopedValue.of(depScope, depKey)) & depScope.length != 0) {
					depScope = Arrays.copyOfRange(depScope, 0, depScope.length - 1);
				}

				if (depScope.length == 0) {
					String dep = "\"" + scopePath + depKey.toString().replace("\"", "\\\"") + '"';

					if (known.add(ScopedValue.of(scope, depKey))) {
						sb.append('\t')
								.append(dep)
								.append(" [label=\"")
								.append(depKey.getDisplayString().replace("\"", "\\\""))
								.append("\", style=dashed, color=")
								.append(dependency.isRequired() ? "red" : "orange")
								.append("];\n");
					}
					sb.append('\t').append(key).append(" -> ").append(dep);
				} else {
					sb.append('\t').append(key).append(" -> \"").append(getScopeId(depScope)).append(depKey.toString().replace("\"", "\\\"")).append('"');
				}
				if (!dependency.isRequired()) {
					sb.append(" [style=dashed]");
				}
				sb.append(";\n");
			}
		}
		for (Entry<Scope, Trie<Scope, Map<Key<?>, Binding<?>>>> entry : trie.getChildren().entrySet()) {
			writeEdges(next(scope, entry.getKey()), entry.getValue(), known, sb);
		}
	}

	private static String getScopeId(Scope[] scope) {
		return Arrays.stream(scope).map(Scope::toString).collect(joining("->", "()->", "")).replace("\"", "\\\"");
	}

	public static int getKeyDisplayCenter(Key<?> key) {
		Name name = key.getName();
		int nameOffset = name != null ? name.getDisplayString().length() + 1 : 0;
		return nameOffset + (key.getDisplayString().length() - nameOffset) / 2;
	}

	public static String drawCycle(Key<?>[] cycle) {
		int offset = getKeyDisplayCenter(cycle[0]);
		String cycleString = Arrays.stream(cycle).map(Key::getDisplayString).collect(joining(" -> ", "\t", ""));
		String indent = new String(new char[offset]).replace('\0', ' ');
		String line = new String(new char[cycleString.length() - offset]).replace('\0', '-');
		return cycleString + " -,\n\t" + indent + "^" + line + "'";
	}
}

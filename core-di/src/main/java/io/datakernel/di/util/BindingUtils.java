package io.datakernel.di.util;

import io.datakernel.di.Binding;
import io.datakernel.di.Dependency;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;

import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.toMultimap;
import static java.util.stream.Collectors.toSet;

public final class BindingUtils {
	private BindingUtils() {
		throw new AssertionError("nope.");
	}

	/**
	 * This method returns mapping from *unstatisfied keys* to *bindings that require them*
	 * and not the common *key and the bindings that provide it*
	 */
	public static Map<Key<?>, Set<Binding<?>>> getUnsatisfiedDependencies(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getUnsatisfiedDependencies(new HashSet<>(), bindings)
				.collect(toMultimap(dtb -> dtb.key, dtb -> dtb.binding));
	}

	private static Stream<DependencyToBinding> getUnsatisfiedDependencies(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return Stream.concat(
				getUnsatisfiedDependencies(known, bindings.get()),
				bindings.getChildren().values().stream().flatMap(scopeBindings -> getUnsatisfiedDependencies(known, scopeBindings))
		);
	}

	private static Stream<DependencyToBinding> getUnsatisfiedDependencies(Set<Key<?>> known, Map<Key<?>, Binding<?>> bindings) {
		known.addAll(bindings.keySet());
		return bindings.values().stream()
				.flatMap(binding -> Arrays.stream(binding.getDependencies())
						.filter(dependency -> dependency.isRequired() && !known.contains(dependency.getKey()))
						.map(dependency -> new DependencyToBinding(dependency.getKey(), binding)));
	}

	private static class DependencyToBinding {
		Key<?> key;
		Binding<?> binding;

		public DependencyToBinding(Key<?> key, Binding<?> binding) {
			this.key = key;
			this.binding = binding;
		}
	}

	public static Set<Key<?>[]> getCycles(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getCycles(new HashSet<>(), bindings).collect(toSet());
	}

	private static Stream<Key<?>[]> getCycles(Set<Key<?>> visited, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return Stream.concat(
				dfs(visited, bindings.get()).stream(),
				bindings.getChildren().values().stream().flatMap(scopeBindings -> getCycles(new HashSet<>(visited), scopeBindings))
		);
	}

	private static Set<Key<?>[]> dfs(Set<Key<?>> visited, Map<Key<?>, Binding<?>> bindings) {
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		for (Key<?> key : bindings.keySet()) {
			dfs(bindings, visited, visiting, cycles, key);
		}
		return cycles;
	}

	private static void dfs(Map<Key<?>, Binding<?>> bindings, Set<Key<?>> visited, LinkedHashSet<Key<?>> visiting, Set<Key<?>[]> cycles, Key<?> key) {
		if (visited.contains(key)) {
			return;
		}
		Binding<?> binding = bindings.get(key);
		if (binding == null) {
			// just ignore unsatisfied dependencies as if they never existed
			visited.add(key); // add to visited as a tiny optimization
			return;
		}
		if (!visiting.add(key)) {
			// so at this point visiting set looks something like a -> b -> c -> d -> e -> g -> c,
			// and in the code below we just get d -> e -> g -> c out of it
			Iterator<Key<?>> backtracked = visiting.iterator();
			int skipped = 0;
			while (backtracked.hasNext() && !backtracked.next().equals(key)) {
				skipped++;
			}
			Key<?>[] cycle = new Key[visiting.size() - skipped];
			for (int i = 0; i < cycle.length - 1; i++) {
				cycle[i] = backtracked.next(); // call to next() without hasNext() should be ok here
			}
			cycle[cycle.length - 1] = key;
			cycles.add(cycle);
			return;
		}
		for (Dependency dependency : binding.getDependencies()) {
			dfs(bindings, visited, visiting, cycles, dependency.getKey());
		}
		visiting.remove(key);
		visited.add(key);
	}
}

package io.datakernel.di.impl;

import io.datakernel.di.core.*;
import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static io.datakernel.di.core.Multibinder.ERROR_ON_DUPLICATE;
import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toSet;

/**
 * This class contains a set of utils for working with binding graph trie.
 */
public final class Preprocessor {
	/**
	 * This is a special marker {@link BindingCompiler} for phantom bindings that will to be replaced by a generated ones.
	 *
	 * @see #completeBindingGraph
	 */
	public static final BindingCompiler<?> TO_BE_GENERATED = (compiledBindings, threadsafe, synchronizedScope, index) -> {
		throw new AssertionError("This binding exists as a marker to be replaced by a generated one, so if you see this message then somethning is really wrong");
	};

	private Preprocessor() {}

	/**
	 * This method converts a trie of binding multimaps, that is provided from the modules,
	 * into a trie of binding maps on which the {@link Injector} would actually operate.
	 */
	@SuppressWarnings("unchecked")
	public static Trie<Scope, Map<Key<?>, Binding<?>>> resolveConflicts(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings, Map<Key<?>, Multibinder<?>> multibinders) {
		return bindings.map(localBindings -> squash(localBindings, (k, v) -> {
			Map<Boolean, Set<Binding<?>>> separated = v.stream().collect(partitioningBy(b -> b.getCompiler() != TO_BE_GENERATED, toSet()));
			Set<Binding<?>> real = separated.get(true);
			Set<Binding<?>> phantom = separated.get(false);

			switch (real.size()) {
				case 0:
					if (phantom.isEmpty()) {
						throw new DIException("Provided key " + k + " with no associated bindings");
					}
					Set<Dependency> dependencies = phantom.stream().flatMap(binding -> binding.getDependencies().stream()).collect(toSet());
					LocationInfo location = phantom.stream().map(Binding::getLocation).filter(Objects::nonNull).findAny().orElse(null);
					return new Binding<>(dependencies, TO_BE_GENERATED).at(location);
				case 1:
					return real.iterator().next();
				default:
					return ((Multibinder) multibinders.getOrDefault(k, ERROR_ON_DUPLICATE)).multibind(real);
			}
		}));
	}

	/**
	 * This method recursively tries to generate missing dependency bindings using given {@link BindingGenerator generator}
	 * and apply given {@link BindingTransformer} to all bindings once.
	 *
	 * @see BindingGenerator#combinedGenerator
	 * @see BindingTransformer#combinedTransformer
	 */
	public static void completeBindingGraph(Trie<Scope, Map<Key<?>, Binding<?>>> bindings,
			BindingTransformer<?> transformer, BindingGenerator<?> generator) {
		completeBindingGraph(new HashMap<>(bindings.get()), UNSCOPED, bindings, transformer, generator);
	}

	private static void completeBindingGraph(Map<Key<?>, Binding<?>> known,
			Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings,
			BindingTransformer<?> transformer, BindingGenerator<?> generator) {
		bindings.getChildren().forEach((subscope, subtrie) -> completeBindingGraph(override(known, subtrie.get()), next(scope, subscope), subtrie, transformer, generator));
		completeBindingGraph(known, scope, bindings.get(), generator, transformer);
	}

	@SuppressWarnings("unchecked")
	private static void completeBindingGraph(Map<Key<?>, Binding<?>> known,
			Scope[] scope, Map<Key<?>, Binding<?>> localBindings,
			BindingGenerator<?> generator, BindingTransformer<?> transformer) {

		Map<Key<?>, Binding<?>> generated = new HashMap<>();

		BindingLocator bindings = new BindingLocator() {
			@Override
			@Nullable
			public <T> Binding<T> get(Key<T> key) {
				Binding<T> binding = (Binding<T>) generated.get(key);
				if (binding == null) {
					binding = (Binding<T>) known.get(key);
				}
				if (binding != null && binding.getCompiler() != TO_BE_GENERATED) {
					return binding;
				}

				binding = ((BindingGenerator<T>) generator).generate(this, scope, key);
				if (binding == null) {
					return null;
				}

				binding = ((BindingTransformer<T>) transformer).transform(this, scope, key, binding);

				generated.put(key, binding);

				// ensure that its dependencies are generated if necessary
				for (Dependency dependency : binding.getDependencies()) {
					get(dependency.getKey());
				}
				return binding;
			}
		};

		for (Entry<Key<?>, Binding<?>> entry : localBindings.entrySet()) {
			Key<Object> key = (Key<Object>) entry.getKey();
			Binding<Object> binding = (Binding<Object>) entry.getValue();

			if (binding.getCompiler() == TO_BE_GENERATED) {
				Binding<Object> generatedBinding = bindings.get(key);
				if (generatedBinding == null) {
					// these bindings are the ones requested with plain `bind(...);` call, here we fail fast
					// see comment below where dependencies are generated
					throw new DIException("Refused to generate a requested binding for key " + key.getDisplayString());
				}
				generatedBinding.at(binding.getLocation()); // set its location to one from the generation request
				known.put(key, generatedBinding);
				generated.put(key, generatedBinding);
			} else {
				Binding<Object> transformed = ((BindingTransformer<Object>) transformer).transform(bindings, scope, key, binding);
				if (transformed != binding) {
					generated.put(key, transformed);
				}
			}

			for (Dependency dependency : binding.getDependencies()) {
				Key<?> depKey = dependency.getKey();
				if (known.containsKey(depKey)) {
					continue;
				}
				known.put(depKey, bindings.get(depKey)); // put even nulls in known just as a little optimization
				// when generating dependencies we don't fail and just do nothing
				// unsatisfied dependency check will collect all of them and make a nice error
			}
		}
		localBindings.putAll(generated);
	}

	/**
	 * A method that checks binding graph trie completeness, meaning that no binding references a key that is not present
	 * at same or lower level of the trie.
	 * <p>
	 * It returns a mapping from unsatisfied keys to a set of key-binding pairs that require them.
	 * If that mapping is empty then the graph trie is valid.
	 */
	public static Map<Key<?>, Set<Entry<Key<?>, Binding<?>>>> getUnsatisfiedDependencies(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getUnsatisfiedDependenciesStream(new HashSet<>(known), bindings)
				.collect(toMultimap(dtb -> dtb.dependency, dtb -> dtb.keybinding));
	}

	private static Stream<DependencyToBinding> getUnsatisfiedDependenciesStream(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return Stream.concat(
				bindings.get().entrySet().stream()
						.flatMap(e -> e.getValue().getDependencies().stream()
								.filter(dependency -> dependency.isRequired() && !known.contains(dependency.getKey()))
								.map(dependency -> new DependencyToBinding(dependency.getKey(), e))),
				bindings.getChildren()
						.values()
						.stream()
						.flatMap(scopeBindings -> getUnsatisfiedDependenciesStream(union(known, scopeBindings.get().keySet()), scopeBindings))
		);
	}

	private static class DependencyToBinding {
		final Key<?> dependency;
		final Entry<Key<?>, Binding<?>> keybinding;

		public DependencyToBinding(Key<?> dependency, Entry<Key<?>, Binding<?>> keybinding) {
			this.dependency = dependency;
			this.keybinding = keybinding;
		}
	}

	/**
	 * A method that checks binding graph trie for cycles, it ensures that each trie node is a <a href="https://en.wikipedia.org/wiki/Directed_acyclic_graph">DAG</a>.
	 * <p>
	 * It does so by performing a simple DFS on each graph that ignores unsatisfied dependencies (dependency keys that have no associated binding).
	 * <p>
	 * It returns a set of key arrays that represent cycles.
	 * If that set is empty then no graphs in given trie contains cycles.
	 */
	public static Set<Key<?>[]> getCyclicDependencies(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getCyclicDependenciesStream(bindings).collect(toSet());
	}

	private static Stream<Key<?>[]> getCyclicDependenciesStream(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		// since no cycles are possible between scopes,
		// we just run a simple dfs that ignores unsatisfied
		// dependencies for each scope independently
		return Stream.concat(
				dfs(bindings.get()).stream(),
				bindings.getChildren().values().stream().flatMap(Preprocessor::getCyclicDependenciesStream)
		);
	}

	private static Set<Key<?>[]> dfs(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		// the DAG is not necessarily connected, so we go through any possibly disconnected part
		for (Key<?> key : bindings.keySet()) {
			if (!visited.contains(key)) {
				dfs(bindings, visited, visiting, cycles, key);
			}
		}
		return cycles;
	}

	private static void dfs(Map<Key<?>, Binding<?>> bindings, Set<Key<?>> visited, LinkedHashSet<Key<?>> visiting, Set<Key<?>[]> cycles, Key<?> key) {
		Binding<?> binding = bindings.get(key);
		if (binding == null) {
			// just ignore unsatisfied dependencies as if they never existed
			// (they may be unsatisfied and be checked later by unsatisfied dependency check or they may just reference some upper scope)
			visited.add(key); // add to visited as a tiny optimization
			return;
		}
		// standard dfs with visited (black) and visiting (grey) sets
		if (visiting.add(key)) {
			for (Dependency dependency : binding.getDependencies()) {
				if (!visited.contains(dependency.getKey())) {
					dfs(bindings, visited, visiting, cycles, dependency.getKey());
				}
			}
			visiting.remove(key);
			visited.add(key);
			return;
		}

		// so at this point visiting set looks something like a -> b -> c -> d -> e -> g (-> c),
		// and in the code below we just get d -> e -> g -> c out of it

		Iterator<Key<?>> backtracked = visiting.iterator();
		int skipped = 0;

		// no .hasNext check since the the set must contain the key because above .add call returned false
		while (!backtracked.next().equals(key)) {
			skipped++;
		}
		Key<?>[] cycle = new Key[visiting.size() - skipped];
		for (int i = 0; i < cycle.length - 1; i++) {
			// no .hasNext check either because this happens exactly (size - previous .next calls - 1) times
			cycle[i] = backtracked.next();
		}
		// no key was added to the set because it already was there
		// and that one was consumed by the skipping while loop above
		// so we just add it manually at the end
		cycle[cycle.length - 1] = key;
		cycles.add(cycle);
	}
}

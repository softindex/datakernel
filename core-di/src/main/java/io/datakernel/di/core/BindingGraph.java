package io.datakernel.di.core;

import io.datakernel.di.util.Constructors.Factory;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.stream.Collectors.toSet;

public final class BindingGraph {
	public static final Factory<?> TO_BE_GENERATED = $ -> {
		throw new AssertionError("This binding exists as a marker to be replaced by a generated one, so if you see this message then somethning is really wrong");
	};

	private BindingGraph() {
		throw new AssertionError("nope.");
	}

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

		BindingProvider provider = new BindingProvider() {
			@Override
			@Nullable
			public <T> Binding<T> getBinding(Key<T> key) {
				Binding<T> binding = (Binding<T>) generated.get(key);
				if (binding == null) {
					binding = (Binding<T>) known.get(key);
				}
				if (binding != null && binding.getFactory() != TO_BE_GENERATED) {
					return binding;
				}

				binding = ((BindingGenerator<T>) generator).generate(this, scope, key);
				if (binding == null) {
					return null;
				}

				binding = ((BindingTransformer<T>) transformer).transform(this, scope, key, binding);

				generated.put(key, binding);

				// ensure that its dependencies are generated if nesessary
				for (Dependency dependency : binding.getDependencies()) {
					getBinding(dependency.getKey());
				}
				return binding;
			}
		};

		for (Entry<Key<?>, Binding<?>> entry : localBindings.entrySet()) {
			Key<Object> key = (Key<Object>) entry.getKey();
			Binding<Object> binding = (Binding<Object>) entry.getValue();

			if (binding.getFactory() == TO_BE_GENERATED) {
				Binding<Object> generatedBinding = provider.getBinding(key);
				if (generatedBinding == null) {
					// these bindings are the ones requested with plain `bind(...);` call, here we fail fast
					// see comment below where dependencies are generated
					throw new DIException("Refused to generate a requested binding for key " + key.getDisplayString());
				}
				generatedBinding.at(binding.getLocation()); // set its location to one from the generation request
				known.put(key, generatedBinding);
			} else {
				Binding<Object> transformed = ((BindingTransformer<Object>) transformer).transform(provider, scope, key, binding);
				if (transformed != binding) {
					localBindings.put(key, transformed);
				}
			}

			for (Dependency dependency : binding.getDependencies()) {
				Key<?> depKey = dependency.getKey();
				if (known.containsKey(depKey)) {
					continue;
				}
				known.put(depKey, provider.getBinding(depKey)); // put even nulls in known just as a little optimization
				// when generating dependencies we dont fail and just do nothing
				// unsatisfied dependency check will collect all of them and make a nice error
			}
		}
		localBindings.putAll(generated);
	}

	/**
	 * This method returns mapping from *unstatisfied keys* to *bindings that require them*
	 * and not the common *key and the bindings that provide it*
	 */
	public static Map<Key<?>, Set<Binding<?>>> getUnsatisfiedDependencies(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getUnsatisfiedDependencies(new HashSet<>(bindings.get().keySet()), bindings)
				.collect(toMultimap(dtb -> dtb.key, dtb -> dtb.binding));
	}

	private static Stream<DependencyToBinding> getUnsatisfiedDependencies(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return Stream.concat(
				bindings.get().values().stream()
						.flatMap(binding -> Arrays.stream(binding.getDependencies())
								.filter(dependency -> dependency.isRequired() && !known.contains(dependency.getKey()))
								.map(dependency -> new DependencyToBinding(dependency.getKey(), binding))),
				bindings.getChildren().values().stream().flatMap(scopeBindings -> getUnsatisfiedDependencies(union(known, scopeBindings.get().keySet()), scopeBindings))
		);
	}

	private static class DependencyToBinding {
		Key<?> key;
		Binding<?> binding;

		public DependencyToBinding(Key<?> key, Binding<?> binding) {
			this.key = key;
			this.binding = binding;
		}
	}

	public static Set<Key<?>[]> getCyclicDependencies(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getCyclicDependenciesStream(bindings).collect(toSet());
	}

	private static Stream<Key<?>[]> getCyclicDependenciesStream(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		// since no cycles are possible between scopes,
		// we just run simple dfs that ignores unsatisfied
		// dependencies for each scope independently
		return Stream.concat(
				dfs(bindings.get()).stream(),
				bindings.getChildren().values().stream().flatMap(BindingGraph::getCyclicDependenciesStream)
		);
	}

	private static Set<Key<?>[]> dfs(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		for (Key<?> key : bindings.keySet()) {
			if (visited.contains(key)) {
				continue;
			}
			dfs(bindings, visited, visiting, cycles, key);
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
	}
}

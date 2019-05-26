package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.util.Trie;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.di.util.Utils.flattenMultimap;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap();

	Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers();

	default Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		Function<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = getConflictResolvers()::get;
		return getBindingsMultimap().map(bindings -> flattenMultimap(bindings, conflictResolvers));
	}

	static Module ofBindings(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> multimap = bindings.map(
				map -> map.entrySet()
						.stream()
						.collect(toMap(Map.Entry::getKey, entry -> Collections.singleton(entry.getValue())))
		);
		return new Module() {
			@Override
			public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
				return multimap;
			}

			@Override
			public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
				return emptyMap();
			}
		};
	}
}

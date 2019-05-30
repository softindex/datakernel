package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.error.MultipleBindingsException;
import io.datakernel.di.error.NoBindingsForKey;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap();

	Map<Key<?>, BindingGenerator<?>> getBindingGenerators();

	Map<Key<?>, ConflictResolver<?>> getConflictResolvers();

	@SuppressWarnings("unchecked")
	default Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		Map<Key<?>, ConflictResolver<?>> resolvers = getConflictResolvers();
		return getBindingsMultimap().map(scopeBindings ->
				scopeBindings.entrySet().stream()
						.collect(toMap(
								Map.Entry::getKey,
								entry -> {
									Key<?> key = entry.getKey();
									Set<Binding<?>> bindings = entry.getValue();
									switch (bindings.size()) {
										case 0:
											throw new NoBindingsForKey(key);
										case 1:
											return bindings.iterator().next();
										default:
											ConflictResolver<?> resolver = resolvers.get(key);
											if (resolver == null) {
												throw new MultipleBindingsException(key, bindings);
											}
											// because Java generics are just broken :(
											return ((ConflictResolver) resolver).resolve(bindings);
									}
								})
						));
	}

	static Module empty() {
		return new Module() {
			private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(emptyMap());

			@Override
			public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
				return emptyTrie;
			}

			@Override
			public Map<Key<?>, BindingGenerator<?>> getBindingGenerators() {
				return emptyMap();
			}

			@Override
			public Map<Key<?>, ConflictResolver<?>> getConflictResolvers() {
				return emptyMap();
			}
		};
	}
}

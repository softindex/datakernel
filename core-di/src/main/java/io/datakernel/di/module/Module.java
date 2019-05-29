package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.LocationInfo;
import io.datakernel.di.Scope;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap();

	Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers();

	default Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> resolvers = getConflictResolvers();
		return getBindingsMultimap().map(bindings ->
				bindings.entrySet().stream()
						.collect(toMap(
								Map.Entry::getKey,
								entry -> {
									Key<?> key = entry.getKey();
									Set<Binding<?>> value = entry.getValue();
									switch (value.size()) {
										case 0:
											throw new IllegalStateException("Module " + getClass().getName() + " provided key " + key + " without bindings");
										case 1:
											return value.iterator().next();
										default: {
											Function<Set<Binding<?>>, Binding<?>> resolver = resolvers.get(key);
											if (resolver == null) {
												throw new IllegalStateException("Duplicate bindings for key " + key + ":\n" +
														entry.getValue().stream()
																.map(binding -> {
																	LocationInfo location = binding.getLocation();
																	if (location == null) {
																		return "at <unknown binding location>";
																	}
																	return "\tat " + location.getDeclaration();
																})
																.collect(joining("\n")) + "\n");
											}
											return resolver.apply(entry.getValue());
										}
									}
								})
						));
	}

	static Module empty() {
		return new Module() {
			@Override
			public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
				return Trie.leaf(emptyMap());
			}

			@Override
			public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
				return emptyMap();
			}
		};
	}
}

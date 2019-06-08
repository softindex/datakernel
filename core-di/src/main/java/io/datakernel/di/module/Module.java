package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.error.MultipleBindingsException;
import io.datakernel.di.error.NoBindingsForKeyException;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<@Nullable Binding<?>>>> getBindingsMultimap();

	Map<Integer, BindingTransformer<?>> getBindingTransformers();

	Map<Type, Set<BindingGenerator<?>>> getBindingGenerators();

	Map<Key<?>, Multibinder<?>> getMultibinders();

	@SuppressWarnings("unchecked")
	default Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		Map<Key<?>, Multibinder<?>> resolvers = getMultibinders();
		return getBindingsMultimap().map(scopeBindings ->
				scopeBindings.entrySet().stream()
						.collect(toMap(
								Map.Entry::getKey,
								entry -> {
									Key<?> key = entry.getKey();
									Set<Binding<?>> bindings = entry.getValue();

									// filter out null but not when binding set is [null]
									if (bindings.size() > 1) {
										bindings.removeIf(Objects::isNull);
									}

									switch (bindings.size()) {
										case 0:
											throw new NoBindingsForKeyException(key);
										case 1:
											return bindings.iterator().next();
										default:
											Multibinder<?> resolver = resolvers.get(key);
											if (resolver == null) {
												throw new MultipleBindingsException(key, bindings);
											}
											// because Java generics are just broken :(
											return ((Multibinder) resolver).resolve(bindings);
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
			public Map<Integer, BindingTransformer<?>> getBindingTransformers() {
				return emptyMap();
			}

			@Override
			public Map<Type, Set<BindingGenerator<?>>> getBindingGenerators() {
				return emptyMap();
			}

			@Override
			public Map<Key<?>, Multibinder<?>> getMultibinders() {
				return emptyMap();
			}
		};
	}
}

package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<@Nullable Binding<?>>>> getBindingsMultimap();

	Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers();

	Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators();

	Map<Key<?>, Multibinder<?>> getMultibinders();

	@SuppressWarnings("unchecked")
	default Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		Map<Key<?>, Multibinder<?>> multibinders = getMultibinders();
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
											throw new DIException("Provided a key " + key + " with no associated bindings");
										case 1:
											return bindings.iterator().next();
										default:
											Multibinder<?> multibinder = multibinders.get(key);
											if (multibinder == null) {
												throw new DIException(bindings.stream()
														.map(binding -> {
															LocationInfo location = binding.getLocation();
															if (location == null) {
																return "at <unknown binding location>";
															}
															return "\tat " + location.getDeclaration();
														})
														.collect(joining("\n", "for key " + key + ":\n", "\n")));
											}
											// because Java generics are just broken :(
											return ((Multibinder) multibinder).multibind(bindings);
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
			public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
				return emptyMap();
			}

			@Override
			public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
				return emptyMap();
			}

			@Override
			public Map<Key<?>, Multibinder<?>> getMultibinders() {
				return emptyMap();
			}
		};
	}
}

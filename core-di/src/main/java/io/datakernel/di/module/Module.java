package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;

import static io.datakernel.di.module.Multibinder.combinedMultibinder;
import static io.datakernel.di.util.Utils.resolve;
import static java.util.Collections.emptyMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings();

	Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers();

	Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators();

	Map<Key<?>, Multibinder<?>> getMultibinders();

	default Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return resolve(getBindings(), combinedMultibinder(getMultibinders()));
	}

	static Module empty() {
		return new Module() {
			private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(emptyMap());

			@Override
			public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
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

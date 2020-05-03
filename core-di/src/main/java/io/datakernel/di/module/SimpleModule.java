package io.datakernel.di.module;

import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.binding.BindingGenerator;
import io.datakernel.di.binding.BindingSet;
import io.datakernel.di.binding.BindingTransformer;
import io.datakernel.di.binding.Multibinder;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;

final class SimpleModule implements Module {
	private final Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings;
	private final Map<Integer, Set<BindingTransformer<?>>> transformers;
	private final Map<Class<?>, Set<BindingGenerator<?>>> generators;
	private final Map<Key<?>, Multibinder<?>> multibinders;

	public SimpleModule(Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		this.bindings = bindings;
		this.transformers = transformers;
		this.generators = generators;
		this.multibinders = multibinders;
	}

	@Override
	public Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings() {
		return bindings;
	}

	@Override
	public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		return transformers;
	}

	@Override
	public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		return generators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return multibinders;
	}
}

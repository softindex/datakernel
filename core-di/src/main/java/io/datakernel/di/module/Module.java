package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.di.module.Multibinder.combinedMultibinder;
import static io.datakernel.di.util.Utils.resolve;
import static java.util.Collections.emptyMap;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings();

	Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers();

	Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators();

	Map<Key<?>, Multibinder<?>> getMultibinders();

	default Module combineWith(Module another) {
		return Modules.combine(this, another);
	}

	default Module overrideWith(Module another) {
		return Modules.override(this, another);
	}

	default Module transformWith(Function<Module, Module> fn) {
		return fn.apply(this);
	}

	default Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return resolve(getBindings(), combinedMultibinder(getMultibinders()));
	}

	static Module empty() {
		return new Modules.ModuleImpl(Trie.leaf(emptyMap()), emptyMap(), emptyMap(), emptyMap());
	}
}

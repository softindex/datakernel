package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.di.core.Multibinder.combinedMultibinder;
import static java.util.Collections.emptyMap;

/**
 * A module is an object, that provides certain sets of bindings, transformers, generators or multibinders
 * arranged by keys in certain data structures.
 * @see AbstractModule
 */
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

	/**
	 * A shortcut that resolves conflicting bindings in this module using multibinders from this module
	 */
	default Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return Preprocessor.resolveConflicts(getBindings(), combinedMultibinder(getMultibinders()));
	}

	static Module empty() {
		return Modules.of(Trie.leaf(emptyMap()), emptyMap(), emptyMap(), emptyMap());
	}
}

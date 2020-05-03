package io.datakernel.di.module;

import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.binding.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.datakernel.di.binding.BindingGenerator.combinedGenerator;
import static io.datakernel.di.binding.BindingTransformer.combinedTransformer;
import static io.datakernel.di.binding.Multibinder.combinedMultibinder;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/**
 * A module is an object, that provides certain sets of bindings, transformers, generators or multibinders
 * arranged by keys in certain data structures.
 *
 * @see AbstractModule
 */
public interface Module {
	Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings();

	Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers();

	Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators();

	Map<Key<?>, Multibinder<?>> getMultibinders();

	default Module combineWith(Module another) {
		return Modules.combine(this, another);
	}

	default Module overrideWith(Module another) {
		return Modules.override(this, another);
	}

	default Module transformWith(UnaryOperator<Module> fn) {
		return fn.apply(this);
	}

	/**
	 * A shortcut that reduces bindings multimap trie from this module using multibinders, transformers and generators from this module.
	 * <p>
	 * Note that this method expensive to call repeatedly
	 */
	default Trie<Scope, Map<Key<?>, BindingInfo>> getReducedBindingInfo() {
		return Preprocessor.reduce(
				getBindings(),
				combinedMultibinder(getMultibinders()),
				combinedTransformer(getBindingTransformers()),
				combinedGenerator(getBindingGenerators())
		)
				.map(map -> map.entrySet().stream().collect(toMap(Entry::getKey, e -> BindingInfo.from(e.getValue()))));
	}

	/**
	 * Returns an empty {@link Module module}.
	 */
	static Module empty() {
		return Modules.EMPTY;
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie
	 */
	static Module of(Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings) {
		return new SimpleModule(bindings, emptyMap(), emptyMap(), emptyMap());
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie, transformers, generators and multibinders
	 */
	static Module of(Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		return new SimpleModule(bindings, transformers, generators, multibinders);
	}
}

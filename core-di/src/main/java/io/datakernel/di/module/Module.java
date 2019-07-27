package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.di.core.Multibinder.combinedMultibinder;
import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * A module is an object, that provides certain sets of bindings, transformers, generators or multibinders
 * arranged by keys in certain data structures.
 *
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

	default <T, V> Module rebind(Key<T> componentKey, Key<V> from, Key<? extends V> to) {
		return rebind((key, binding) -> componentKey.equals(key) ? binding.rebindDependency(from, to) : binding);
	}

	default <T> Module rebind(Key<T> componentKey, @NotNull Map<Key<?>, Key<?>> map) {
		return rebind((key, binding) -> componentKey.equals(key) ? binding.rebindDependencies(map) : binding);
	}

	default <V> Module rebind(Key<V> from, Key<? extends V> to) {
		return rebind((key, binding) -> binding.hasDependency(from) ? binding.rebindDependency(from, to) : binding);
	}

	default Module rebind(@NotNull Map<Key<?>, Key<?>> map) {
		return rebind((key, binding) ->
				binding.rebindDependencies(
						binding.getDependencies()
								.stream()
								.map(Dependency::getKey)
								.filter(map::containsKey)
								.collect(toMap(identity(), map::get))));
	}

	default <V> Module rebind(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return Modules.rebind(this, rebinder);
	}

	default Module export(Key<?>... keys) {
		return export(new HashSet<>(Arrays.asList(keys)));
	}

	default Module export(Set<Key<?>> keys) {
		return Modules.export(this, keys);
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

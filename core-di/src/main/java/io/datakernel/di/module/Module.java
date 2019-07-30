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
import static java.util.Collections.singletonMap;
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

	default Module export(Key<?>... keys) {
		return export(new HashSet<>(Arrays.asList(keys)));
	}

	default Module export(Set<Key<?>> keys) {
		return Modules.export(this, keys);
	}

	default <V> Module rebindExports(Key<V> from, Key<? extends V> to) {
		return rebindExports(singletonMap(from, to));
	}

	default Module rebindExports(@NotNull Map<Key<?>, Key<?>> map) {
		return Modules.rebindExports(this, map);
	}

	default <V> Module rebindImports(Key<V> from, Key<? extends V> to) {
		return rebindImports(singletonMap(from, to));
	}

	default Module rebindImports(@NotNull Map<Key<?>, Key<?>> map) {
		return Modules.rebindImports(this, (key, binding) ->
				binding.rebindDependencies(
						binding.getDependencies()
								.stream()
								.map(Dependency::getKey)
								.filter(map::containsKey)
								.collect(toMap(identity(), map::get))));
	}

	default <T, V> Module rebindImports(Key<T> componentKey, Key<V> from, Key<? extends V> to) {
		return rebindImports((key, binding) -> componentKey.equals(key) ? binding.rebindDependency(from, to) : binding);
	}

	default <T> Module rebindImports(Key<T> componentKey, @NotNull Map<Key<?>, Key<?>> map) {
		return rebindImports((key, binding) -> componentKey.equals(key) ? binding.rebindDependencies(map) : binding);
	}

	default <V> Module rebindImports(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return Modules.rebindImports(this, rebinder);
	}

	/**
	 * A shortcut that resolves conflicting bindings in this module using multibinders from this module
	 */
	default Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return Preprocessor.resolveConflicts(getBindings(), combinedMultibinder(getMultibinders()));
	}

	static Module ofDeclarativeBindingsFrom(Object module) {
		return new AbstractModule() {
			@Override
			protected void configure() {
				addDeclarativeBindingsFrom(module);
			}
		};
	}

	static Module ofDeclarativeBindingsFrom(Class<?> moduleClass) {
		return new AbstractModule() {
			@Override
			protected void configure() {
				addDeclarativeBindingsFrom(moduleClass);
			}
		};
	}

	static Module empty() {
		return Modules.of(Trie.leaf(emptyMap()), emptyMap(), emptyMap(), emptyMap());
	}
}

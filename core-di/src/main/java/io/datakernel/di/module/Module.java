package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.datakernel.di.core.BindingGenerator.combinedGenerator;
import static io.datakernel.di.core.BindingTransformer.combinedTransformer;
import static io.datakernel.di.core.Multibinder.combinedMultibinder;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

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

	default Module transformWith(UnaryOperator<Module> fn) {
		return fn.apply(this);
	}

	default Module export(Key<?> key, Key<?>... keys) {
		return export(Stream.concat(Stream.of(key), Arrays.stream(keys)).collect(toSet()));
	}

	default Module export(Set<Key<?>> keys) {
		return Modules.export(this, keys);
	}

	default <V> Module rebindExport(Class<V> from, Key<? extends V> to) {
		return rebindExport(Key.of(from), to);
	}

	default <V> Module rebindExport(Class<V> from, Class<? extends V> to) {
		return rebindExport(Key.of(from), Key.of(to));
	}

	default <V> Module rebindExport(Key<V> from, Class<? extends V> to) {
		return rebindExport(from, Key.of(to));
	}

	default <V> Module rebindExport(Key<V> from, Key<? extends V> to) {
		return rebindExports(singletonMap(from, to));
	}

	default <V> Module rebindImport(Key<V> from, Key<? extends V> to) {
		return rebindImportKeys(singletonMap(from, to));
	}

	default <V> Module rebindImport(Class<V> from, Binding<? extends V> binding) {
		return rebindImport(Key.of(from), binding);
	}

	default <V> Module rebindImport(Key<V> from, Binding<? extends V> binding) {
		return rebindImports(singletonMap(from, binding));
	}

	default Module rebindExports(@NotNull Map<Key<?>, Key<?>> map) {
		return Modules.rebindExports(this, map);
	}

	default Module rebindImports(@NotNull Map<Key<?>, Binding<?>> map) {
		return Modules.rebindImports(this, map);
	}

	default Module rebindImports(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return Modules.rebindImports(this, rebinder);
	}

	default Module rebindImportKeys(@NotNull Map<Key<?>, Key<?>> mapping) {
		return Modules.rebindImportKeys(this, mapping);
	}

	default <T, V> Module rebindImportDependencies(Key<T> key, Key<V> dependency, Key<? extends V> to) {
		return rebindImports((k, binding) -> k.equals(key) ? binding.rebindDependency(dependency, to) : binding);
	}

	default <T> Module rebindImportDependencies(Key<T> key, @NotNull Map<Key<?>, Key<?>> dependencyMapping) {
		return rebindImports((k, binding) -> key.equals(k) ? binding.rebindDependencies(dependencyMapping) : binding);
	}

	/**
	 * A shortcut that reduces bindings multimap trie from this module using multibinders, transformers and generators from this module
	 */
	default Trie<Scope, Map<Key<?>, Binding<?>>> getReducedBindings() {
		return Preprocessor.reduce(getBindings(),
				combinedMultibinder(getMultibinders()),
				combinedTransformer(getBindingTransformers()),
				combinedGenerator(getBindingGenerators()));
	}

	/**
	 * Returns an empty {@link Module module}.
	 */
	static Module empty() {
		return Modules.EMPTY;
	}

	static ModuleBuilder create() {
		return new ModuleBuilderImpl<>();
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie
	 */
	static Module of(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings) {
		return new SimpleModule(bindings, emptyMap(), emptyMap(), emptyMap());
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie, transformers, generators and multibinders
	 */
	static Module of(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		return new SimpleModule(bindings, transformers, generators, multibinders);
	}
}

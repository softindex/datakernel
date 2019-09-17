package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

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

	default Module transformWith(UnaryOperator<Module> fn) {
		return fn.apply(this);
	}

	default Module export(Key<?>... keys) {
		return export(new HashSet<>(Arrays.asList(keys)));
	}

	default Module export(Set<Key<?>> keys) {
		return Modules.export(this, keys);
	}

	default <V> Module rebindExport(Key<V> from, Key<? extends V> to) {
		return rebindExports(singletonMap(from, to));
	}

	default Module rebindExports(@NotNull Map<Key<?>, Key<?>> map) {
		return Modules.rebindExports(this, map);
	}

	default <V> Module rebindImport(Key<V> from, Key<? extends V> to) {
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

	default Module rebindImports(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return Modules.rebindImports(this, rebinder);
	}

	/**
	 * A shortcut that resolves conflicting bindings in this module using multibinders from this module
	 */
	default Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return Preprocessor.resolveConflicts(getBindings(), combinedMultibinder(getMultibinders()));
	}

	/**
	 * Returns an empty {@link Module module}.
	 */
	static Module empty() {
		return Modules.EMPTY;
	}

	static BuilderModuleBindingStage create() {
		return new BuilderModule<>();
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie
	 */
	static Module of(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return new SimpleModule(bindings.map(Utils::toMultimap), emptyMap(), emptyMap(), emptyMap());
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

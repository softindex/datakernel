package io.datakernel.di.module;

import io.datakernel.di.annotation.EagerSingleton;
import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.core.*;
import io.datakernel.di.util.Constructors.*;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.function.Supplier;

public interface ModuleBuilderBinder<T> extends ModuleBuilder {
	/**
	 * If bound key does not have a name already then sets it to a given one
	 */
	ModuleBuilderBinder<T> named(@NotNull Name name);

	/**
	 * @see #named(Name)
	 */
	default ModuleBuilderBinder<T> named(@NotNull String name) {
		return named(Name.of(name));
	}

	/**
	 * @see #named(Name)
	 */
	default ModuleBuilderBinder<T> annotatedWith(@NotNull Annotation annotation) {
		return named(Name.of(annotation));
	}

	/**
	 * @see #named(Name)
	 */
	default ModuleBuilderBinder<T> annotatedWith(@NotNull Class<? extends Annotation> annotationType) {
		return named(Name.of(annotationType));
	}

	/**
	 * The binding being built by this builder will be added to the binding graph trie at given scope path
	 */
	ModuleBuilderBinder<T> in(@NotNull Scope[] scope);

	/**
	 * @see #in(Scope[])
	 */
	ModuleBuilderBinder<T> in(@NotNull Scope scope, @NotNull Scope... scopes);

	/**
	 * @see #in(Scope[])
	 */
	ModuleBuilderBinder<T> in(@NotNull Class<? extends Annotation> annotationClass, @NotNull Class<?>... annotationClasses);

	/**
	 * Sets a binding which would be bound to a given key and added to the binding graph trie
	 */
	ModuleBuilderBinder<T> to(@NotNull Binding<? extends T> binding);

	/**
	 * DSL shortcut for creating a binding that just calls a binding at given key
	 * and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> to(@NotNull Key<? extends T> implementation) {
		return to(Binding.to(implementation));
	}

	/**
	 * @see #to(Key)
	 */
	default ModuleBuilderBinder<T> to(@NotNull Class<? extends T> implementation) {
		return to(Binding.to(implementation));
	}

	/**
	 * DSL shortcut for creating a binding from a given instance
	 * and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> toInstance(@NotNull T instance) {
		return to(Binding.toInstance(instance));
	}

	/**
	 * DSL shortcut for creating a binding that calls a supplier from binding at given key
	 * and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> toSupplier(@NotNull Key<? extends Supplier<? extends T>> supplierKey) {
		return to(Binding.toSupplier(supplierKey));
	}

	/**
	 * @see #toSupplier(Key)
	 */
	default ModuleBuilderBinder<T> toSupplier(@NotNull Class<? extends Supplier<? extends T>> supplierType) {
		return to(Binding.toSupplier(supplierType));
	}

	// region public BuilderModule<T> to(constructor*, dependencies...) { ... }

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Class<?>[] dependencies) {
		return to(Binding.to(factory, dependencies));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Key<?>[] dependencies) {
		return to(Binding.to(factory, dependencies));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Dependency[] dependencies) {
		return to(Binding.to(factory, dependencies));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default ModuleBuilderBinder<T> to(@NotNull Constructor0<? extends T> constructor) {
		return to(Binding.to(constructor));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1> ModuleBuilderBinder<T> to(@NotNull Constructor1<T1, ? extends T> constructor,
			@NotNull Class<T1> dependency1) {
		return to(Binding.to(constructor, dependency1));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2> ModuleBuilderBinder<T> to(@NotNull Constructor2<T1, T2, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2) {
		return to(Binding.to(constructor, dependency1, dependency2));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3> ModuleBuilderBinder<T> to(@NotNull Constructor3<T1, T2, T3, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3, T4> ModuleBuilderBinder<T> to(@NotNull Constructor4<T1, T2, T3, T4, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3, T4, T5> ModuleBuilderBinder<T> to(@NotNull Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3, T4, T5, T6> ModuleBuilderBinder<T> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5, @NotNull Class<T6> dependency6) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1> ModuleBuilderBinder<T> to(@NotNull Constructor1<T1, ? extends T> constructor,
			@NotNull Key<T1> dependency1) {
		return to(Binding.to(constructor, dependency1));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2> ModuleBuilderBinder<T> to(@NotNull Constructor2<T1, T2, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
		return to(Binding.to(constructor, dependency1, dependency2));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3> ModuleBuilderBinder<T> to(@NotNull Constructor3<T1, T2, T3, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3, T4> ModuleBuilderBinder<T> to(@NotNull Constructor4<T1, T2, T3, T4, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3, T4, T5> ModuleBuilderBinder<T> to(@NotNull Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
	}

	/**
	 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
	 */
	default <T1, T2, T3, T4, T5, T6> ModuleBuilderBinder<T> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
		return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
	}

	/**
	 * Adds bound key to a key set named with given name.
	 * <p>
	 * A key set is a special key of type Set&lt;Key&lt;?&gt;&gt; and name with annotation that is
	 * marked with {@link KeySetAnnotation} instead of {@link NameAnnotation}
	 *
	 * @see KeySetAnnotation
	 */
	ModuleBuilderBinder<T> as(@NotNull Name name);

	/**
	 * @see #as(Name)
	 */
	default ModuleBuilderBinder<T> as(@NotNull Class<? extends Annotation> annotationType) {
		return as(Name.of(annotationType));
	}

	/**
	 * @see #as(Name)
	 */
	default ModuleBuilderBinder<T> as(@NotNull Annotation annotation) {
		return as(Name.of(annotation));
	}

	/**
	 * @see #as(Name)
	 */
	default ModuleBuilderBinder<T> as(@NotNull String name) {
		return as(Name.of(name));
	}

	/**
	 * A shortcut for <code>as({@link EagerSingleton}.class)</code>
	 */
	default ModuleBuilderBinder<T> asEagerSingleton() {
		return as(EagerSingleton.class);
	}

	/**
	 * Adds given dependencies to the underlying binding
	 */
	ModuleBuilderBinder<T> withExtraDependencies(Set<Dependency> dependencies);

	/**
	 * @see #withExtraDependencies(Set)
	 */
	ModuleBuilderBinder<T> withExtraDependencies(Dependency... dependencies);

	/**
	 * @see #withExtraDependencies(Set)
	 */
	ModuleBuilderBinder<T> withExtraDependencies(Key<?>... dependencies);

	/**
	 * @see #withExtraDependencies(Set)
	 */
	ModuleBuilderBinder<T> withExtraDependencies(Class<?>... dependencies);

	ModuleBuilderBinder<T> export();
}

package io.datakernel.di.module;

import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.*;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to restrict the DSL.
 * Basically, it disallows any methods from {@link ModuleBuilderBinder} not listed below
 * to be called without previously calling {@link #bind bind(...)}.
 */
@SuppressWarnings("UnusedReturnValue")
public interface ModuleBuilder extends Module {
	/**
	 * Adds all bindings, transformers, generators and multibinders from given modules to this one.
	 * <p>
	 * This works just as if you'd define all of those directly in this module.
	 */
	ModuleBuilder install(Collection<Module> modules);

	/**
	 * @see #install(Collection)
	 */
	default ModuleBuilder install(Module... modules) {
		return install(Arrays.asList(modules));
	}

	/**
	 * Scans class hierarchy and then installs providers from each class as modules,
	 * so that exports do not interfere between classes.
	 * Class parameter is used to specify from which class in the hierarchy to start.
	 */
	ModuleBuilder scan(@NotNull Class<?> containerClass, @Nullable Object container);

	/**
	 * Same as {@link #scan}, with staring class defaulting to the class of the object instance.
	 */
	default ModuleBuilder scan(Object container) {
		return scan(container.getClass(), container);
	}

	/**
	 * Same as {@link #scan}, but scans only static methods and does not depend on instance of the class.
	 * Non-static annotated methods are {@link IllegalStateException prohibited}.
	 */
	default ModuleBuilder scan(Class<?> container) {
		return scan(container, null);
	}

	/**
	 * @see #bind(Key)
	 */
	default <T> ModuleBuilderBinder<T> bind(Class<T> cls) {
		return bind(Key.of(cls));
	}

	/**
	 * @see #bind(Key)
	 */
	default <T> ModuleBuilderBinder<T> bind(Class<T> cls, Name name) {
		return bind(Key.of(cls, name));
	}

	/**
	 * @see #bind(Key)
	 */
	default <T> ModuleBuilderBinder<T> bind(Class<T> cls, String name) {
		return bind(Key.of(cls, name));
	}

	/**
	 * @see #bind(Key)
	 */
	default <T> ModuleBuilderBinder<T> bind(Class<T> cls, Class<? extends Annotation> annotationType) {
		return bind(Key.of(cls, annotationType));
	}

	/**
	 * This method begins a chain of binding builder DSL calls.
	 * <p>
	 * You can use generics in it, only those that are defined at the module class.
	 * And you need to subclass the module at the usage point to 'bake' those generics
	 * into subclass bytecode so that they could be fetched by this bind call.
	 */
	<T> ModuleBuilderBinder<T> bind(@NotNull Key<T> key);

	default <T> ModuleBuilder bindInstanceProvider(Class<T> type) {
		return bindInstanceProvider(Key.of(type));
	}

	default <T> ModuleBuilder bindInstanceProvider(Key<T> key) {
		return bind(Key.ofType(Types.parameterized(InstanceProvider.class, key.getType()), key.getName()));
	}

	default <T> ModuleBuilder bindInstanceInjector(Class<T> type) {
		return bindInstanceInjector(Key.of(type));
	}

	default <T> ModuleBuilder bindInstanceInjector(Key<T> key) {
		return bind(Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()));
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinder#toSet multibinds} each of such sets together.
	 */
	<S, E extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Binding<E> binding);

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#to(Key)
	 */
	default <S, E extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Key<E> item) {
		return bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * @see #postInjectInto(Key)
	 */
	default ModuleBuilder postInjectInto(Class<?> type) {
		return postInjectInto(Key.of(type));
	}

	/**
	 * {@link #bindIntoSet(Key, Key) Binds into set} a key of instance injector for given type at a {@link Injector#postInjectInstances special}
	 * key Set&lt;InstanceInjector&lt;?&gt;&gt;.
	 * <p>
	 * Instance injector bindings are {@link DefaultModule generated automatically}.
	 *
	 * @see Injector#postInjectInstances
	 */
	default ModuleBuilder postInjectInto(Key<?> key) {
		Key<InstanceInjector<?>> instanceInjectorKey = Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName());
		bind(instanceInjectorKey); // so that its location is set to this module
		return bindIntoSet(new Key<InstanceInjector<?>>() {}, instanceInjectorKey);
	}

	/**
	 * Adds a {@link BindingTransformer transformer} with a given priority to this module.
	 */
	<E> ModuleBuilder transform(int priority, BindingTransformer<E> bindingTransformer);

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 */
	<E> ModuleBuilder generate(Class<?> pattern, BindingGenerator<E> bindingGenerator);

	/**
	 * Adds a {@link Multibinder multibinder} for a given key to this module.
	 */
	<E> ModuleBuilder multibind(Key<E> key, Multibinder<E> multibinder);

	default <V> ModuleBuilder multibindToSet(Class<V> type) {
		return multibindToSet(Key.of(type));
	}

	default <V> ModuleBuilder multibindToSet(Class<V> type, String name) {
		return multibindToSet(Key.of(type, name));
	}

	default <V> ModuleBuilder multibindToSet(Class<V> type, Name name) {
		return multibindToSet(Key.of(type, name));
	}

	default <V> ModuleBuilder multibindToSet(Key<V> key) {
		return multibind(Key.ofType(Types.parameterized(Set.class, key.getType()), key.getName()), Multibinder.toSet());
	}

	default <K, V> ModuleBuilder multibindToMap(Class<K> keyType, Class<V> valueType) {
		return multibindToMap(keyType, valueType, (Name) null);
	}

	default <K, V> ModuleBuilder multibindToMap(Class<K> keyType, Class<V> valueType, String name) {
		return multibindToMap(keyType, valueType, Name.of(name));
	}

	default <K, V> ModuleBuilder multibindToMap(Class<K> keyType, Class<V> valueType, Name name) {
		return multibind(Key.ofType(Types.parameterized(Map.class, keyType, valueType), name), Multibinder.toMap());
	}
}

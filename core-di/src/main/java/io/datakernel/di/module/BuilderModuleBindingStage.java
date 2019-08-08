package io.datakernel.di.module;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.*;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/**
 * This interface is used to restrict the DSL.
 * Basically, it disallows any methods from {@link BuilderModule} not listed below
 * to be called without previously calling {@link #bind bind(...)}.
 */
@SuppressWarnings("UnusedReturnValue")
public interface BuilderModuleBindingStage extends Module {

	/**
	 * This method begins a chain of binding builder DSL calls.
	 * <p>
	 * You can use generics in it, only those that are defined at the module class.
	 * And you need to subclass the module at the usage point to 'bake' those generics
	 * into subclass bytecode so that they could be fetched by this bind call.
	 */
	<T> BuilderModule<T> bind(Key<T> key);

	/**
	 * @see #bind(Key)
	 */
	<T> BuilderModule<T> bind(Class<T> cls);

	default <T> BuilderModuleBindingStage bindInstanceProvider(Class<T> type) {
		return bind(Key.of(type));
	}

	default <T> BuilderModuleBindingStage bindInstanceProvider(Key<T> key) {
		return bind(Key.ofType(Types.parameterized(InstanceProvider.class, key.getType()), key.getName()));
	}

	default <T> BuilderModuleBindingStage bindInstanceFactory(Class<T> type) {
		return bind(Key.of(type));
	}

	default <T> BuilderModuleBindingStage bindInstanceFactory(Key<T> key) {
		return bind(Key.ofType(Types.parameterized(InstanceFactory.class, key.getType()), key.getName()));
	}

	default <T> BuilderModuleBindingStage bindInstanceInjector(Class<T> type) {
		return bind(Key.of(type));
	}

	default <T> BuilderModuleBindingStage bindInstanceInjector(Key<T> key) {
		return bind(Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()));
	}

	/**
	 * Adds all bindings, transformers, generators and multibinders from given modules to this one.
	 * <p>
	 * This works just as if you'd define all of those directly in this module.
	 */
	BuilderModuleBindingStage install(Collection<Module> modules);

	/**
	 * @see #install(Collection)
	 */
	BuilderModuleBindingStage install(Module... modules);

	/**
	 * Scans given object for provider methods (ones annotated with {@link Provides @Provides} annotation)
	 * and adds them as bindings (or generators, in case of generic template methods) of this module.
	 * <p>
	 * It scans both static and non-static methods.
	 */
	BuilderModuleBindingStage scan(Object container);

	/**
	 * Same as {@link #scan}, but scans only static methods and does not depend on instance of the class.
	 * Non-static annotated methods are {@link IllegalStateException prohibited}.
	 */
	BuilderModuleBindingStage scanStatics(Class<?> container);

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinder#toSet multibinds} each of such sets together.
	 */
	<S, E extends S> BuilderModuleBindingStage bindIntoSet(Key<S> setOf, Binding<E> binding);

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#to(Key)
	 */
	<S, E extends S> BuilderModuleBindingStage bindIntoSet(Key<S> setOf, Key<E> item);

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#toInstance(Object)
	 */
	<S, E extends S> BuilderModuleBindingStage bindIntoSet(@NotNull Key<S> setOf, @NotNull E element);

	/**
	 * {@link #bindIntoSet(Key, Key) Binds into set} a key of instance injector for given type at a {@link Injector#postInjectInstances special}
	 * key Set&lt;InstanceInjector&lt;?&gt;&gt;.
	 * <p>
	 * Instance injector bindings are {@link DefaultModule generated automatically}.
	 *
	 * @see Injector#postInjectInstances
	 */
	BuilderModuleBindingStage postInjectInto(Key<?> key);

	/**
	 * @see #postInjectInto(Key)
	 */
	BuilderModuleBindingStage postInjectInto(Class<?> type);

	/**
	 * Adds a {@link BindingTransformer transformer} with a given priority to this module.
	 */
	<E> BuilderModuleBindingStage transform(int priority, BindingTransformer<E> bindingTransformer);

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 */
	<E> BuilderModuleBindingStage generate(Class<?> pattern, BindingGenerator<E> bindingGenerator);

	/**
	 * Adds a {@link Multibinder multibinder} for a given key to this module.
	 */
	<E> BuilderModuleBindingStage multibind(Key<E> key, Multibinder<E> multibinder);

	default <V> BuilderModuleBindingStage multibindToSet(Class<V> type) {
		return multibindToSet(Key.of(type));
	}

	default <V> BuilderModuleBindingStage multibindToSet(Class<V> type, String name) {
		return multibindToSet(Key.of(type, name));
	}

	default <V> BuilderModuleBindingStage multibindToSet(Class<V> type, Name name) {
		return multibindToSet(Key.of(type, name));
	}

	default <V> BuilderModuleBindingStage multibindToSet(Key<V> key) {
		return multibind(Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()), Multibinder.toSet());
	}

	default <K, V> BuilderModuleBindingStage multibindToMap(Class<K> keyType, Class<V> valueType) {
		return multibindToMap(keyType, valueType, (Name) null);
	}

	default <K, V> BuilderModuleBindingStage multibindToMap(Class<K> keyType, Class<V> valueType, String name) {
		return multibindToMap(keyType, valueType, Name.of(name));
	}

	default <K, V> BuilderModuleBindingStage multibindToMap(Class<K> keyType, Class<V> valueType, Name name) {
		return multibind(Key.ofType(Types.parameterized(Map.class, keyType, valueType), name), Multibinder.toMap());
	}

	@Override
	default BuilderModuleBindingStage transformWith(UnaryOperator<Module> fn) {
		return new BuilderModule<>().install(Module.super.transformWith(fn));
	}

	@Override
	default BuilderModuleBindingStage combineWith(Module another) {
		return install(another);
	}

	// region override default module methods with return type changed to this so that DSL call-chain could continue even further

	@Override
	default BuilderModuleBindingStage overrideWith(Module another) {
		return new BuilderModule<>().install(Module.super.overrideWith(another));
	}

	@Override
	default BuilderModuleBindingStage export(Key<?>... keys) {
		return new BuilderModule<>().install(Module.super.export(keys));
	}

	@Override
	default BuilderModuleBindingStage export(Set<Key<?>> keys) {
		return new BuilderModule<>().install(Module.super.export(keys));
	}

	@Override
	default <V> BuilderModuleBindingStage rebindExport(Key<V> from, Key<? extends V> to) {
		return new BuilderModule<>().install(Module.super.rebindExport(from, to));
	}

	@Override
	default BuilderModuleBindingStage rebindExports(@NotNull Map<Key<?>, Key<?>> map) {
		return new BuilderModule<>().install(Module.super.rebindExports(map));
	}

	@Override
	default <V> BuilderModuleBindingStage rebindImport(Key<V> from, Key<? extends V> to) {
		return new BuilderModule<>().install(Module.super.rebindImport(from, to));
	}

	@Override
	default BuilderModuleBindingStage rebindImports(@NotNull Map<Key<?>, Key<?>> map) {
		return new BuilderModule<>().install(Module.super.rebindImports(map));
	}

	@Override
	default <T, V> BuilderModuleBindingStage rebindImports(Key<T> componentKey, Key<V> from, Key<? extends V> to) {
		return new BuilderModule<>().install(Module.super.rebindImports(componentKey, from, to));
	}

	@Override
	default <T> BuilderModuleBindingStage rebindImports(Key<T> componentKey, @NotNull Map<Key<?>, Key<?>> map) {
		return new BuilderModule<>().install(Module.super.rebindImports(componentKey, map));
	}

	@Override
	default BuilderModuleBindingStage rebindImports(BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return new BuilderModule<>().install(Module.super.rebindImports(rebinder));
	}

	// endregion
}

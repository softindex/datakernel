package io.datakernel.di.module;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

import static io.datakernel.di.util.ReflectionUtils.generateInjectingInitializer;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;

/**
 * This module provides a set of default generators.
 * <p>
 * The first one tries to generate a binding for any missing key by searching for {@link Inject} constructors.
 * <p>
 * The second one generates any Key&lt;SomeType&gt; instance for SomeType.
 * Its purpose is to get reified types from generics in templated providers.
 * <p>
 * The last three generate appropriate instances for {@link InstanceProvider}, {@link InstanceFactory} and {@link InstanceInjector} requests.
 */
public final class DefaultModule implements Module {

	private static final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(new HashMap<>());

	private static final Map<Class<?>, Set<BindingGenerator<?>>> generators = new HashMap<>();

	static {
		// generating bindings for classes that have @Inject constructors/factory methods
		generators.put(Object.class, singleton((bindings, scope, key) -> ReflectionUtils.generateImplicitBinding(key)));

		// generating dummy bindings for reified type requests (can be used in templated providers to get a Key<T> instance)
		generators.put(Key.class, singleton((bindings, scope, key) -> Binding.toInstance(key.getTypeParameter(0))));

		// generating bindings for provider requests
		generators.put(InstanceProvider.class, singleton(
				(bindings, scope, key) -> {
					Key<Object> instanceKey = key.getTypeParameter(0).named(key.getName());
					Binding<Object> instanceBinding = bindings.locate(instanceKey);
					if (instanceBinding == null) {
						return null;
					}
					return new Binding<>(
							instanceBinding.getDependencies(),
							(compiledBindings, level, index) ->
									new AbstractCompiledBinding<Object>(level, index) {
										final CompiledBinding<Object> instanceCompiledBinding = compiledBindings.locate(instanceKey);

										@Override
										public InstanceProvider<Object> createInstance(AtomicReferenceArray[] instances) {
											return new InstanceProvider<Object>() {
												@Override
												public Key<Object> key() {
													return instanceKey;
												}

												@Override
												public Object get() {
													return instanceCompiledBinding.getInstance(instances);
												}

												@Override
												public String toString() {
													return "factory of " + instanceKey.toString();
												}
											};
										}
									});
				}
		));

		// generating bindings for factory requests
		generators.put(InstanceFactory.class, singleton(
				(bindings, scope, key) -> {
					Key<Object> instanceKey = key.getTypeParameter(0).named(key.getName());
					Binding<Object> instanceBinding = bindings.locate(instanceKey);
					if (instanceBinding == null) {
						return (Binding<Object>) null;
					}
					return new Binding<>(
							instanceBinding.getDependencies(),
							(compiledBindings, level, index) ->
									new AbstractCompiledBinding<Object>(level, index) {
										final CompiledBinding<Object> instanceCompiledBinding = compiledBindings.locate(instanceKey);

										@Override
										public InstanceFactory<Object> createInstance(AtomicReferenceArray[] instances) {
											return new InstanceFactory<Object>() {
												@Override
												public Key<Object> key() {
													return instanceKey;
												}

												@Override
												public Object create() {
													return instanceCompiledBinding.createInstance(instances);
												}

												@Override
												public String toString() {
													return "factory of " + instanceKey.toString();
												}
											};
										}
									});
				}
		));

		// generating bindings for injector requests
		generators.put(InstanceInjector.class, singleton(
				(bindings, scope, key) -> {
					Key<Object> instanceKey = key.getTypeParameter(0).named(key.getName());
					BindingInitializer<Object> bindingInitializer = generateInjectingInitializer(instanceKey);
					return new Binding<>(
							bindingInitializer.getDependencies(),
							(compiledBindings, level, index) ->
									new AbstractCompiledBinding<Object>(level, index) {
										final BiConsumer<AtomicReferenceArray[], Object> consumer = bindingInitializer.getCompiler().compile(compiledBindings);

										@Override
										public Object createInstance(AtomicReferenceArray[] instances) {
											return new InstanceInjector<Object>() {
												@Override
												public Key<Object> key() {
													return instanceKey;
												}

												@Override
												public void injectInto(Object existingInstance) {
													consumer.accept(instances, existingInstance);
												}

												@Override
												public String toString() {
													return "injector for " + instanceKey.toString();
												}
											};
										}
									}
					);
				}
		));
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
		return emptyTrie;
	}

	@Override
	public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		return emptyMap();
	}

	@Override
	public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		return generators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return emptyMap();
	}
}

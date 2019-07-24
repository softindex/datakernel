package io.datakernel.di.module;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.*;
import io.datakernel.di.impl.AbstractCompiledBinding;
import io.datakernel.di.impl.BindingInitializer;
import io.datakernel.di.impl.CompiledBinding;
import io.datakernel.di.impl.CompiledBindingInitializer;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
							(compiledBindings, synchronizedScope, index) ->
									new AbstractCompiledBinding<Object>(synchronizedScope, index) {
										@Override
										public InstanceProvider<Object> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											return new InstanceProvider<Object>() {
												final CompiledBinding<Object> compiledBinding = compiledBindings.locate(instanceKey);

												@Override
												public Key<Object> key() {
													return instanceKey;
												}

												@Override
												public Object get() {
													return compiledBinding.getInstance(scopedInstances, synchronizedScope);
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
							(compiledBindings, synchronizedScope, index) ->
									new AbstractCompiledBinding<Object>(synchronizedScope, index) {
										@Override
										protected InstanceFactory<Object> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											return new InstanceFactory<Object>() {
												final CompiledBinding<Object> compiledBinding = compiledBindings.locate(instanceKey);

												@Override
												public Key<Object> key() {
													return instanceKey;
												}

												@Override
												public Object create() {
													return compiledBinding.createInstance(scopedInstances, synchronizedScope);
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
							(compiledBindings, synchronizedScope, index) ->
									new AbstractCompiledBinding<Object>(synchronizedScope, index) {
										@Override
										public Object doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											return new InstanceInjector<Object>() {
												final CompiledBindingInitializer<Object> compiledBindingInitializer = bindingInitializer.getCompiler().compile(compiledBindings);

												@Override
												public Key<Object> key() {
													return instanceKey;
												}

												@Override
												public void injectInto(Object existingInstance) {
													compiledBindingInitializer.initInstance(existingInstance, scopedInstances, synchronizedScope);
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

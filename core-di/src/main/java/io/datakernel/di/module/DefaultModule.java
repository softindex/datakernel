package io.datakernel.di.module;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
		generators.put(Object.class, singleton((provider, scope, key) -> ReflectionUtils.generateImplicitBinding(key)));

		// generating dummy bindings for reified type requests (can be used in templated providers to get a Key<T> instance)
		generators.put(Key.class, singleton((provider, scope, key) -> Binding.toInstance(key.getTypeParameter(0))));

		// generating bindings for provider requests
		generators.put(InstanceProvider.class, singleton(
				(provider, scope, key) -> {
					Key<Object> elementKey = key.getTypeParameter(0).named(key.getName());
					Binding<Object> elementBinding = provider.getBinding(elementKey);
					if (elementBinding == null) {
						return null;
					}
					return Binding.to(
							args -> {
								Injector injector = (Injector) args[0];
								return new InstanceProvider<Object>() {
									@Override
									public Key<Object> key() {
										return elementKey;
									}

									@Override
									public Object get() {
										return injector.getInstance(elementKey);
									}
								};
							},
							new Dependency[]{Dependency.toKey(Key.of(Injector.class))});
				}
		));

		// generating bindings for factory requests
		generators.put(InstanceFactory.class, singleton(
				(provider, scope, key) -> {
					Key<Object> elementKey = key.getTypeParameter(0).named(key.getName());
					Binding<Object> elementBinding = provider.getBinding(elementKey);
					if (elementBinding == null) {
						return null;
					}
					return new Binding<>(
							elementBinding.getDependencies(),
							locator -> new InstanceFactory<Object>() {
								@Override
								public Key<Object> key() {
									return elementKey;
								}

								@Override
								public Object create() {
									return elementBinding.getFactory().create(locator);
								}

								@Override
								public String toString() {
									return elementKey.toString();
								}
							});
				}
		));

		// generating bindings for injector requests
		generators.put(InstanceInjector.class, singleton(
				(provider, scope, key) -> {
					Key<Object> elementKey = key.getTypeParameter(0).named(key.getName());

					BindingInitializer<Object> injectingInitializer = generateInjectingInitializer(elementKey.getType());
					BindingInitializer.Initializer<Object> initializer = injectingInitializer.getInitializer();

					return Binding.to(
							args -> new InstanceInjector<Object>() {
								@Override
								public Key<Object> key() {
									return elementKey;
								}

								@Override
								public void injectInto(Object existingInstance) {
									initializer.apply(existingInstance, args);
								}

								@Override
								public String toString() {
									return elementKey.toString();
								}
							},
							injectingInitializer.getDependencies());
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

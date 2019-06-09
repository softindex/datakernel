package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.di.util.ReflectionUtils.generateInjectingInitializer;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;

public final class DefaultModule implements Module {

	private static final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(new HashMap<>());

	private static final Map<Type, Set<BindingGenerator<?>>> generators = new HashMap<>();
	private static final Map<Integer, BindingTransformer<?>> transformers = new HashMap<>();

	static {
		// generating bindings for classes that have @Inject constructors/factory methods
		generators.put(Object.class, singleton((scope, key, provider) -> ReflectionUtils.generateImplicitBinding(key)));

		// generating bindings for provider requests
		generators.put(new Key<InstanceProvider<?>>() {}.getType(), singleton(
				(scope, key, provider) -> {
					Key<Object> elementKey = Key.ofType(key.getTypeParams()[0], key.getName());
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
							new Dependency[]{new Dependency(Key.of(Injector.class), true)});
				}
		));

		// generating bindings for factory requests
		generators.put(new Key<InstanceFactory<?>>() {}.getType(), singleton(
				(scope, key, provider) -> {
					Key<Object> elementKey = Key.ofType(key.getTypeParams()[0], key.getName());
					Binding<Object> elementBinding = provider.getBinding(elementKey);
					if (elementBinding == null) {
						return null;
					}
					return Binding.to(
							args -> new InstanceFactory<Object>() {
								@Override
								public Key<Object> key() {
									return elementKey;
								}

								@Override
								public Object create() {
									return elementBinding.getFactory().create(args);
								}
							},
							elementBinding.getDependencies());
				}
		));

		// generating bindings for injector requests
		generators.put(new Key<InstanceInjector<?>>() {}.getType(), singleton(
				(scope, key, provider) -> {
					Key<Object> elementKey = Key.ofType(key.getTypeParams()[0], key.getName());

					BindingInitializer<Object> injectingInitializer = generateInjectingInitializer(elementKey);
					BindingInitializer.Initializer<Object> initializer = injectingInitializer.getInitializer();

					return Binding.to(
							args -> new InstanceInjector<Object>() {
								@Override
								public Key<Object> key() {
									return elementKey;
								}

								@Override
								public void inject(Object existingInstance) {
									initializer.apply(existingInstance, args);
								}
							},
							injectingInitializer.getDependencies());
				}
		));
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
		return emptyTrie;
	}

	@Override
	public Map<Integer, BindingTransformer<?>> getBindingTransformers() {
		return transformers;
	}

	@Override
	public Map<Type, Set<BindingGenerator<?>>> getBindingGenerators() {
		return generators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return emptyMap();
	}
}

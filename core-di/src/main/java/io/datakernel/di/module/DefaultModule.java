package io.datakernel.di.module;

import io.datakernel.di.*;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.di.util.ReflectionUtils.bindingForInstanceInjector;
import static io.datakernel.di.util.ReflectionUtils.bindingForInstanceProvider;
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
					//noinspection unchecked
					return (Binding) bindingForInstanceProvider(elementKey, elementBinding);
				}
		));

		generators.put(new Key<InstanceInjector<?>>() {}.getType(), singleton(
				(scope, key, provider) -> {
					Key<Object> elementKey = Key.ofType(key.getTypeParams()[0], key.getName());
					//noinspection unchecked
					return (Binding) bindingForInstanceInjector(ReflectionUtils.generateBindingInitializer(elementKey));
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
	public Map<Key<?>, ConflictResolver<?>> getConflictResolvers() {
		return emptyMap();
	}
}

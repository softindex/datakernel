package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Provider;
import io.datakernel.di.Scope;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.di.util.ReflectionUtils.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;

public final class DefaultModule implements Module {

	private static final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(new HashMap<>());

	private static final Map<Type, Set<BindingGenerator<?>>> generators = new HashMap<>();
	private static final Map<Integer, BindingTransformer<?>> transformers = new HashMap<>();

	static {
		// transforming bindings to add @Inject fields and methods as dependencies
		transformers.put(0, (key, context) -> ReflectionUtils.injectingInitializer(key));

		// generating bindings for classes that have @Inject constructors/factory methods
		generators.put(Object.class, singleton((scope, key, context) -> ReflectionUtils.generateImplicitBinding(key)));

		// generating bindings for provider requests
		generators.put(new Key<Provider<?>>() {}.getType(), singleton((scope, key, context) -> {
			Key<Object> elementKey = Key.ofType(key.getTypeParams()[0], key.getName());
			Binding<Object> elementBinding = context.getBinding(elementKey);
			if (elementBinding == null) {
				return null;
			}
			//noinspection unchecked
			return (Binding) bindingForProvider(elementKey, elementBinding);
		}));
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

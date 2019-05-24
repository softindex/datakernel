package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.util.Trie;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface Module {
	Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings();

	Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers();
}

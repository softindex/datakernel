package io.datakernel.di;

import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.combineMultimap;

public final class ScopedBindings {
	private final Map<Key<?>, Set<Binding<?>>> bindings;
	private final Map<Scope, ScopedBindings> children;

	public ScopedBindings(Map<Key<?>, Set<Binding<?>>> bindings, Map<Scope, ScopedBindings> children) {
		this.bindings = bindings;
		this.children = children;
	}

	public static ScopedBindings create() {
		return new ScopedBindings(new HashMap<>(), new HashMap<>());
	}

	public static ScopedBindings of(Map<Key<?>, Set<Binding<?>>> bindings) {
		return new ScopedBindings(bindings, new HashMap<>());
	}

	public static ScopedBindings of(Map<Key<?>, Set<Binding<?>>> bindings, Map<Scope, ScopedBindings> children) {
		return new ScopedBindings(bindings, children);
	}

	public Map<Key<?>, Set<Binding<?>>> getBindings() {
		return bindings;
	}

	public Map<Scope, ScopedBindings> getChildren() {
		return children;
	}

	public <T> void add(Key<T> key, Binding<T> binding) {
		bindings.computeIfAbsent(key, $ -> new HashSet<>()).add(binding);
	}

	public ScopedBindings resolve(Scope[] path) {
		ScopedBindings bindings = this;
		for (Scope scope : path) {
			bindings = bindings.children.computeIfAbsent(scope, $ -> create());
		}
		return bindings;
	}

	private static void mergeInto(ScopedBindings into, ScopedBindings from) {
		combineMultimap(into.bindings, from.bindings);
		from.children.forEach((scope, child) -> mergeInto(into.children.computeIfAbsent(scope, $ -> create()), child));
	}

	public static ScopedBindings merge(ScopedBindings first, ScopedBindings second) {
		ScopedBindings combined = create();
		mergeInto(combined, first);
		mergeInto(combined, second);
		return combined;
	}

	public static ScopedBindings merge(ScopedBindings... bindings) {
		return merge(Arrays.asList(bindings));
	}

	public static ScopedBindings merge(Collection<ScopedBindings> bindings) {
		return merge(bindings.stream());
	}

	public static ScopedBindings merge(Stream<ScopedBindings> bindings) {
		ScopedBindings combined = create();
		bindings.forEach(sb -> mergeInto(combined, sb));
		return combined;
	}
}

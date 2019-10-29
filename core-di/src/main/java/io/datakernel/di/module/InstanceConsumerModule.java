package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.util.Types;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;

/**
 * Extension module.
 * <p>
 * For each type <code>T</code> if a <code>Set&lt;Consumer&lt;T&gt;&gt;</code> is defined,
 * all consumers from that set are called upon <code>T</code>'s creation
 */
public final class InstanceConsumerModule extends AbstractModule {
	private Predicate<Key<?>> matcher = key -> true;
	private int priority = 0;

	public static InstanceConsumerModule create() {
		return new InstanceConsumerModule();
	}

	private InstanceConsumerModule() {
	}

	public InstanceConsumerModule withMatcher(Predicate<Key<?>> matcher) {
		this.matcher = matcher;
		return this;
	}

	public InstanceConsumerModule withPriority(int priority) {
		this.priority = priority;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void configure() {
		transform(priority, (bindings, scope, key, binding) -> {
			if (!matcher.test(key)) {
				return binding;
			}

			Type consumerType = Types.parameterized(Consumer.class, key.getType());
			Key<Set<Consumer<?>>> setKey = Key.ofType(Types.parameterized(Set.class, consumerType), key.getName());

			Binding<Set<Consumer<?>>> consumerBinding = bindings.get(setKey);

			if (consumerBinding == null) {
				return binding;
			}
			return binding
					.addDependencies(setKey)
					.mapInstance(singletonList(setKey), (objects, obj) -> {
						((Set<Consumer>) objects[0]).forEach(consumer -> consumer.accept(obj));
						return obj;
					});
		});
	}
}

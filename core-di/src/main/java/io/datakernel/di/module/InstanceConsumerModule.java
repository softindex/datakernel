package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.BindingTransformer;
import io.datakernel.di.core.Key;
import io.datakernel.di.util.Types;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;

/**
 * @see BindingTransformer#transform
 * Extension module.
 * It allows to accept any T instances after their creatoon, if
 * multibinder to Set <Consumer <? extends T >> was defined for current module.
 */
public class InstanceConsumerModule extends AbstractModule {
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

	@Override
	protected void configure() {
		BindingTransformer<?> transformer = (bindings, scope, key, binding) -> {
			if (!matcher.test(key)) {
				return binding;
			}
			Key<Set<Consumer<?>>> consumerSet = Key.ofType(Types.parameterized(Set.class,
					Types.parameterized(Consumer.class, key.getType())), key.getName());
			Binding<Set<Consumer<?>>> consumerBinding = bindings.get(consumerSet);
			if (consumerBinding == null) {
				return binding;
			}
			return binding
					.addDependencies(consumerSet)
					.mapInstance(singletonList(consumerSet), (objects, obj) -> {
						((Set<Consumer>) objects[0]).forEach(consumer -> consumer.accept(obj));
						return obj;
					});
		};
		transform(priority, transformer);
	}
}

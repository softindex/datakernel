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
public class ConsumerApplierModule extends AbstractModule {
	private Class<? extends Consumer> consumerType;
	private Predicate<Key<?>> matcher;
	private int priority;

	public static ConsumerApplierModule create() {
		return new ConsumerApplierModule();
	}

	private ConsumerApplierModule() {
		this.consumerType = Consumer.class;
		this.priority = 0;
		this.matcher = key -> true;
	}

	public ConsumerApplierModule withConsumerType(Class<? extends Consumer> consumerType) {
		this.consumerType = consumerType;
		return this;
	}

	public ConsumerApplierModule withPriority(int priority) {
		this.priority = priority;
		return this;
	}

	public ConsumerApplierModule withMatcher(Predicate<Key<?>> matcher) {
		this.matcher = matcher;
		return this;
	}

	@Override
	protected void configure() {
		BindingTransformer<?> transformer = (bindings, scope, key, binding) -> {
			if (!matcher.test(key)) {
				return binding;
			}
			Key<Set<Consumer<?>>> consumerSet = Key.ofType(Types.parameterized(Set.class,
					Types.parameterized(consumerType, key.getType())), key.getName());
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

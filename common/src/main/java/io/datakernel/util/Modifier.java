package io.datakernel.util;

import java.util.List;
import java.util.function.UnaryOperator;

@SuppressWarnings("unchecked")
public interface Modifier<T extends Modifier<T>> {
	default T with(UnaryOperator<T> modifier) {
		return modifier.apply((T) this);
	}

	default T with(List<? extends UnaryOperator<T>> modifiers) {
		T self = (T) this;
		for (UnaryOperator<T> modifier : modifiers) {
			self = self.with(modifier);
		}
		return self;
	}
}

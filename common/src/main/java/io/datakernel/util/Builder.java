package io.datakernel.util;

import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

@SuppressWarnings("unchecked")
public interface Builder<T extends Builder<T>> {
	default T with(UnaryOperator<T> operator) {
		return operator.apply((T) this);
	}

	default T with(List<? extends UnaryOperator<T>> operators) {
		T self = (T) this;
		for (UnaryOperator<T> operator : operators) {
			self = self.with(operator);
		}
		return self;
	}

	default T with(UnaryOperator<T>... operators) {
		return with(Arrays.asList(operators));
	}
}

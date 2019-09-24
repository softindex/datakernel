package io.global.blog.preprocessor;

public interface Preprocessor<T> {
	T process(T instance, Object... contextParams);

	default Preprocessor<T> then(Preprocessor<T> preprocessor) {
		return (instance, params) -> preprocessor.process(this.process(instance, params), params);
	}
}

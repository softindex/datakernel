package io.datakernel.http.decoder;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static io.datakernel.util.CollectionUtils.concat;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public interface HttpValidator<T> {
	List<HttpDecodeErrors.Error> validate(T value);

	default HttpValidator<T> and(HttpValidator<T> next) {
		if (this == alwaysOk()) return next;
		if (next == alwaysOk()) return this;
		return value -> {
			List<HttpDecodeErrors.Error> thisErrors = this.validate(value);
			List<HttpDecodeErrors.Error> nextErrors = next.validate(value);
			return concat(thisErrors, nextErrors);
		};
	}

	default HttpValidator<T> then(HttpValidator<T> next) {
		if (this == alwaysOk()) return next;
		if (next == alwaysOk()) return this;
		return value -> {
			List<HttpDecodeErrors.Error> thisErrors = this.validate(value);
			return thisErrors.isEmpty() ? next.validate(value) : thisErrors;
		};
	}

	static <T> HttpValidator<T> alwaysOk() {
		return value -> emptyList();
	}

	@SafeVarargs
	static <T> HttpValidator<T> sequence(HttpValidator<T>... validators) {
		return sequence(Arrays.asList(validators));
	}

	static <T> HttpValidator<T> sequence(List<HttpValidator<T>> validators) {
		return validators.stream().reduce(alwaysOk(), HttpValidator::then);
	}

	@SafeVarargs
	static <T> HttpValidator<T> of(HttpValidator<T>... validators) {
		return of(Arrays.asList(validators));
	}

	static <T> HttpValidator<T> of(List<HttpValidator<T>> validators) {
		return validators.stream().reduce(alwaysOk(), HttpValidator::and);
	}

	static <T> HttpValidator<T> of(Predicate<T> predicate, String template) {
		return value -> predicate.test(value) ?
				emptyList() :
				singletonList(HttpDecodeErrors.Error.of(template, value));
	}
}

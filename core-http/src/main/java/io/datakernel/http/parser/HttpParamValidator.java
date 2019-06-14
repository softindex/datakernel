package io.datakernel.http.parser;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static io.datakernel.util.CollectionUtils.concat;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public interface HttpParamValidator<T> {
	List<HttpParamParseErrorsTree.Error> validate(T value);

	default HttpParamValidator<T> and(HttpParamValidator<T> next) {
		if (this == alwaysOk()) return next;
		if (next == alwaysOk()) return this;
		return value -> {
			List<HttpParamParseErrorsTree.Error> thisErrors = this.validate(value);
			List<HttpParamParseErrorsTree.Error> nextErrors = next.validate(value);
			return concat(thisErrors, nextErrors);
		};
	}

	default HttpParamValidator<T> then(HttpParamValidator<T> next) {
		if (this == alwaysOk()) return next;
		if (next == alwaysOk()) return this;
		return value -> {
			List<HttpParamParseErrorsTree.Error> thisErrors = this.validate(value);
			return thisErrors.isEmpty() ? next.validate(value) : thisErrors;
		};
	}

	static <T> HttpParamValidator<T> alwaysOk() {
		return value -> emptyList();
	}

	@SafeVarargs
	static <T> HttpParamValidator<T> sequence(HttpParamValidator<T>... validators) {
		return sequence(Arrays.asList(validators));
	}

	static <T> HttpParamValidator<T> sequence(List<HttpParamValidator<T>> validators) {
		return validators.stream().reduce(alwaysOk(), HttpParamValidator::then);
	}

	@SafeVarargs
	static <T> HttpParamValidator<T> of(HttpParamValidator<T>... validators) {
		return of(Arrays.asList(validators));
	}

	static <T> HttpParamValidator<T> of(List<HttpParamValidator<T>> validators) {
		return validators.stream().reduce(alwaysOk(), HttpParamValidator::and);
	}

	static <T> HttpParamValidator<T> of(Predicate<T> predicate, String template) {
		return value -> predicate.test(value) ?
				emptyList() :
				singletonList(HttpParamParseErrorsTree.Error.of(template, value));
	}
}

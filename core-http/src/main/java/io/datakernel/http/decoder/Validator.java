package io.datakernel.http.decoder;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static io.datakernel.common.collection.CollectionUtils.concat;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * An enhanced predicate which can return a list of errors for given input object.
 * This can be used to put additional constraints on the parsed object from HTTP decoder.
 * For example to ensure, that age of the person is in range 0-100 or something
 * and return a specific error tree for that input.
 */
public interface Validator<T> {
	List<DecodeError> validate(T value);

	/**
	 * Combines this validator with some other one.
	 * This calls both the validators on the same input
	 * and then returns their errors combined.
	 */
	default Validator<T> and(Validator<T> next) {
		if (this == alwaysOk()) return next;
		if (next == alwaysOk()) return this;
		return value -> {
			List<DecodeError> thisErrors = this.validate(value);
			List<DecodeError> nextErrors = next.validate(value);
			return concat(thisErrors, nextErrors);
		};
	}

	/**
	 * Combines this validator with some other one.
	 * This calls the validators in order and if the first fails
	 * the the second is never called and errors of the first one are returned.
	 */
	default Validator<T> then(Validator<T> next) {
		if (this == alwaysOk()) return next;
		if (next == alwaysOk()) return this;
		return value -> {
			List<DecodeError> thisErrors = this.validate(value);
			return thisErrors.isEmpty() ? next.validate(value) : thisErrors;
		};
	}

	static <T> Validator<T> alwaysOk() {
		return value -> emptyList();
	}

	/**
	 * Combines multiple validators repeatedly with the {@link #then} call.
	 */
	@SafeVarargs
	static <T> Validator<T> sequence(Validator<T>... validators) {
		return sequence(Arrays.asList(validators));
	}

	/**
	 * Combines multiple validators repeatedly with the {@link #then} call.
	 */

	static <T> Validator<T> sequence(List<Validator<T>> validators) {
		return validators.stream().reduce(alwaysOk(), Validator::then);
	}

	/**
	 * Combines multiple validators repeatedly with the {@link #and} call.
	 */
	@SafeVarargs
	static <T> Validator<T> of(Validator<T>... validators) {
		return of(Arrays.asList(validators));
	}

	/**
	 * Combines multiple validators repeatedly with the {@link #and} call.
	 */
	static <T> Validator<T> of(List<Validator<T>> validators) {
		return validators.stream().reduce(alwaysOk(), Validator::and);
	}

	/**
	 * Creates the validator from the given predicate, using <code>template</code>
	 * as a message, formatted with the unsatisfactory value as a first argument.
	 */
	static <T> Validator<T> of(Predicate<T> predicate, String template) {
		return value -> predicate.test(value) ?
				emptyList() :
				singletonList(DecodeError.of(template, value));
	}
}

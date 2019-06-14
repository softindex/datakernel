package io.datakernel.parser;

import io.datakernel.http.parser.HttpParamParseErrorsTree;
import io.datakernel.http.parser.HttpParamValidator;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class HttpParamValidatorTest {
	@Test
	public void testAnd() {
		HttpParamValidator<String> validator =
				HttpParamValidator.of(s -> !s.isEmpty(), "test");
		validator = validator.and(HttpParamValidator.of(s -> s.length() > 5, "Invalid length"));

		List<HttpParamParseErrorsTree.Error> errors = validator.validate("");
		assertEquals(2, errors.size());

		errors = validator.validate("tmp");
		assertEquals(1, errors.size());
	}

	@Test(expected = NullPointerException.class)
	public void testAndNull() {
		HttpParamValidator<String> validator =
				HttpParamValidator.of(Objects::nonNull, "test");
		validator = validator.and(HttpParamValidator.of(s -> s.length() > 5, "Invalid length"));

		validator.validate(null);
	}

	@Test
	public void testThenNull() {
		HttpParamValidator<String> validator =
				HttpParamValidator.of(Objects::nonNull, "Cannot be null");
		validator = validator.then(HttpParamValidator.of(s -> s.length() > 5, "Invalid length"));

		assertEquals(validator.validate(null).get(0).getMessage(), "Cannot be null");
	}
}

package io.datakernel.http.decoder;

import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class HttpValidatorTest {
	@Test
	public void testAnd() {
		HttpValidator<String> validator =
				HttpValidator.of(s -> !s.isEmpty(), "test");
		validator = validator.and(HttpValidator.of(s -> s.length() > 5, "Invalid length"));

		List<HttpDecodeErrors.Error> errors = validator.validate("");
		assertEquals(2, errors.size());

		errors = validator.validate("tmp");
		assertEquals(1, errors.size());
	}

	@Test(expected = NullPointerException.class)
	public void testAndNull() {
		HttpValidator<String> validator =
				HttpValidator.of(Objects::nonNull, "test");
		validator = validator.and(HttpValidator.of(s -> s.length() > 5, "Invalid length"));

		validator.validate(null);
	}

	@Test
	public void testThenNull() {
		HttpValidator<String> validator =
				HttpValidator.of(Objects::nonNull, "Cannot be null");
		validator = validator.then(HttpValidator.of(s -> s.length() > 5, "Invalid length"));

		assertEquals(validator.validate(null).get(0).getMessage(), "Cannot be null");
	}
}

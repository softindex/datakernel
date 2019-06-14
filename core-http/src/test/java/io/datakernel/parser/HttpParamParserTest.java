package io.datakernel.parser;

import io.datakernel.functional.Either;
import io.datakernel.http.HttpCookie;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.parser.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HttpParamParserTest {
	@Test
	public void test() throws HttpParamParseException {
		HttpParamParser<String> parser = HttpParamParsers.ofCookie("tmp");
		parser.parseOrThrow(HttpRequest.get("http://example.com")
				.withCookie(HttpCookie.of("tmp", "1")));
	}

	@Test
	public void testMap() {
		HttpParamParser<Double> parser = HttpParamParsers.ofCookie("key")
				.map(Integer::parseInt)
				.validate(HttpParamValidator.of(param -> param > 10, "Lower then 10"))
				.map(Integer::doubleValue)
				.validate(HttpParamValidator.of(value -> value % 2 == 0, "Is even"));

		Either<Double, HttpParamParseErrorsTree> key = parser.parse(HttpRequest.get("http://example.com")
				.withCookie(HttpCookie.of("key", "11")));
		HttpParamParseErrorsTree exception = key.getRight();
		assertEquals(exception.getErrors().get(0).getMessage(), "Is even");
	}
}

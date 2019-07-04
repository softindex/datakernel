package io.datakernel.http.decoder;

import io.datakernel.functional.Either;
import io.datakernel.http.HttpCookie;
import io.datakernel.http.HttpRequest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HttpDecoderTest {
	@Test
	public void test() throws HttpDecodeException {
		HttpDecoder<String> parser = HttpDecoders.ofCookie("tmp");
		assertEquals("1",
				parser.decodeOrThrow(HttpRequest.get("http://example.com")
						.withCookie(HttpCookie.of("tmp", "1"))));
	}

	@Test
	public void testMap() {
		HttpDecoder<Double> parser = HttpDecoders.ofCookie("key")
				.map(Integer::parseInt)
				.validate(HttpValidator.of(param -> param > 10, "Lower then 10"))
				.map(Integer::doubleValue)
				.validate(HttpValidator.of(value -> value % 2 == 0, "Is even"));

		Either<Double, HttpDecodeErrors> key = parser.decode(HttpRequest.get("http://example.com")
				.withCookie(HttpCookie.of("key", "11")));
		HttpDecodeErrors exception = key.getRight();
		assertEquals(exception.getErrors().get(0).getMessage(), "Is even");
	}
}

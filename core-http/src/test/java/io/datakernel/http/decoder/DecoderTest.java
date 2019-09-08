package io.datakernel.http.decoder;

import io.datakernel.common.collection.Either;
import io.datakernel.http.HttpCookie;
import io.datakernel.http.HttpRequest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DecoderTest {
	@Test
	public void test() throws DecodeException {
		Decoder<String> parser = Decoders.ofCookie("tmp");
		assertEquals("1",
				parser.decodeOrThrow(HttpRequest.get("http://example.com")
						.withCookie(HttpCookie.of("tmp", "1"))));
	}

	@Test
	public void testMap() {
		Decoder<Double> parser = Decoders.ofCookie("key")
				.map(Integer::parseInt)
				.validate(Validator.of(param -> param > 10, "Lower then 10"))
				.map(Integer::doubleValue)
				.validate(Validator.of(value -> value % 2 == 0, "Is even"));

		Either<Double, DecodeErrors> key = parser.decode(HttpRequest.get("http://example.com")
				.withCookie(HttpCookie.of("key", "11")));
		DecodeErrors exception = key.getRight();
		assertEquals(exception.getErrors().get(0).getMessage(), "Is even");
	}
}

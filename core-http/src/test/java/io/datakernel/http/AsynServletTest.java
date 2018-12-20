package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class AsynServletTest {
	@Test
	public void testEnsureRequestBody() throws ParseException {
		AsyncServlet servlet = AsyncServlet.ensureRequestBody(request -> Promise.of(HttpResponse.ok200().withBody(request.takeBody())));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSupplier.of(
						ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
						ByteBuf.wrapForReading("Test2".getBytes(UTF_8)))
				);

		servlet.serve(testRequest)
				.whenComplete(assertComplete(res -> assertEquals("Test1Test2", res.takeBody().asString(UTF_8))));
	}

	@Test
	public void testEnsureRequestBodyWithException() throws ParseException {
		AsyncServlet servlet = AsyncServlet.ensureRequestBody(request -> Promise.of(HttpResponse.ok200().withBody(request.takeBody())));

		String exceptionMessage = "TestException";

		ByteBuf byteBuf = ByteBufPool.allocate(100);
		byteBuf.put("Test1".getBytes(UTF_8));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSuppliers.concat(
						ChannelSupplier.of(byteBuf),
						ChannelSupplier.ofException(new Exception(exceptionMessage))
				));

		servlet.serve(testRequest)
				.whenComplete(assertFailure(exceptionMessage));
	}

	@Test
	public void testEnsureRequestBodyMaxSize() throws ParseException {
		AsyncServlet servlet = AsyncServlet.ensureRequestBody(request -> Promise.of(HttpResponse.ok200().withBody(request.takeBody())), 2);

		ByteBuf byteBuf = ByteBufPool.allocate(100);
		byteBuf.put("Test1".getBytes(UTF_8));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSupplier.of(byteBuf));

		servlet.serve(testRequest)
				.whenComplete(assertFailure("ByteBufQueue exceeds maximum size of 2 bytes"));
	}
}

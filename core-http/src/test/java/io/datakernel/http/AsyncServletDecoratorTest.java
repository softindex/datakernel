package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.UncheckedException;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.http.AsyncServletDecorator.*;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class AsyncServletDecoratorTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testOf() {
		List<Integer> result = new ArrayList<>();
		AsyncServlet rootServlet = combineDecorators(
				servlet ->
						request -> {
							result.add(1);
							return servlet.serve(request);
						},
				servlet ->
						request -> {
							result.add(2);
							return servlet.serve(request);
						},
				servlet ->
						request -> {
							result.add(3);
							return servlet.serve(request);
						})
				.serve(request -> {
					result.add(4);
					return null;
				});
		rootServlet.serve(null);

		assertEquals(result.size(), 4);
		assertArrayEquals(result.toArray(new Integer[0]), new Integer[]{1, 2, 3, 4});
	}

	@Test
	public void testOnRequest() {
		byte[] body = {0};
		AsyncServlet rootServlet = AsyncServletDecorator.identity()
				.combine(onRequest(request -> request.setBody(body)))
				.combine(loadBody())
				.serve(request -> {
					ByteBuf loadedBody = request.getBody();
					assertArrayEquals(loadedBody.getArray(), body);
					loadedBody.recycle();
					return Promise.of(HttpResponse.ok200());
				});

		HttpRequest request = HttpRequest.get("http://example.com");
		await(rootServlet.serve(request)).recycle();
	}

	@Test
	public void testOnResponse() {
		byte[] body = {0};
		AsyncServlet rootServlet = onResponse(response -> response.setBody(body))
				.serve(request -> Promise.of(HttpResponse.ok200()
						.withBody(new byte[10])));

		HttpRequest request = HttpRequest.get("http://example.com");
		ByteBuf loadedBody = await(rootServlet.serve(request)
				.then(HttpMessage::loadBody));
		assertArrayEquals(loadedBody.getArray(), body);
		loadedBody.recycle();
	}

	@Test
	public void testOnResponseBiConsumer() {
		AsyncServlet servlet = onResponse(
				(request, response) -> {
					request.recycle();
					response.withCookie(HttpCookie.of("test2", "test2"));
				})
				.serve(request -> {
					assertNull(request.getCookie("test1"));
					return Promise.of(HttpResponse.ok200());
				});

		HttpResponse response = await(servlet.serve(HttpRequest.get("http://example.com")));
		assertNotNull(response.getCookie("test2"));
		assertEquals(response.getCookie("test2").getValue(), "test2");
	}

	@Test
	public void testMapResponse() {
		AsyncServlet servlet = mapResponse(
				response -> HttpResponse.ok200())
				.serve(request -> Promise.of(HttpResponse.ok200()
						.withCookie(HttpCookie.of("test2", "test2"))
						.withBody(ByteBufPool.allocate(100))));

		ByteBuf body = ByteBufPool.allocate(100);
		await(servlet.serve(HttpRequest.get("http://example.com")
				.withBody(body)));
		body.recycle();
	}

	@Test
	public void testMapResponse2() {
		AsyncServlet servlet = mapResponse(
				response -> HttpResponse.ok200())
				.serve(request -> Promise.of(HttpResponse.ok200()
						.withCookie(HttpCookie.of("test2", "test2"))
						.withBodyStream(ChannelSupplier.of(
								ByteBufPool.allocate(100),
								ByteBufPool.allocate(100)))));

		ByteBuf body = ByteBufPool.allocate(100);
		await(servlet.serve(HttpRequest.get("http://example.com")
				.withBody(body)));
		body.recycle();
	}

	@Test
	public void testOnException() {
		AsyncServlet servlet = onException((request, throwable) -> assertEquals(throwable.getClass(), HttpException.class))
				.serve(request -> Promise.ofException(new HttpException(202)));
		awaitException(servlet.serve(null));
	}

	@Test
	public void testMapException() {
		AsyncServlet servlet = combineDecorators(
				mapException(throwable -> HttpResponse.ofCode(100)),
				mapException(throwable -> HttpResponse.ofCode(200)))
				.serve(request -> Promise.ofException(new HttpException(300)));

		HttpResponse response = await(servlet.serve(null));
		assertEquals(response.getCode(), 200);
	}

	@Test
	public void testRuntimeExeptionExceptions() {
		AsyncServlet servlet = combineDecorators(catchRuntimeExceptions(), loadBody())
				.serve(request -> Promise.of(HttpResponse.ok200()));

		NullPointerException exception = awaitException(servlet.serve(HttpRequest.get("http://example.com")));
		assertNotNull(exception);
	}

	@Test
	public void testCathcUncheckedException() {
		AsyncServlet servlet = AsyncServletDecorator.catchUncheckedExceptions()
				.serve(request -> {
					throw new UncheckedException(new NullPointerException());
				});

		NullPointerException throwable = awaitException(servlet.serve(null));
		assertNotNull(throwable);
	}
}

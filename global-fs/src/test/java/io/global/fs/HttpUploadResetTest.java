package io.global.fs;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.ContentTypes;
import io.datakernel.http.HttpHeaderValue;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.ref.RefLong;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.global.fs.util.HttpDataFormats.httpUpload;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpUploadResetTest {

	public static void main(String[] args) throws IOException {

		Eventloop eventloop = Eventloop.create()
				.withCurrentThread()
				.withFatalErrorHandler(rethrowOnAnyError());

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, request ->
				httpUpload(request, (name, offset, revision) -> {
					System.out.println("name = " + name + ", offset = " + offset + ", revision = " + revision);
					RefLong size = new RefLong(0);
					return Promise.of(ChannelConsumers.<ByteBuf>recycling().mapAsync(buf -> {
						if (size.inc(buf.readRemaining()) > 10 * 1024 * 1024 /* 10 MB*/) {
							// could be tested by Content-Length ofc, but here we are testing what happens
							// exactly at some point of actual data being received
							return Promise.<ByteBuf>ofException(new StacklessException(HttpUploadResetTest.class, "File too large"));
						}
						return Promises.delay(10L, buf);
					}));
				}))
				.withHttpErrorFormatter(e ->
						HttpResponse.ofCode(500)
								.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.JSON_UTF_8))
								.withBody(e.toString().getBytes(UTF_8)))
				.withListenAddress(new InetSocketAddress(8080));
		server.listen();

		eventloop.run();

		// use a command similar to below one to test it
		// curl -vF file=@big-or-small-random-file.bin localhost:8080
	}
}

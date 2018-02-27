package io.datakernel.http.boot;

import com.google.inject.Provides;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.AsyncServlet;
import io.datakernel.util.guice.SimpleModule;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.http.HttpResponse.ok200;

public class HelloWorldServletModule extends SimpleModule {

	// region creators
	private HelloWorldServletModule() {
	}

	public static HelloWorldServletModule create() {
		return new HelloWorldServletModule();
	}
	// endregion

	@Provides
	public AsyncServlet provide() {
		return AsyncServlet.ofBlocking(req -> ok200()
				.withBody(ByteBuf.wrapForReading(encodeAscii("Hello from DataKernel async HTTP server!"))));
	}
}

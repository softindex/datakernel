package io.datakernel.boot.http;

import com.google.inject.Provides;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.AsyncServlet;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.http.HttpResponse.ok200;

public class HelloWorldWorkerServletModule extends SimpleModule {

	// region creators
	private HelloWorldWorkerServletModule() {
	}

	public static HelloWorldWorkerServletModule create() {
		return new HelloWorldWorkerServletModule();
	}
	// endregion

	@Provides
	@Worker
	AsyncServlet provideServlet(@WorkerId int worker) {
		return AsyncServlet.ofBlocking(req -> ok200()
				.withBody(ByteBuf.wrapForReading(encodeAscii("Hello, world! #" + worker))));
	}
}

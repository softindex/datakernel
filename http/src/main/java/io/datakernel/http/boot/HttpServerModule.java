package io.datakernel.http.boot;

import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.util.MemSize;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;

import java.util.function.Consumer;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_KEEP_ALIVE_MILLIS;

public class HttpServerModule extends SimpleModule {

	// region creators
	private HttpServerModule() {
	}

	public static HttpServerModule create() {
		return new HttpServerModule();
	}
	// endregion

	@Provides
	@Worker
	public AsyncHttpServer provide(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
		return AsyncHttpServer.create(eventloop, rootServlet)
				.initialize(getHttpServerInitializer(config.getChild("http")));
	}

	static Consumer<AsyncHttpServer> getHttpServerInitializer(Config config) {
		return s -> {
			s.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)));
			s.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS));
			s.withReadTimeout(config.get(ofInteger(), "readTimeout", 0));
			s.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
			s.initialize(config.get(ofAbstractServerInitializer(8080), "server"));
		};
	}
}

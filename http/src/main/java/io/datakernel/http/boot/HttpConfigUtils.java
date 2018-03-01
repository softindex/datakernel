package io.datakernel.http.boot;

import io.datakernel.config.Config;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.util.MemSize;

import java.util.function.Consumer;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_KEEP_ALIVE_MILLIS;

public class HttpConfigUtils {
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

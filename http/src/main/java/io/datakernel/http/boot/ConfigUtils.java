package io.datakernel.http.boot;

import io.datakernel.config.Config;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.util.MemSize;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofMemSize;
import static io.datakernel.config.ConfigUtils.initializeAbstractServer;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_KEEP_ALIVE_MILLIS;

public class ConfigUtils {
	private ConfigUtils() {
	}

	public static void initializeHttpServer(AsyncHttpServer server, Config config) {
		initializeAbstractServer(server, config);
		server.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)));
		server.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS));
		server.withReadTimeout(config.get(ofInteger(), "readTimeout", 0));
		server.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
	}

	public static void initializeHttpWorker(AsyncHttpServer worker, Config config) {
		worker.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)));
		worker.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS));
		worker.withReadTimeout(config.get(ofInteger(), "readTimeout", 0));
		worker.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
	}
}
